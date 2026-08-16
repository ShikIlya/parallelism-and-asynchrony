import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from crawler import AsyncCrawler
from main import load_sequentially


def make_mock_response(status=200, body="", raise_error=None):
    response = MagicMock()
    response.status = status
    response.text = AsyncMock(return_value=body)

    if raise_error is not None:
        response.raise_for_status = MagicMock(side_effect=raise_error)
    else:
        response.raise_for_status = MagicMock(return_value=None)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=response)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def patch_session_get(crawler, get_side_effect):
    fake_session = MagicMock()
    fake_session.closed = False
    fake_session.get = MagicMock(side_effect=get_side_effect)
    fake_session.close = AsyncMock()

    crawler._get_session = AsyncMock(return_value=fake_session)
    return fake_session


# ---------------------------------------------------------------------------
# 1. Загрузка валидных URL
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_url_valid_returns_content():
    url = "https://example.com/ok"
    crawler = AsyncCrawler(max_concurrent=5)

    patch_session_get(
        crawler,
        lambda u: make_mock_response(status=200, body="<html>hello world</html>"),
    )

    content = await crawler.fetch_url(url)

    assert content == "<html>hello world</html>"
    assert len(content) > 0


@pytest.mark.asyncio
async def test_fetch_urls_multiple_valid():
    urls = [f"https://example.com/page{i}" for i in range(3)]
    crawler = AsyncCrawler(max_concurrent=10)

    def side_effect(u):
        return make_mock_response(status=200, body=f"content-{u}")

    patch_session_get(crawler, side_effect)

    results = await crawler.fetch_urls(urls)

    assert set(results.keys()) == set(urls)
    for u in urls:
        assert results[u] == f"content-{u}"


# ---------------------------------------------------------------------------
# 2. Обработка несуществующих URL (DNS-ошибка / 404 / 500)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_url_nonexistent_domain_returns_empty_string():
    url = "https://this-domain-does-not-exist-12345.com"
    crawler = AsyncCrawler(max_concurrent=5)

    def side_effect(u):
        raise aiohttp.ClientConnectorError(
            connection_key=None, os_error=OSError("Name or service not known")
        )

    patch_session_get(crawler, side_effect)

    content = await crawler.fetch_url(url)

    assert content == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [404, 500])
async def test_fetch_url_http_error_status_returns_empty_string(status_code):
    url = f"https://httpbingo.org/status/{status_code}"
    crawler = AsyncCrawler(max_concurrent=5)

    error = aiohttp.ClientResponseError(
        request_info=MagicMock(), history=(), status=status_code, message="error"
    )

    patch_session_get(
        crawler,
        lambda u: make_mock_response(status=status_code, body="error", raise_error=error),
    )

    content = await crawler.fetch_url(url)

    assert content == ""


@pytest.mark.asyncio
async def test_fetch_urls_mixed_valid_and_invalid():
    good_url = "https://example.com/good"
    bad_url = "https://example.com/bad"
    crawler = AsyncCrawler(max_concurrent=5)

    error = aiohttp.ClientResponseError(
        request_info=MagicMock(), history=(), status=404, message="not found"
    )

    def side_effect(u):
        if u == good_url:
            return make_mock_response(status=200, body="ok-content")
        return make_mock_response(status=404, body="not found", raise_error=error)

    patch_session_get(crawler, side_effect)

    results = await crawler.fetch_urls([good_url, bad_url])

    assert results[good_url] == "ok-content"
    assert results[bad_url] == ""


# ---------------------------------------------------------------------------
# 3. Обработка таймаутов
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_url_timeout_returns_empty_string():
    url = "https://example.com/slow"
    crawler = AsyncCrawler(max_concurrent=5)

    def side_effect(u):
        raise asyncio.TimeoutError()

    patch_session_get(crawler, side_effect)

    content = await crawler.fetch_url(url)

    assert content == ""


@pytest.mark.asyncio
async def test_fetch_url_timeout_does_not_raise():
    url = "https://example.com/slow2"
    crawler = AsyncCrawler(max_concurrent=5)

    def side_effect(u):
        raise asyncio.TimeoutError()

    patch_session_get(crawler, side_effect)

    try:
        content = await crawler.fetch_url(url)
    except asyncio.TimeoutError:
        pytest.fail("fetch_url не должен поднимать TimeoutError наружу")

    assert content == ""


# ---------------------------------------------------------------------------
# 4. Сравнение времени последовательной и параллельной загрузки
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parallel_is_faster_than_sequential():
    urls = [f"https://example.com/delay{i}" for i in range(5)]
    delay = 0.2  # секунд на один запрос

    async def fake_fetch_url(self, url):
        await asyncio.sleep(delay)
        return f"content-{url}"

    crawler_parallel = AsyncCrawler(max_concurrent=len(urls))
    crawler_parallel.fetch_url = fake_fetch_url.__get__(crawler_parallel, AsyncCrawler)

    crawler_sequential = AsyncCrawler(max_concurrent=1)
    crawler_sequential.fetch_url = fake_fetch_url.__get__(crawler_sequential, AsyncCrawler)

    start_parallel = time.perf_counter()
    parallel_results = await crawler_parallel.fetch_urls(urls)
    parallel_time = time.perf_counter() - start_parallel

    start_sequential = time.perf_counter()
    sequential_results = await load_sequentially(crawler_sequential, urls)
    sequential_time = time.perf_counter() - start_sequential

    assert parallel_results == sequential_results
    assert parallel_time < sequential_time
    assert sequential_time >= delay * len(urls) * 0.8
    assert parallel_time < delay * len(urls) * 0.8


@pytest.mark.asyncio
async def test_fetch_urls_preserves_order_and_speedup_ratio():
    urls = [f"https://example.com/item{i}" for i in range(4)]
    delay = 0.15

    async def fake_fetch_url(self, url):
        await asyncio.sleep(delay)
        return "data"

    crawler = AsyncCrawler(max_concurrent=len(urls))
    crawler.fetch_url = fake_fetch_url.__get__(crawler, AsyncCrawler)

    start = time.perf_counter()
    results = await crawler.fetch_urls(urls)
    elapsed = time.perf_counter() - start

    assert list(results.keys()) == urls
    assert elapsed < delay * len(urls)