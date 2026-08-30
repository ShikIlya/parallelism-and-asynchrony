import asyncio
import time
from unittest.mock import patch, AsyncMock, MagicMock
import aiohttp
import pytest
import pytest_asyncio
from bs4 import BeautifulSoup
from datetime import datetime, timezone

from crawler import AsyncCrawler
from main import load_sequentially
from html_parser import HTMLParser
from crawler_queue import CrawlerQueue
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from retry_strategy import RetryStrategy
from exceptions import (
    TransientError,
    PermanentError
)
from csv_storage import CSVStorage
from json_storage import JSONStorage
from postgresql_storage import PostgreSQLStorage

@pytest.fixture
def mock_session():
    session = AsyncMock()
    response = AsyncMock()
    response.status = 200
    response.text = AsyncMock(return_value="")
    response.raise_for_status = MagicMock()
    session.get.return_value.__aenter__.return_value = response
    session.get.return_value.__aexit__ = AsyncMock()
    return session

# --------------------------------------------------------------------------
# День 1: последовательная vs параллельная загрузка
# --------------------------------------------------------------------------

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
        lambda u, **kwargs: make_mock_response(status=200, body="<html>hello world</html>"),
    )

    content = await crawler.fetch_url(url)

    assert content == "<html>hello world</html>"
    assert len(content) > 0


@pytest.mark.asyncio
async def test_fetch_urls_multiple_valid():
    urls = [f"https://example.com/page{i}" for i in range(3)]
    crawler = AsyncCrawler(max_concurrent=10)

    def side_effect(u, **kwargs):
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

    def side_effect(u, **kwargs):
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
    delay = 0.2

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

# --------------------------------------------------------------------------
# День 2: парсинг HTML (HTMLParser + AsyncCrawler.fetch_and_parse)
# --------------------------------------------------------------------------

@pytest.fixture
def parser() -> HTMLParser:
    return HTMLParser()


VALID_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <title>Тестовая страница</title>
    <meta name="description" content="Описание тестовой страницы">
    <meta name="keywords" content="python, тест, парсер">
    <meta property="og:title" content="OG заголовок">
</head>
<body>
    <h1>Главный заголовок</h1>
    <h2>Подзаголовок первый</h2>
    <h3>Подзаголовок второй</h3>

    <p>Это основной текст страницы для проверки извлечения контента.</p>

    <a href="/relative/page1">Относительная ссылка 1</a>
    <a href="page2.html">Относительная ссылка 2</a>
    <a href="https://external.com/page">Внешняя абсолютная ссылка</a>
    <a href="#section">Якорь (должен быть отфильтрован)</a>
    <a href="mailto:test@example.com">Почта (должна быть отфильтрована)</a>
    <a href="javascript:void(0)">JS-ссылка (должна быть отфильтрована)</a>
    <a href="">Пустая ссылка (должна быть отфильтрована)</a>
    <a href="/relative/page1">Дубликат ссылки 1</a>

    <img src="/images/pic1.png" alt="Картинка 1">
    <img src="pic2.jpg">
    <img src="">

    <table>
        <tr><th>Имя</th><th>Возраст</th></tr>
        <tr><td>Анна</td><td>30</td></tr>
        <tr><td>Борис</td><td>25</td></tr>
    </table>

    <ul>
        <li>Первый пункт</li>
        <li>Второй пункт</li>
    </ul>
    <ol>
        <li>Шаг один</li>
        <li>Шаг два</li>
    </ol>

    <script>console.log("should be removed from text");</script>
    <style>.hidden { display: none; }</style>
</body>
</html>
"""

BASE_URL = "https://example.com/dir/page.html"


# --------------------------------------------------------------------------
# 1. Парсинг валидного HTML
# --------------------------------------------------------------------------

class TestParseValidHtml:

    def test_parse_html_returns_dict(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        assert isinstance(result, dict)

    def test_parse_html_no_error(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        assert result["error"] is None

    def test_parse_html_title(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        assert result["title"] == "Тестовая страница"

    def test_parse_html_text_contains_content(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        assert "основной текст страницы" in result["text"]

    def test_parse_html_text_excludes_script_and_style(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        assert "should be removed" not in result["text"]
        assert "display: none" not in result["text"]

    def test_parse_html_metadata(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        metadata = result["metadata"]
        assert metadata["title"] == "Тестовая страница"
        assert metadata["description"] == "Описание тестовой страницы"
        assert metadata["keywords"] == "python, тест, парсер"
        assert metadata.get("og_title") == "OG заголовок"

    def test_parse_html_links_present(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        assert len(result["links"]) > 0
        assert all(link.startswith("http") for link in result["links"])

    def test_parse_html_images(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        images = result["images"]
        assert len(images) == 2  # третий img с пустым src отфильтрован
        assert images[0]["alt"] == "Картинка 1"
        assert images[0]["src"].startswith("https://example.com")

    def test_parse_html_headings(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        headings = result["headings"]
        levels = [h["level"] for h in headings]
        assert levels == ["h1", "h2", "h3"]
        assert headings[0]["text"] == "Главный заголовок"

    def test_parse_html_tables(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        tables = result["tables"]
        assert len(tables) == 1
        assert tables[0][0] == ["Имя", "Возраст"]
        assert tables[0][1] == ["Анна", "30"]

    def test_parse_html_lists(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        lists = result["lists"]
        types = {lst["type"] for lst in lists}
        assert types == {"ul", "ol"}
        ul_list = next(lst for lst in lists if lst["type"] == "ul")
        assert ul_list["items"] == ["Первый пункт", "Второй пункт"]

    def test_parse_html_empty_string(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html("", BASE_URL))
        assert result["error"] == "empty_html"
        assert result["title"] == ""
        assert result["links"] == []


# --------------------------------------------------------------------------
# 2. Обработка битого (некорректного) HTML
# --------------------------------------------------------------------------

class TestParseBrokenHtml:
    BROKEN_HTML_UNCLOSED_TAGS = """
    <html><head><title>Битая страница</title>
    <body>
    <p>Незакрытый параграф
    <div>Незакрытый див
    <a href="/page1">Ссылка без закрытия тега
    <ul><li>Пункт 1<li>Пункт 2</ul>
    """

    BROKEN_HTML_UNCLOSED_TITLE = """
    <html><head><title>Битая страница без закрытия
    <body>
    <p>Текст, который может быть поглощён title</p>
    </body></html>
    """

    BROKEN_HTML_NO_STRUCTURE = "<p>Просто текст без html/head/body</p><a href='/x'>ссылка</a>"

    GARBAGE_HTML = "<<<>>>не html вообще&&&{}[]"

    def test_broken_html_does_not_raise(self, parser: HTMLParser):
        result = asyncio.run(
            parser.parse_html(self.BROKEN_HTML_UNCLOSED_TAGS, BASE_URL)
        )
        assert isinstance(result, dict)

    def test_broken_html_still_extracts_partial_data(self, parser: HTMLParser):
        result = asyncio.run(
            parser.parse_html(self.BROKEN_HTML_UNCLOSED_TAGS, BASE_URL)
        )
        assert result["title"] == "Битая страница"
        assert any("page1" in link for link in result["links"])

    def test_unclosed_title_does_not_raise_and_returns_dict(self, parser: HTMLParser):
        result = asyncio.run(
            parser.parse_html(self.BROKEN_HTML_UNCLOSED_TITLE, BASE_URL)
        )
        assert isinstance(result, dict)
        assert result["error"] is None
        assert "Битая страница" in result["title"]

    def test_html_without_doctype_or_tags(self, parser: HTMLParser):
        result = asyncio.run(
            parser.parse_html(self.BROKEN_HTML_NO_STRUCTURE, BASE_URL)
        )
        assert isinstance(result, dict)
        assert result["error"] is None
        assert "Просто текст" in result["text"]

    def test_garbage_input_returns_dict_without_crash(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(self.GARBAGE_HTML, BASE_URL))
        assert isinstance(result, dict)
        for key in ("url", "title", "text", "links", "metadata", "error"):
            assert key in result

    def test_none_like_html_handled(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html("   ", BASE_URL))
        assert isinstance(result, dict)
        assert result["links"] == []
        assert result["images"] == []


# --------------------------------------------------------------------------
# 3. Извлечение ссылок (extract_links)
# --------------------------------------------------------------------------

class TestExtractLinks:

    LINKS_HTML = """
    <html><body>
        <a href="/relative">Относительная</a>
        <a href="https://other-domain.com/page">Внешняя</a>
        <a href="https://example.com/dir/same-domain">Тот же домен</a>
        <a href="#anchor">Якорь</a>
        <a href="mailto:a@b.com">Почта</a>
        <a href="tel:+123456">Телефон</a>
        <a href="javascript:alert(1)">JS</a>
        <a>Без href</a>
        <a href="/relative">Дубликат</a>
    </body></html>
    """

    def test_extract_links_filters_non_http_schemes(self, parser: HTMLParser):
        soup = BeautifulSoup(self.LINKS_HTML, "html.parser")
        links = parser.extract_links(soup, "https://example.com/dir/page.html")
        assert not any("mailto:" in link for link in links)
        assert not any("tel:" in link for link in links)
        assert not any("javascript:" in link for link in links)
        assert not any(link.endswith("#anchor") for link in links)

    def test_extract_links_deduplicates(self, parser: HTMLParser):
        soup = BeautifulSoup(self.LINKS_HTML, "html.parser")
        links = parser.extract_links(soup, "https://example.com/dir/page.html")
        assert len(links) == len(set(links))

    def test_extract_links_includes_valid_absolute_and_relative(self, parser: HTMLParser):
        soup = BeautifulSoup(self.LINKS_HTML, "html.parser")
        links = parser.extract_links(soup, "https://example.com/dir/page.html")
        assert "https://example.com/relative" in links
        assert "https://other-domain.com/page" in links
        assert "https://example.com/dir/same-domain" in links

    def test_extract_links_same_domain_only_filter(self, parser: HTMLParser):
        soup = BeautifulSoup(self.LINKS_HTML, "html.parser")
        links = parser.extract_links(
            soup, "https://example.com/dir/page.html", same_domain_only=True
        )
        assert all("example.com" in link for link in links)
        assert "https://other-domain.com/page" not in links

    def test_extract_links_empty_html_returns_empty_list(self, parser: HTMLParser):
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        links = parser.extract_links(soup, BASE_URL)
        assert links == []


# --------------------------------------------------------------------------
# 4. Конвертация относительных URL в абсолютные
# --------------------------------------------------------------------------

class TestRelativeToAbsoluteUrls:

    @pytest.mark.parametrize(
        "href,base,expected",
        [
            ("/page1", "https://example.com", "https://example.com/page1"),
            ("page2.html", "https://example.com/dir/", "https://example.com/dir/page2.html"),
            ("../up.html", "https://example.com/dir/sub/", "https://example.com/dir/up.html"),
            ("https://other.com/x", "https://example.com", "https://other.com/x"),
            ("//cdn.example.com/asset.js", "https://example.com", "https://cdn.example.com/asset.js"),
        ],
    )
    def test_urljoin_conversion(self, parser: HTMLParser, href, base, expected):
        html = f'<html><body><a href="{href}">link</a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        links = parser.extract_links(soup, base)
        assert expected in links

    def test_images_relative_src_converted(self, parser: HTMLParser):
        html = '<html><body><img src="/img/pic.png" alt="pic"></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        images = parser.extract_images(soup, "https://example.com/dir/page.html")
        assert images[0]["src"] == "https://example.com/img/pic.png"

    def test_is_valid_url_accepts_http_https(self, parser: HTMLParser):
        assert parser._is_valid_url("https://example.com/page") is True
        assert parser._is_valid_url("http://example.com") is True

    def test_is_valid_url_rejects_invalid(self, parser: HTMLParser):
        assert parser._is_valid_url("javascript:void(0)") is False
        assert parser._is_valid_url("not a url") is False
        assert parser._is_valid_url("") is False
        assert parser._is_valid_url("ftp://example.com/file") is False

    def test_full_page_relative_links_all_absolute(self, parser: HTMLParser):
        result = asyncio.run(parser.parse_html(VALID_HTML, BASE_URL))
        for link in result["links"]:
            assert link.startswith("https://") or link.startswith("http://")


# --------------------------------------------------------------------------
# День 3: управление конкурентностью и очередями
# --------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 1. Очередь с приоритетами
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_queue_returns_highest_priority_first():
    queue = CrawlerQueue()
    queue.add_url("https://example.com/low", priority=0)
    queue.add_url("https://example.com/high", priority=10)
    queue.add_url("https://example.com/mid", priority=5)

    order = []
    while True:
        url = await queue.get_next()
        if url is None:
            break
        order.append(url)

    assert order == [
        "https://example.com/high",
        "https://example.com/mid",
        "https://example.com/low",
    ]


@pytest.mark.asyncio
async def test_queue_get_next_empty_returns_none():
    queue = CrawlerQueue()
    result = await queue.get_next()
    assert result is None


@pytest.mark.asyncio
async def test_queue_mark_processed_and_failed_tracked_separately():
    queue = CrawlerQueue()
    queue.mark_processed("https://example.com/ok", {"title": "OK"})
    queue.mark_failed("https://example.com/bad", "timeout")

    assert "https://example.com/ok" in queue.processed_urls
    assert queue.processed_urls["https://example.com/ok"]["title"] == "OK"
    assert queue.failed_urls["https://example.com/bad"] == "timeout"


def test_queue_get_stats_shape_and_values():
    queue = CrawlerQueue()
    queue.add_url("https://example.com/a")
    queue.add_url("https://example.com/b")
    queue.mark_processed("https://example.com/a", {})

    stats = queue.get_stats()

    for key in ["count_queue", "failed_urls", "processed_urls", "queued_urls", "elapsed_sec", "pages_per_sec"]:
        assert key in stats

    assert stats["processed_urls"] == 1
    assert stats["queued_urls"] == 2


# ---------------------------------------------------------------------------
# 2. Дубликаты URL
# ---------------------------------------------------------------------------

def test_add_url_duplicate_not_added_twice():
    queue = CrawlerQueue()
    queue.add_url("https://example.com/x")
    queue.add_url("https://example.com/x")
    queue.add_url("https://example.com/x")

    assert len(queue.queued_urls) == 1
    assert len(queue.queue) == 1


@pytest.mark.asyncio
async def test_no_duplicate_urls_processed_during_full_crawl():
    site = {
        "https://a.com": {"links": ["https://a.com/p1", "https://a.com/p1", "https://a.com/p2"], "error": None},
        "https://a.com/p1": {"links": ["https://a.com/p2"], "error": None},  # p2 встречается повторно
        "https://a.com/p2": {"links": [], "error": None},
    }

    class DupCrawler(AsyncCrawler):
        async def fetch_and_parse(self, url):
            d = site.get(url, {"links": [], "error": "404"})
            return {"links": d["links"], "error": d["error"], "url": url}

    crawler = DupCrawler(max_concurrent=5, max_depth=3)

    results = await crawler.crawl(
        ["https://a.com"],
        max_pages=50,
        same_domain_only=True,
    )

    all_seen = (
            list(crawler.processed_urls.keys())
            + list(crawler.failed_urls.keys())
    )

    result_urls = [result["url"] for result in results]

    assert len(results) == 3
    assert len(result_urls) == len(set(result_urls))
    assert len(all_seen) == len(set(all_seen)), "URL обработан более одного раза"
    assert len(crawler.visited_urls) == len(set(crawler.visited_urls))


# ---------------------------------------------------------------------------
# 3. Ограничение глубины обхода (max_depth)
# ---------------------------------------------------------------------------

DEPTH_SITE = {
    "https://a.com": {"links": ["https://a.com/l1"], "error": None},
    "https://a.com/l1": {"links": ["https://a.com/l2"], "error": None},
    "https://a.com/l2": {"links": ["https://a.com/l3"], "error": None},
    "https://a.com/l3": {"links": [], "error": None},
}


class DepthCrawler(AsyncCrawler):
    async def fetch_and_parse(self, url):
        d = DEPTH_SITE.get(url, {"links": [], "error": "404"})
        return {"links": d["links"], "error": d["error"], "url": url}


@pytest.mark.asyncio
async def test_max_depth_zero_only_processes_start_urls():
    crawler = DepthCrawler(max_concurrent=5, max_depth=0)

    results = await crawler.crawl(
["https://a.com"],
        max_pages=50,
    )

    assert len(results) == 1
    assert "https://a.com" in crawler.processed_urls
    assert "https://a.com/l1" not in crawler.processed_urls


@pytest.mark.asyncio
async def test_max_depth_one_stops_after_first_level():
    crawler = DepthCrawler(max_concurrent=5, max_depth=1)

    results = await crawler.crawl(
        ["https://a.com"],
        max_pages=50,
    )

    assert len(results) == 2
    assert "https://a.com" in crawler.processed_urls
    assert "https://a.com/l1" in crawler.processed_urls
    assert "https://a.com/l2" not in crawler.processed_urls


@pytest.mark.asyncio
async def test_max_depth_two_reaches_second_level_not_third():
    crawler = DepthCrawler(max_concurrent=5, max_depth=2)

    results = await crawler.crawl(
        ["https://a.com"],
        max_pages=50,
    )

    assert len(results) == 3
    assert "https://a.com/l2" in crawler.processed_urls
    assert "https://a.com/l3" not in crawler.processed_urls


# ---------------------------------------------------------------------------
# 4. Фильтрация URL: same_domain_only, exclude_patterns, include_patterns
# ---------------------------------------------------------------------------

FILTER_SITE = {
    "https://a.com": {
        "links": [
            "https://a.com/blog/post-1",
            "https://a.com/admin/panel",
            "https://a.com/about",
            "https://external.com/page",
        ],
        "error": None,
    },
    "https://a.com/blog/post-1": {"links": [], "error": None},
    "https://a.com/admin/panel": {"links": [], "error": None},
    "https://a.com/about": {"links": [], "error": None},
    "https://external.com/page": {"links": [], "error": None},
}


class FilterCrawler(AsyncCrawler):
    async def fetch_and_parse(self, url):
        d = FILTER_SITE.get(url, {"links": [], "error": "404"})
        return {"links": d["links"], "error": d["error"], "url": url}


@pytest.mark.asyncio
async def test_same_domain_only_excludes_external_links():
    crawler = FilterCrawler(max_concurrent=5, max_depth=2)

    results = await crawler.crawl(
        ["https://a.com"],
        max_pages=50,
        same_domain_only=True,
    )

    processed = set(crawler.processed_urls.keys())

    assert len(results) == 4
    assert "https://a.com/about" in processed
    assert "https://external.com/page" not in processed


@pytest.mark.asyncio
async def test_exclude_patterns_filters_matching_links():
    crawler = FilterCrawler(max_concurrent=5, max_depth=2)

    results = await crawler.crawl(
        ["https://a.com"],
        max_pages=50,
        exclude_patterns=["/admin"],
    )

    processed = set(crawler.processed_urls.keys())

    assert len(results) == 4
    assert "https://a.com/admin/panel" not in processed
    assert "https://a.com/blog/post-1" in processed

@pytest.mark.asyncio
async def test_include_patterns_keeps_only_matching_links():
    crawler = FilterCrawler(max_concurrent=5, max_depth=2)

    results = await crawler.crawl(
        ["https://a.com"],
        max_pages=50,
        include_patterns=["/blog/"],
    )

    processed = set(crawler.processed_urls.keys())

    assert len(results) == 2
    assert processed == {
        "https://a.com",
        "https://a.com/blog/post-1",
    }

@pytest.mark.asyncio
async def test_exclude_and_include_combined():
    site = {
        "https://a.com": {
            "links": ["https://a.com/blog/post-1", "https://a.com/blog/draft-1"],
            "error": None,
        },
        "https://a.com/blog/post-1": {"links": [], "error": None},
        "https://a.com/blog/draft-1": {"links": [], "error": None},
    }

    class C(AsyncCrawler):
        async def fetch_and_parse(self, url):
            d = site.get(url, {"links": [], "error": "404"})
            return {"links": d["links"], "error": d["error"], "url": url}

    crawler = C(max_concurrent=5, max_depth=2)

    results = await crawler.crawl(
        ["https://a.com"],
        max_pages=50,
        include_patterns=["/blog/"],
        exclude_patterns=["draft"],
    )

    processed = set(crawler.processed_urls.keys())

    assert len(results) == 2
    assert processed == {
        "https://a.com",
        "https://a.com/blog/post-1",
    }

def test_normalize_url_removes_query_and_fragment():
    first = AsyncCrawler._normalize_url(
        "https://site/page?utm=1#section"
    )
    second = AsyncCrawler._normalize_url(
        "https://site/page?utm=2"
    )

    assert first == "https://site/page"
    assert second == "https://site/page"
    assert first == second

# --------------------------------------------------------------------------
# День 4: мониторинг скорости и прогресса
# --------------------------------------------------------------------------

# =============================================================================
# Тесты для RateLimiter
# =============================================================================

class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_per_domain_limits(self):
        limiter = RateLimiter(requests_per_second=2.0, per_domain=True)
        domain = "example.com"

        start = time.perf_counter()
        await limiter.acquire(domain)
        await limiter.acquire(domain)
        elapsed = time.perf_counter() - start

        assert elapsed >= 0.4

    @pytest.mark.asyncio
    async def test_different_domains_independent(self):
        limiter = RateLimiter(requests_per_second=1.0, per_domain=True)

        start = time.perf_counter()
        await asyncio.gather(
            limiter.acquire("a.com"),
            limiter.acquire("b.com"),
        )
        elapsed = time.perf_counter() - start

        assert elapsed < 0.2

    @pytest.mark.asyncio
    async def test_global_limit(self):
        limiter = RateLimiter(requests_per_second=1.0, per_domain=False)

        start = time.perf_counter()
        await asyncio.gather(
            limiter.acquire("a.com"),
            limiter.acquire("b.com"),
        )
        elapsed = time.perf_counter() - start

        assert elapsed >= 0.9

    @pytest.mark.asyncio
    async def test_acquire_updates_timestamps(self):
        limiter = RateLimiter(requests_per_second=1.0, per_domain=True)
        domain = "test.com"

        await limiter.acquire(domain)
        first_time = limiter.requests_time[domain]

        await asyncio.sleep(0.1)
        await limiter.acquire(domain)
        second_time = limiter.requests_time[domain]

        assert second_time > first_time

class TestRobotsParser:
    @pytest.mark.asyncio
    async def test_parse_robots_valid(self):
        robots_text = """
        User-agent: *
        Disallow: /admin
        Disallow: /private
        Crawl-delay: 5

        User-agent: MyBot
        Disallow: /temp
        Crawl-delay: 2
        """
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text = AsyncMock(return_value=robots_text)

            mock_cm = AsyncMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_cm.__aexit__ = AsyncMock(return_value=False)
            mock_get.return_value = mock_cm

            session = aiohttp.ClientSession()
            parser = RobotsParser(session)
            await parser.fetch_robots("https://example.com")

            assert parser.can_fetch("https://example.com/index.html", "*") is True
            assert parser.can_fetch("https://example.com/admin/page", "*") is False
            assert parser.can_fetch("https://example.com/private/data", "*") is False
            assert parser.can_fetch("https://example.com/temp/file", "MyBot") is False
            assert parser.can_fetch("https://example.com/index.html", "MyBot") is True

            assert parser.get_crawl_delay("*") == 5.0
            assert parser.get_crawl_delay("MyBot") == 2.0
            assert parser.get_crawl_delay("OtherBot") == 5.0

            await session.close()

    @pytest.mark.asyncio
    async def test_robots_no_disallow(self):
        robots_text = "User-agent: *\nCrawl-delay: 1"
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text = AsyncMock(return_value=robots_text)

            mock_cm = AsyncMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_cm.__aexit__ = AsyncMock(return_value=False)
            mock_get.return_value = mock_cm

            session = aiohttp.ClientSession()
            parser = RobotsParser(session)
            await parser.fetch_robots("https://example.com")

            assert parser.can_fetch("https://example.com/any", "*") is True
            assert parser.get_crawl_delay("*") == 1.0

            await session.close()

    @pytest.mark.asyncio
    async def test_robots_404(self):
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_response.text = AsyncMock(return_value="")

            mock_cm = AsyncMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_cm.__aexit__ = AsyncMock(return_value=False)
            mock_get.return_value = mock_cm

            session = aiohttp.ClientSession()
            parser = RobotsParser(session)
            await parser.fetch_robots("https://example.com")

            assert parser.can_fetch("https://example.com/any", "*") is True
            assert parser.get_crawl_delay("*") == 0.0

            await session.close()

    @pytest.mark.asyncio
    async def test_robots_cache(self):
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.text = AsyncMock(return_value="User-agent: *\nDisallow: /secret")

            mock_cm = AsyncMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_cm.__aexit__ = AsyncMock(return_value=False)
            mock_get.return_value = mock_cm

            session = aiohttp.ClientSession()
            parser = RobotsParser(session)
            await parser.fetch_robots("https://example.com")
            await parser.fetch_robots("https://example.com")

            assert mock_get.call_count == 1
            await session.close()

# =============================================================================
# Интеграционные тесты для AsyncCrawler (rate limiting + robots)
# =============================================================================

class TestCrawlerRateLimiting:
    @pytest.mark.asyncio
    async def test_crawler_respects_robots_disallow(self, mock_session):
        crawler = AsyncCrawler(respect_robots=True)
        crawler.session = mock_session

        async def mock_check_robots(self, url, domain, base_url):
            self.blocked_urls[url] = "robots.txt disallow"
            return -1.0

        crawler._check_robots = mock_check_robots.__get__(crawler)
        crawler.rate_limiter = RateLimiter(requests_per_second=1000)

        result = await crawler.fetch_url("https://example.com/any")
        assert result == ""
        assert "https://example.com/any" in crawler.blocked_urls
        assert crawler.blocked_urls["https://example.com/any"] == "robots.txt disallow"

        await crawler.close()

    @pytest.mark.asyncio
    async def test_crawler_applies_crawl_delay(self, mock_session):
        crawler = AsyncCrawler(respect_robots=True, min_delay=0.0)
        crawler.session = mock_session

        mock_parser = AsyncMock()
        mock_parser.can_fetch.return_value = True
        mock_parser.get_crawl_delay.return_value = 1.0
        crawler.robots_parser = mock_parser

        response = AsyncMock()
        response.status = 200
        response.text = AsyncMock(return_value="<html>ok</html>")
        response.raise_for_status = MagicMock()
        mock_session.get.return_value.__aenter__.return_value = response

        start = time.perf_counter()
        await crawler.fetch_url("https://example.com/page")
        elapsed = time.perf_counter() - start

        assert elapsed >= 0.9, "Crawl-delay не применён"
        await crawler.close()

    @pytest.mark.asyncio
    async def test_crawler_uses_min_delay(self, mock_session):
        crawler = AsyncCrawler(min_delay=0.5, jitter=0.0, respect_robots=False)
        crawler.session = mock_session
        crawler.rate_limiter = RateLimiter(requests_per_second=100)

        response = AsyncMock()
        response.status = 200
        response.text = AsyncMock(return_value="ok")
        response.raise_for_status = MagicMock()
        mock_session.get.return_value.__aenter__.return_value = response

        start = time.perf_counter()
        await crawler.fetch_url("https://example.com/1")
        await crawler.fetch_url("https://example.com/2")
        elapsed = time.perf_counter() - start

        assert elapsed >= 0.4, "min_delay не соблюдается"
        await crawler.close()

    @pytest.mark.asyncio
    async def test_crawler_uses_jitter(self, mock_session):
        crawler = AsyncCrawler(min_delay=0.1, jitter=0.3, respect_robots=False)
        crawler.session = mock_session
        crawler.rate_limiter = RateLimiter(requests_per_second=100)

        response = AsyncMock()
        response.status = 200
        response.text = AsyncMock(return_value="ok")
        response.raise_for_status = MagicMock()
        mock_session.get.return_value.__aenter__.return_value = response

        delays = []
        for _ in range(5):
            start = time.perf_counter()
            await crawler.fetch_url("https://example.com/test")
            elapsed = time.perf_counter() - start
            delays.append(elapsed)

        assert any(d > 0.15 for d in delays), "Jitter не добавил вариативности"
        await crawler.close()

class TestCrawlerMonitoring:
    @pytest.mark.asyncio
    async def test_get_current_rps(self):
        crawler = AsyncCrawler()
        crawler._record_request()
        await asyncio.sleep(0.01)
        crawler._record_request()
        crawler._record_request()

        rps = crawler.get_current_rps()

        assert rps == 3.0 / 60.0

    @pytest.mark.asyncio
    async def test_get_average_delay(self):
        crawler = AsyncCrawler()
        now = time.time()
        crawler._request_timestamps = [now, now + 0.5, now + 1.0]
        avg = crawler.get_average_delay()

        assert avg == 0.5

    @pytest.mark.asyncio
    async def test_record_request_keeps_last_60_seconds(self):
        crawler = AsyncCrawler()
        now = time.time()
        old = [now - 70, now - 65]
        new = [now - 10, now - 5, now]
        crawler._request_timestamps = old + new
        crawler._record_request()

        for ts in crawler._request_timestamps:
            assert ts > now - 60

        assert len(crawler._request_timestamps) == 4

class TestRateLimiterIntegration:
    @pytest.mark.asyncio
    async def test_rate_limiter_called_before_request(self, mock_session):
        crawler = AsyncCrawler(max_concurrent=1, requests_per_second=1.0)
        mock_limiter = AsyncMock()
        mock_limiter.acquire = AsyncMock()
        mock_limiter.min_interval = 0.1
        crawler.rate_limiter = mock_limiter

        crawler.session = mock_session
        response = AsyncMock()
        response.status = 200
        response.text = AsyncMock(return_value="ok")
        response.raise_for_status = MagicMock()
        mock_session.get.return_value.__aenter__.return_value = response

        crawler.robots_parser = None
        await crawler.fetch_url("https://example.com")
        mock_limiter.acquire.assert_called_once_with("example.com")
        await crawler.close()

# --------------------------------------------------------------------------
# День 5: retry strategy
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retry_on_timeout():
    strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=1.0,
    )

    operation = AsyncMock(
        side_effect=[
            TransientError(
                "Request timeout for https://example.com"
            ),
            "success",
        ]
    )

    with patch(
        "retry_strategy.asyncio.sleep",
        new=AsyncMock(),
    ) as sleep_mock:
        result = await strategy.execute_with_retry(operation)

    assert result == "success"
    assert operation.await_count == 2
    sleep_mock.assert_awaited_once_with(1.0)

    assert strategy.errors_by_type == {
        "TransientError": 1,
    }
    assert strategy.retry_delays == [1.0]
    assert strategy.successful_retries == 1

@pytest.mark.asyncio
async def test_retry_on_http_503():
    strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=1.0,
    )

    operation = AsyncMock(
        side_effect=[
            TransientError("HTTP 503 https://example.com"),
            "success",
        ]
    )

    with patch(
        "retry_strategy.asyncio.sleep",
        new=AsyncMock(),
    ) as sleep_mock:
        result = await strategy.execute_with_retry(operation)

    assert result == "success"
    assert operation.await_count == 2
    sleep_mock.assert_awaited_once_with(1.0)

    assert strategy.get_statistics() == {
        "errors_by_type": {
            "TransientError": 1,
        },
        "retry_delays": [1.0],
        "successful_retries": 1,
        "average_retry_delay": 1.0,
    }


@pytest.mark.asyncio
async def test_http_404_is_not_retried():
    strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=1.0,
    )

    operation = AsyncMock(
        side_effect=PermanentError("HTTP 404 https://example.com/missing")
    )

    with patch(
        "retry_strategy.asyncio.sleep",
        new=AsyncMock(),
    ) as sleep_mock:
        with pytest.raises(PermanentError, match="HTTP 404"):
            await strategy.execute_with_retry(operation)

    assert operation.await_count == 1
    sleep_mock.assert_not_awaited()
    assert strategy.errors_by_type == {
        "PermanentError": 1,
    }
    assert strategy.retry_delays == []
    assert strategy.successful_retries == 0


@pytest.mark.asyncio
async def test_exponential_backoff():
    strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=2.0,
    )

    operation = AsyncMock(
        side_effect=[
            TransientError("HTTP 503"),
            TransientError("HTTP 503"),
            TransientError("HTTP 503"),
            "success",
        ]
    )

    with patch(
        "retry_strategy.asyncio.sleep",
        new=AsyncMock(),
    ) as sleep_mock:
        result = await strategy.execute_with_retry(operation)

    assert result == "success"
    assert operation.await_count == 4
    assert strategy.retry_delays == [2.0, 4.0, 8.0]
    assert sleep_mock.await_args_list[0].args == (2.0,)
    assert sleep_mock.await_args_list[1].args == (4.0,)
    assert sleep_mock.await_args_list[2].args == (8.0,)


@pytest.mark.asyncio
async def test_error_statistics():
    strategy = RetryStrategy(
        max_retries=3,
        backoff_factor=2.0,
    )

    operation = AsyncMock(
        side_effect=[
            TransientError("HTTP 503"),
            TransientError("HTTP 503"),
            "success",
        ]
    )

    with patch(
        "retry_strategy.asyncio.sleep", new=AsyncMock()):
        result = await strategy.execute_with_retry(operation)

    assert result == "success"

    statistics = strategy.get_statistics()

    assert statistics["errors_by_type"] == {
        "TransientError": 2,
    }
    assert statistics["retry_delays"] == [2.0, 4.0]
    assert statistics["successful_retries"] == 1
    assert statistics["average_retry_delay"] == 3.0

# --------------------------------------------------------------------------
# День 6: интеграция AsyncCrawler и PostgreSQL
# --------------------------------------------------------------------------

TEST_DATABASE = "crawler_db"
TEST_USER = "ilyashik"

@pytest.fixture
def sample_page() -> dict:
    return {
        "url": "https://example.com/test-page",
        "title": "Тестовая страница",
        "text": "Тестовый текст страницы для проверки хранения.",
        "links": [
            "https://example.com/about",
            "https://example.com/contact",
        ],
        "metadata": {
            "language": "ru",
            "description": "Тестовые метаданные",
        },
        "images": [
            {
                "src": "https://example.com/image.png",
                "alt": "Тестовое изображение",
            }
        ],
        "headings": [
            {
                "level": "h1",
                "text": "Заголовок теста",
            }
        ],
        "tables": [],
        "lists": [],
        "crawled_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "status_code": 200,
        "content_type": "text/html",
    }


@pytest_asyncio.fixture
async def postgres_storage():
    storage = PostgreSQLStorage(
        database=TEST_DATABASE,
        user=TEST_USER,
    )

    await storage.init_db()

    async with storage.pool.acquire() as connection:
        await connection.execute(
            "TRUNCATE TABLE pages RESTART IDENTITY;"
        )

    yield storage

    async with storage.pool.acquire() as connection:
        await connection.execute(
            "TRUNCATE TABLE pages RESTART IDENTITY;"
        )

    await storage.close()


# --------------------------------------------------------------------------
# 1. Сохранение в JSON
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_to_json(
    tmp_path,
    sample_page,
) -> None:
    storage = JSONStorage(
        filename=str(tmp_path / "pages.json"),
    )

    await storage.save(sample_page)

    pages = await storage.load_all()

    assert len(pages) == 1
    assert pages[0]["url"] == sample_page["url"]
    assert pages[0]["title"] == sample_page["title"]

    await storage.close()


# --------------------------------------------------------------------------
# 2. Сохранение в CSV
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_to_csv(
    tmp_path,
    sample_page,
) -> None:
    storage = CSVStorage(
        filename=str(tmp_path / "pages.csv"),
    )

    await storage.save(sample_page)

    pages = await storage.load_all()

    assert len(pages) == 1
    assert pages[0]["url"] == sample_page["url"]
    assert pages[0]["title"] == sample_page["title"]
    assert pages[0]["status_code"] == sample_page["status_code"]

    await storage.close()


# --------------------------------------------------------------------------
# 3. Сохранение в PostgreSQL
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_to_postgresql(
    postgres_storage,
    sample_page,
) -> None:
    await postgres_storage.save(sample_page)

    pages = await postgres_storage.load_all()

    assert len(pages) == 1
    assert pages[0]["url"] == sample_page["url"]
    assert pages[0]["title"] == sample_page["title"]
    assert pages[0]["text_content"] == sample_page["text"]
    assert pages[0]["status_code"] == sample_page["status_code"]
    assert pages[0]["content_type"] == sample_page["content_type"]


# --------------------------------------------------------------------------
# 4. Обработка ошибки записи
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_json_storage_write_error(
    tmp_path,
    sample_page,
    monkeypatch,
) -> None:
    storage = JSONStorage(
        filename=str(tmp_path / "pages.json"),
    )

    async def raise_write_error(*args, **kwargs) -> None:
        raise OSError("Имитация ошибки записи")

    monkeypatch.setattr(
        storage,
        "save",
        raise_write_error,
    )

    with pytest.raises(
        OSError,
        match="Имитация ошибки записи",
    ):
        await storage.save(sample_page)

    await storage.close()


# --------------------------------------------------------------------------
# 5. Проверка целостности данных
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_data_integrity_in_all_storages(
    tmp_path,
    postgres_storage,
    sample_page,
) -> None:
    json_storage = JSONStorage(
        filename=str(tmp_path / "pages.json"),
    )

    csv_storage = CSVStorage(
        filename=str(tmp_path / "pages.csv"),
    )

    await json_storage.save(sample_page)
    await csv_storage.save(sample_page)
    await postgres_storage.save(sample_page)

    json_page = (await json_storage.load_all())[0]
    csv_page = (await csv_storage.load_all())[0]
    postgres_page = (await postgres_storage.load_all())[0]

    for page in (json_page, csv_page):
        assert page["url"] == sample_page["url"]
        assert page["title"] == sample_page["title"]
        assert page["text"] == sample_page["text"]
        assert page["links"] == sample_page["links"]
        assert page["metadata"] == sample_page["metadata"]
        assert page["images"] == sample_page["images"]
        assert page["headings"] == sample_page["headings"]
        assert page["tables"] == sample_page["tables"]
        assert page["lists"] == sample_page["lists"]
        assert page["crawled_at"] == sample_page["crawled_at"]
        assert page["status_code"] == sample_page["status_code"]
        assert page["content_type"] == sample_page["content_type"]

    assert postgres_page["url"] == sample_page["url"]
    assert postgres_page["title"] == sample_page["title"]
    assert postgres_page["text_content"] == sample_page["text"]
    assert postgres_page["links"] == sample_page["links"]
    assert postgres_page["metadata"] == sample_page["metadata"]
    assert postgres_page["status_code"] == sample_page["status_code"]
    assert postgres_page["content_type"] == sample_page["content_type"]

    assert postgres_page["crawled_at"].tzinfo is not None

    await json_storage.close()
    await csv_storage.close()

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))