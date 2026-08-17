import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from bs4 import BeautifulSoup

from crawler import AsyncCrawler
from main import load_sequentially
from html_parser import HTMLParser

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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))