import logging
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class HTMLParser:
    def __init__(self, parser: str = "lxml", fallback_parser: str = "html.parser"):
        self.parser = parser
        self.fallback_parser = fallback_parser

    def _make_soup(self, html: str,  url: str) -> BeautifulSoup:
        try:
            return BeautifulSoup(html, self.parser)
        except Exception as error:
            logger.warning(
                "Парсер '%s' не смог обработать %s (%s), переключаюсь на '%s'",
                self.parser, url, error, self.fallback_parser,
            )
            return BeautifulSoup(html, self.fallback_parser)

    async def parse_html(self, html: str, url: str) -> dict:
        result: dict = {
            "url": url,
            "title": "",
            "text": "",
            "links": [],
            "metadata": {},
            "images": [],
            "headings": [],
            "tables": [],
            "lists": [],
            "error": None,
        }

        if not html:
            logger.warning("Пустой HTML для %s, парсинг пропущен", url)
            result["error"] = "empty_html"
            return result

        try:
            soup = self._make_soup(html, url)
        except Exception as error:
            logger.warning("Не удалось распарсить HTML для %s: %s", url, error)
            result["error"] = f"parse_failed: {error}"
            return result

        try:
            result["metadata"] = self.extract_metadata(soup)
            result["title"] = result["metadata"].get("title", "")
        except Exception as error:
            logger.warning("Ошибка извлечения метаданных для %s: %s", url, error)
            result["error"] = f"metadata_failed: {error}"

        try:
            result["text"] = self.extract_text(soup)
        except Exception as error:
            logger.warning("Ошибка извлечения текста для %s: %s", url, error)
            result["error"] = f"text_failed: {error}"

        try:
            result["links"] = self.extract_links(soup, url)
        except Exception as error:
            logger.warning("Ошибка извлечения ссылок для %s: %s", url, error)
            result["error"] = f"links_failed: {error}"

        try:
            result["images"] = self.extract_images(soup, url)
        except Exception as error:
            logger.warning("Ошибка извлечения изображений для %s: %s", url, error)

        try:
            result["headings"] = self.extract_headings(soup)
        except Exception as error:
            logger.warning("Ошибка извлечения заголовков для %s: %s", url, error)

        try:
            result["tables"] = self.extract_tables(soup)
        except Exception as error:
            logger.warning("Ошибка извлечения таблиц для %s: %s", url, error)

        try:
            result["lists"] = self.extract_lists(soup)
        except Exception as error:
            logger.warning("Ошибка извлечения списков для %s: %s", url, error)

        return result

    def extract_links(
            self,
            soup: BeautifulSoup,
            base_url: str,
            same_domain_only: bool = False,
    ) -> list:
        links: list[str] = []
        base_domain = urlparse(base_url).netloc

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()

            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            absolute_url = urljoin(base_url, href)

            if not self._is_valid_url(absolute_url):
                continue

            if same_domain_only and urlparse(absolute_url).netloc != base_domain:
                continue

            links.append(absolute_url)

        return list(dict.fromkeys(links))

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> list:
        scope = soup

        if selector:
            found = soup.select_one(selector)
            scope = found if found is not None else soup

        for tag in scope.find_all(["script", "style", "noscript"]):
            tag.decompose()

        text = scope.get_text(separator=" ", strip=True)

        return " ".join(text.split())

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        metadata: dict = {
            "title": "",
            "description": "",
            "keywords": "",
        }

        title_tag = soup.find("title")

        if title_tag and title_tag.string:
            metadata["title"] = title_tag.string.strip()

        description_tag = soup.find("meta", attrs={"name": "description"})

        if description_tag and description_tag.get("content"):
            metadata["description"] = description_tag["content"].strip()

        keywords_tag = soup.find("meta", attrs={"name": "keywords"})

        if keywords_tag and keywords_tag.get("content"):
            metadata["keywords"] = keywords_tag["content"].strip()

        for meta_tag in soup.find_all("meta"):
            prop = meta_tag.get("property", "")
            if prop.startswith("og:") and meta_tag.get("content"):
                key = prop.replace("og:", "og_")
                metadata[key] = meta_tag["content"].strip()

        return metadata

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        images: list[dict] = []

        for tag in soup.find_all("img"):
            src = tag.get("src", "").strip()

            if not src:
                continue

            absolute_src = urljoin(base_url, src)

            if not self._is_valid_url(absolute_src):
                continue

            images.append({
                "src": absolute_src,
                "alt": tag.get("alt", "").strip(),
            })

        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[dict]:
        headings: list[dict] = []

        for level in ("h1", "h2", "h3"):
            for tag in soup.find_all(level):
                text = tag.get_text(strip=True)
                if text:
                    headings.append({"level": level, "text": text})

        return headings

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables: list[list[list[str]]] = []

        for table_tag in soup.find_all("table"):
            rows: list[list[str]] = []
            for row_tag in table_tag.find_all("tr"):
                cells = row_tag.find_all(["td", "th"])
                row = [cell.get_text(strip=True) for cell in cells]
                if row:
                    rows.append(row)
            if rows:
                tables.append(rows)

        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        lists: list[dict] = []

        for list_tag in soup.find_all(["ul", "ol"]):
            items = [
                li.get_text(strip=True)
                for li in list_tag.find_all("li", recursive=False)
                if li.get_text(strip=True)
            ]
            if items:
                lists.append({
                    "type": list_tag.name,
                    "items": items,
                })

        return lists

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        try:
            parsed = urlparse(url)

            return parsed.scheme in ("http", "https") and bool(parsed.netloc)
        except Exception:
            return False