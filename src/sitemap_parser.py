import aiohttp
import xml.etree.ElementTree as ET

class SitemapParser:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        visited: set[str] = set()

        return await self._fetch_recursive(sitemap_url, visited)

    async def _fetch_recursive(
            self,
            sitemap_url: str,
            visited: set[str],
    ) -> list[str]:
        if sitemap_url in visited:
            return []

        visited.add(sitemap_url)

        async with self.session.get(sitemap_url) as response:
            response.raise_for_status()
            xml_text = await response.text()

        root = ET.fromstring(xml_text)

        namespace = {
            "sm": "http://www.sitemaps.org/schemas/sitemap/0.9"
        }

        urls: list[str] = []

        if root.tag == "{http://www.sitemaps.org/schemas/sitemap/0.9}urlset":
            for location in root.findall("sm:url/sm:loc", namespace):
                if location.text:
                    urls.append(location.text.strip())

            return urls

        if root.tag == "{http://www.sitemaps.org/schemas/sitemap/0.9}sitemapindex":
            for location in root.findall("sm:sitemap/sm:loc", namespace):
                if not location.text:
                    continue

                child_sitemap_url = location.text.strip()

                child_urls = await self._fetch_recursive(
                    child_sitemap_url,
                    visited,
                )

                urls.extend(child_urls)

            return urls

        raise ValueError(f"Неизвестный формат sitemap: {root.tag}")