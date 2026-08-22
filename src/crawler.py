import asyncio
import aiohttp
import logging
from urllib.parse import urlparse

from html_parser import HTMLParser
from crawler_queue import CrawlerQueue
from semaphore_manager import SemaphoreManager

logger = logging.getLogger(__name__)

class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
    ):
        if max_concurrent <= 0:
           raise ValueError('max_concurrent must be positive')

        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: aiohttp.ClientSession | None = None
        self.parser = HTMLParser()
        self.semaphore_manager = SemaphoreManager(max_concurrent=max_concurrent)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=30,
                connect=10,
                sock_read=20
            )

            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent,
                limit_per_host=self.max_concurrent,
            )

            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    "User-agent": "AsyncCrawler/1.0"
                }
            )

        return self.session


    async def fetch_url(self, url: str) -> str:
        logger.info('Начало загрузки: %s', url)

        async with self.semaphore:
            try:
                session = await self._get_session()

                async with session.get(url) as response:
                    response.raise_for_status()
                    content = await response.text()

                logger.info(
                    "Успешно загружено: %s, статус: %s",
                    url,
                    response.status
                )

                return content

            except aiohttp.ClientResponseError as error:
                logger.warning(
                    "HTTP-ошибка для %s: %s %s",
                    url,
                    error.status,
                    error.message,
                )
                return ""

            except asyncio.TimeoutError:
                logger.warning(
                    "Таймаут при загрузке: %s",
                    url,
                )
                return ""

            except aiohttp.ClientError as error:
                logger.warning(
                    "Сетевая ошибка для %s: %s",
                    url,
                    type(error).__name__,
                )
                return ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = [
            self.fetch_url(url)
            for url in urls
        ]

        contents = await asyncio.gather(*tasks)

        return dict(zip(urls, contents))

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)

        if not html:
            logger.warning("Не удалось загрузить контент для парсинга: %s", url)

            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
                "headings": [],
                "tables": [],
                "lists": [],
                "error": "fetch_failed",
            }

        parsed = await self.parser.parse_html(html, url)

        return parsed

    async def crawl(
            self,
            start_urls: list[str],
            max_pages: int = 100,
            max_depth: int = 2,
            same_domain_only: bool = False,
            exclude_patterns: list[str] = None,
            include_patterns: list[str] = None,
    ):
        queue = CrawlerQueue()
        depths = {}
        origin_domains = {}

        for url in start_urls:
            queue.add_url(url, 0)
            depths[url] = 0
            origin_domains[url] = urlparse(url).netloc

        in_flight = {}

        def total_done():
            return len(queue.processed_urls) + len(queue.failed_urls)

        while total_done() < max_pages:
            while len(in_flight) < self.max_concurrent and total_done() + len(in_flight) < max_pages:
                url = await queue.get_next()
                if url is None:
                    break

                domain = origin_domains.get(url, urlparse(url).netloc)
                task = asyncio.create_task(
                    self.semaphore_manager.acquire_and_run(domain, self.fetch_and_parse, url)
                )
                in_flight[task] = url

            if not in_flight:
                break

            done, _ = await asyncio.wait(in_flight.keys(), return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                url = in_flight.pop(task)
                current_depth = depths.get(url, 0)
                origin_domain = origin_domains.get(url)

                result = task.result()

                if result is None:
                    queue.mark_failed(url, "request_failed")
                    continue

                error = result.get("error")
                if error:
                    queue.mark_failed(url, error)
                    continue

                queue.mark_processed(url, result)

                for link in result.get("links", []):
                    next_depth = current_depth + 1
                    if next_depth > max_depth:
                        continue
                    if same_domain_only and urlparse(link).netloc != origin_domain:
                        continue
                    if exclude_patterns and any(p in link for p in exclude_patterns):
                        continue
                    if include_patterns and not any(p in link for p in include_patterns):
                        continue

                    queue.add_url(link, 0)
                    depths[link] = next_depth
                    origin_domains[link] = origin_domain

        return queue

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
            logger.info("HTTP-сессия закрыта")