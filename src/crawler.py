import asyncio
import aiohttp
import logging
from urllib.parse import urlparse, urlunparse

from html_parser import HTMLParser
from crawler_queue import CrawlerQueue
from semaphore_manager import SemaphoreManager
from rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        max_per_domain: int = 3,
        requests_per_second: float = 1.0,
        rate_limit_per_domain: bool = True,
    ):
        if max_concurrent <= 0:
            raise ValueError("max_concurrent must be positive")

        if max_depth < 0:
            raise ValueError("max_depth must be greater than or equal to zero")

        self.max_concurrent = max_concurrent
        self.max_depth = max_depth

        self.session: aiohttp.ClientSession | None = None
        self.parser = HTMLParser()
        self.semaphore_manager = SemaphoreManager(
            max_concurrent=max_concurrent,
            max_per_domain=max_per_domain,
        )
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=rate_limit_per_domain,
        )

        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}

    @staticmethod
    def _normalize_url(url: str) -> str:
        parsed = urlparse(url)

        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                "",
                "",
            )
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=30,
                connect=10,
                sock_read=20,
            )

            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent,
                limit_per_host=self.semaphore_manager.max_per_domain,
            )

            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": "AsyncCrawler/1.0"},
            )

        return self.session

    async def fetch_url(self, url: str) -> str:
        logger.info("Начало загрузки: %s", url)

        try:
            session = await self._get_session()

            domain = urlparse(url).netloc
            await self.rate_limiter.acquire(domain)

            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.text()

                logger.info(
                    "Успешно загружено: %s, статус: %s",
                    url,
                    response.status,
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
            logger.warning("Таймаут при загрузке: %s", url)
            return ""

        except aiohttp.ClientError as error:
            logger.warning(
                "Сетевая ошибка для %s: %s",
                url,
                type(error).__name__,
            )
            return ""

    async def _fetch_with_limits(self, url: str) -> tuple[str, str]:
        domain = urlparse(url).netloc

        content = await self.semaphore_manager.acquire_and_run(
            domain,
            self.fetch_url,
            url,
        )

        return url, content

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = [
            self._fetch_with_limits(url)
            for url in urls
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        fetched: dict[str, str] = {}

        for url, result in zip(urls, results):
            if isinstance(result, Exception):
                logger.warning(
                    "Не удалось загрузить %s: %s",
                    url,
                    result,
                )
                fetched[url] = ""
                continue

            fetched_url, content = result
            fetched[fetched_url] = content

        return fetched

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)

        if not html:
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

        return await self.parser.parse_html(html, url)

    def _reset_crawl_state(self) -> None:
        self.visited_urls.clear()
        self.failed_urls.clear()
        self.processed_urls.clear()

    def _is_allowed_url(
        self,
        url: str,
        origin_domain: str,
        same_domain_only: bool,
        exclude_patterns: list[str] | None,
        include_patterns: list[str] | None,
    ) -> bool:
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        if same_domain_only and parsed.netloc != origin_domain:
            return False

        if exclude_patterns and any(pattern in url for pattern in exclude_patterns):
            return False

        if include_patterns and not any(pattern in url for pattern in include_patterns):
            return False

        return True

    def _print_progress(self, queue: CrawlerQueue) -> None:
        stats = queue.get_stats()
        done = len(self.processed_urls)

        print(
            f"\r📄 Обработано: {done} | "
            f"⏳ В очереди: {stats['count_queue']} | "
            f"❌ Ошибок: {len(self.failed_urls)} | "
            f"⚡ Скорость: {stats['pages_per_sec']:.2f} стр/сек",
            end="",
            flush=True,
        )

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = False,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> list[dict]:
        if max_pages <= 0:
            return []

        self._reset_crawl_state()

        queue = CrawlerQueue()
        depths: dict[str, int] = {}
        origin_domains: dict[str, str] = {}

        for raw_url in start_urls:
            url = self._normalize_url(raw_url)
            parsed = urlparse(url)

            if parsed.scheme not in {"http", "https"}:
                logger.warning("Пропущен некорректный стартовый URL: %s", url)
                continue

            queue.add_url(url)
            depths[url] = 0
            origin_domains[url] = parsed.netloc

        in_flight: dict[asyncio.Task, str] = {}

        while (
            len(self.processed_urls) + len(self.failed_urls) < max_pages
        ):
            while (
                len(in_flight) < self.max_concurrent
                and len(self.processed_urls)
                + len(self.failed_urls)
                + len(in_flight)
                < max_pages
            ):
                url = await queue.get_next()

                if url is None:
                    break

                if url in self.visited_urls:
                    continue

                self.visited_urls.add(url)

                domain = urlparse(url).netloc
                task = asyncio.create_task(
                    self.semaphore_manager.acquire_and_run(
                        domain,
                        self.fetch_and_parse,
                  url,
                    )
                )
                in_flight[task] = url

            if not in_flight:
                break

            done, _ = await asyncio.wait(
                in_flight,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                url = in_flight.pop(task)
                current_depth = depths[url]
                origin_domain = origin_domains[url]

                try:
                    result = task.result()
                except Exception as error:
                    logger.exception("Ошибка обработки %s", url)
                    self.failed_urls[url] = str(error)
                    queue.mark_failed(url, str(error))
                    continue

                if result is None:
                    self.failed_urls[url] = "request_failed"
                    queue.mark_failed(url, "request_failed")
                    continue

                error = result.get("error")

                if error:
                    self.failed_urls[url] = error
                    queue.mark_failed(url, error)
                else:
                    self.processed_urls[url] = result
                    queue.mark_processed(url, result)

                    next_depth = current_depth + 1

                    if next_depth <= self.max_depth:
                        for raw_link in result.get("links", []):
                            link = self._normalize_url(raw_link)

                            if not self._is_allowed_url(
                                url=link,
                                origin_domain=origin_domain,
                                same_domain_only=same_domain_only,
                                exclude_patterns=exclude_patterns,
                                include_patterns=include_patterns,
                            ):
                                continue

                            if link in self.visited_urls:
                                continue

                            old_depth = depths.get(link)

                            if old_depth is None or next_depth < old_depth:
                                depths[link] = next_depth
                                origin_domains.setdefault(link, origin_domain)
                                queue.add_url(link)

                    self._print_progress(queue)

        return list(self.processed_urls.values())

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()

        logger.info("HTTP-сессия закрыта")