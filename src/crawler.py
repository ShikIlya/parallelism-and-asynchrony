import asyncio
import aiohttp
import logging
from urllib.parse import urlparse, urlunparse
import random
import time

from html_parser import HTMLParser
from crawler_queue import CrawlerQueue
from semaphore_manager import SemaphoreManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        max_per_domain: int = 3,
        requests_per_second: float = 1.0,
        rate_limit_per_domain: bool = True,
        respect_robots: bool = False,
        user_agent: str = "AsyncCrawler/1.0",
        min_delay: float = 0.0,
        jitter: float = 0.0,
        backoff_base: float = 0,
        backoff_max: float = 0,
        backoff_max_retries: int = 0,
    ):
        if max_concurrent <= 0:
            raise ValueError("max_concurrent must be positive")

        if max_depth < 0:
            raise ValueError("max_depth must be greater than or equal to zero")

        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.min_delay = min_delay
        self.jitter = jitter
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.backoff_max_retries = backoff_max_retries

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
        self.robots_parser: RobotsParser | None = None

        self.blocked_urls: dict[str, str] = {}
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.error_counts: dict[str, int] = {}
        self._request_timestamps: list[float] = []
        self._start_time: float = 0.0
        self._total_requests: int = 0

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
                headers={"User-Agent": self.user_agent},
            )

            self.robots_parser = RobotsParser(self.session)

        return self.session

    async def fetch_url(self, url: str) -> str:
        logger.info("Начало загрузки: %s", url)

        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        domain = parsed.netloc

        retries = 0

        while retries <= self.backoff_max_retries:
            try:
                session = await self._get_session()

                crawl_delay = await self._check_robots(url, domain, base_url)

                if crawl_delay < 0:
                    return ""

                await self.rate_limiter.acquire(domain)
                await self._wait_with_delay(crawl_delay)

                async with session.get(url) as response:
                    response.raise_for_status()
                    content = await response.text()

                    logger.info(
                        "Успешно загружено: %s, статус: %s",
                        url,
                        response.status,
                    )

                    self._reset_error_count(domain)

                    return content

            except aiohttp.ClientResponseError as error:
                if 400 <= error.status < 500:
                    logger.warning(
                        "HTTP-ошибка для %s: %s %s (без повтора)",
                        url,
                        error.status,
                        error.message,
                    )
                    return ""

                logger.warning(
                    "HTTP-ошибка для %s: %s %s (попытка %d)",
                    url,
                    error.status,
                    error.message,
                    retries + 1,
                )

                await self._wait_with_backoff(domain)
                retries += 1

            except asyncio.TimeoutError:
                logger.warning(
                    "Таймаут при загрузке: %s (попытка %d)",
                    url,
                    retries + 1,
                )

                await self._wait_with_backoff(domain)
                retries += 1

            except aiohttp.ClientError as error:
                logger.warning(
                    "Сетевая ошибка для %s: %s (попытка %d)",
                    url,
                    type(error).__name__,
                    retries + 1,
                )

                await self._wait_with_backoff(domain)
                retries += 1

        logger.error("Превышено количество попыток для %s", url)
        self.failed_urls[url] = "max_retries_exceeded"

        return ""

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
        self._start_time = time.time()

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

    def _calculate_delay(self, crawl_delay: float) -> float:
        base_delay = self.rate_limiter.min_interval

        if crawl_delay > 0:
            base_delay = max(base_delay, crawl_delay)

        if self.min_delay > 0:
            base_delay = max(base_delay, self.min_delay)

        if self.jitter > 0:
            jitter_value = random.uniform(0, self.jitter)
            base_delay += jitter_value

        return base_delay

    async def _wait_with_delay(self, crawl_delay: float) -> None:
        delay = self._calculate_delay(crawl_delay)

        if delay > 0:
            await asyncio.sleep(delay)

    async def _check_robots(self, url: str, domain: str, base_url: str) -> float:
        if not self.respect_robots or self.robots_parser is None:
            return 0.0

        await self.rate_limiter.acquire(domain)
        await self.robots_parser.fetch_robots(base_url)

        if not self.robots_parser.can_fetch(url, self.user_agent):
            logger.warning("URL запрещён robots.txt: %s", url)
            self.blocked_urls[url] = "robots.txt disallow"

            return -1.0

        return self.robots_parser.get_crawl_delay(self.user_agent)

    def _reset_error_count(self, domain: str) -> None:
        self.error_counts[domain] = 0

    def _apply_backoff(self, domain: str) -> float:
        self.error_counts[domain] = self.error_counts.get(domain, 0) + 1

        if self.error_counts[domain] > self.backoff_max_retries:
            return 0.0

        backoff_delay = min(
            self.backoff_base * (2 ** self.error_counts[domain]),
            self.backoff_max,
        )

        logger.info(
            "Backoff для %s: %.2f сек (попытка %d)",
            domain,
            backoff_delay,
            self.error_counts[domain],
        )

        return backoff_delay

    async def _wait_with_backoff(self, domain: str) -> None:
        backoff_delay = self._apply_backoff(domain)

        if backoff_delay > 0:
            await asyncio.sleep(backoff_delay)

    async def _fetch_with_limits(self, url: str) -> tuple[str, str]:
        domain = urlparse(url).netloc

        content = await self.semaphore_manager.acquire_and_run(
            domain,
            self.fetch_url,
            url,
        )

        return url, content

    def _reset_crawl_state(self) -> None:
        self.visited_urls.clear()
        self.failed_urls.clear()
        self.processed_urls.clear()
        self.blocked_urls.clear()
        self.error_counts.clear()

        self._request_timestamps = []
        self._total_requests = 0
        self._start_time = 0.0

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

    def _record_request(self) -> None:
        current_time = time.time()
        self._request_timestamps.append(current_time)
        self._total_requests += 1

        cutoff = current_time - 60.0
        self._request_timestamps = [
            ts for ts in self._request_timestamps
            if ts > cutoff
        ]

    def get_current_rps(self) -> float:
        if not self._request_timestamps:
            return 0.0

        return len(self._request_timestamps) / 60.0

    def get_average_delay(self) -> float:
        if len(self._request_timestamps) < 2:
            return 0.0

        total_delay = 0.0

        for i in range(1, len(self._request_timestamps)):
            total_delay += self._request_timestamps[i] - self._request_timestamps[i - 1]

        return total_delay / (len(self._request_timestamps) - 1)

    def get_elapsed_time(self) -> float:
        if self._start_time == 0.0:
            return 0.0

        return time.time() - self._start_time

    def _print_progress(self, queue: CrawlerQueue) -> None:
        stats = queue.get_stats()
        done = len(self.processed_urls)

        self._record_request()

        current_rps = self.get_current_rps()
        avg_delay = self.get_average_delay()
        elapsed = self.get_elapsed_time()

        print(
            f"\r📄 Обработано: {done} | "
            f"⏳ В очереди: {stats['count_queue']} | "
            f"❌ Ошибок: {len(self.failed_urls)} | "
            f"🚫 Robots: {len(self.blocked_urls)} | "
            f"⚡ RPS: {current_rps:.2f} | "
            f"⏱ Задержка: {avg_delay:.2f}с | "
            f"⏰ Время: {elapsed:.1f}с",
            end="",
            flush=True,
        )