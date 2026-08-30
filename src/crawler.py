import asyncio
import aiohttp
import logging
from urllib.parse import urlparse, urlunparse
import random
import time
from datetime import datetime, timezone

from html_parser import HTMLParser
from crawler_queue import CrawlerQueue
from semaphore_manager import SemaphoreManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from retry_strategy import RetryStrategy
from exceptions import TransientError, PermanentError, NetworkError, ParseError
from data_storage import DataStorage

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
        backoff_factor: float = 2.0,
        max_retries: int = 3,
        retry_on: list = None,
        retry_limits: dict[type[Exception], int] | None = None,
        backoff_factors: dict[type[Exception], float] | None = None,
        total_timeout: float = 30.0,
        connect_timeout: float = 10.0,
        read_timeout: float = 20.0,
        timeout_backoff_factor: float = 1.5,
        max_timeout: float = 120.0,
        storage: DataStorage | None = None,
    ):
        if max_concurrent <= 0:
            raise ValueError("max_concurrent must be positive")

        if max_depth < 0:
            raise ValueError("max_depth must be greater than or equal to zero")

        if total_timeout <= 0:
            raise ValueError("total_timeout must be positive")

        if connect_timeout <= 0:
            raise ValueError("connect_timeout must be positive")

        if read_timeout <= 0:
            raise ValueError("read_timeout must be positive")

        if timeout_backoff_factor < 1:
            raise ValueError(
                "timeout_backoff_factor must be greater than or equal to 1"
            )

        if max_timeout <= 0:
            raise ValueError('max_timeout must be positive')

        if connect_timeout > total_timeout:
            raise ValueError(
                "connect_timeout cannot be greater than total_timeout"
            )

        if read_timeout > total_timeout:
            raise ValueError(
                "read_timeout cannot be greater than total_timeout"
            )
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
        self.retry_strategy = RetryStrategy(
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            retry_on=retry_on,
            retry_limits=retry_limits,
            backoff_factors=backoff_factors,
        )

        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.min_delay = min_delay
        self.jitter = jitter
        self.total_timeout = total_timeout
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.timeout_backoff_factor = timeout_backoff_factor
        self.max_timeout = max_timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.storage = storage

        self.blocked_urls: dict[str, str] = {}
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.errors_by_type: dict[str, int] = {}
        self.permanent_error_urls: set[str] = set()
        self._request_timestamps: list[float] = []
        self._start_time: float = 0.0
        self._total_requests: int = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(
                total=self.total_timeout,
                connect=self.connect_timeout,
                sock_read=self.read_timeout,
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

    async def _fetch_url_once(
            self,
            url: str,
            attempt: int = 0,
    ) -> str:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        domain = parsed.netloc

        session = await self._get_session()

        crawl_delay = await self._check_robots(url, domain, base_url)

        if crawl_delay < 0:
            return ""

        await self.rate_limiter.acquire(domain)
        await self._wait_with_delay(crawl_delay)

        try:
            self._record_request()

            timeout_multiplier = self.timeout_backoff_factor ** attempt

            total_timeout = min(
                self.total_timeout * timeout_multiplier,
                self.max_timeout,
            )

            connect_timeout = min(
                self.connect_timeout * timeout_multiplier,
                total_timeout,
            )

            read_timeout = min(
                self.read_timeout * timeout_multiplier,
                total_timeout,
            )

            request_timeout = aiohttp.ClientTimeout(
                total=total_timeout,
                connect=connect_timeout,
                sock_read=read_timeout,
            )

            async with session.get(url, timeout=request_timeout) as response:
                status = response.status

                if status == 401 or status == 403 or status == 404:
                    raise PermanentError(f"HTTP {status} {url}")
                elif status == 429:
                    raise TransientError(f"HTTP {status} {url}")
                elif 400 <= status < 500:
                    raise PermanentError(f"HTTP {status} {url}")
                elif 500 <= status < 600:
                    raise TransientError(f"HTTP {status} {url}")

                content = await response.text()

                logger.info(
                "Успешно загружено: %s, статус: %s",
                url,
                      response.status,
                )

                return content

        except asyncio.TimeoutError as error:
            raise TransientError(f"Request timeout for {url}: {error}") from error
        except aiohttp.ClientError as error:
            raise NetworkError(f"Network error for {url}: {error}") from error

    async def fetch_url(self, url: str) -> str:
        logger.info("Начало загрузки: %s", url)

        attempt = 0

        async def fetch_attempt() -> str:
            nonlocal attempt

            current_attempt = attempt
            attempt += 1

            return await self._fetch_url_once(
                url,
                attempt=current_attempt,
            )

        try:
            content = await self.retry_strategy.execute_with_retry(
                fetch_attempt
            )

            logger.info("Загрузка завершена успешно: %s", url)

            return content

        except PermanentError as error:
            self.failed_urls[url] = str(error)
            self.permanent_error_urls.add(url)

            logger.warning(
           "Постоянная ошибка при загрузке %s: %s. Повтор не будет выполнен.",
          url,
                error,
            )

            return ""

        except TransientError as error:
            self.failed_urls[url] = str(error)

            logger.error(
           "Не удалось загрузить %s: исчерпан лимит повторов (%d). Ошибка: %s",
          url,
                self.retry_strategy.max_retries,
                error,
            )

            return ""

        except NetworkError as error:
            self.failed_urls[url] = str(error)

            logger.error(
           "Не удалось загрузить %s: исчерпан лимит повторов (%d). Сетевая ошибка: %s",
          url,
                self.retry_strategy.max_retries,
                error,
            )

            return ""

        except asyncio.CancelledError:
            raise

        except Exception as error:
            self.failed_urls[url] = str(error)

            logger.exception(
                "Непредвиденная ошибка при загрузке %s: %s",
                url,
                error,
            )

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

        try:
            result = await self.parser.parse_html(html, url)

        except Exception as error:
            parse_error = ParseError(f"Parse error for {url}: {error}")

            self._record_error(parse_error)

            logger.error(
                "Ошибка парсинга URL %s: %s",
                url,
                parse_error,
            )

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
                "error": str(parse_error),
            }

        result["crawled_at"] = datetime.now(
            timezone.utc
        ).isoformat()
        result["status_code"] = 200
        result["content_type"] = "text/html"

        if self.storage is not None:
            try:
                await self.storage.save(result)

            except Exception:
                logger.exception(
                    "Ошибка сохранения данных URL %s",
                    url,
                )

        return result

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

        if self.storage is not None:
            await self.storage.close()

        logger.info("HTTP-сессия и storage закрыты")


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
        self.permanent_error_urls.clear()
        self.errors_by_type.clear()
        self.retry_strategy.reset_statistics()

        self._request_timestamps = []
        self._total_requests = 0
        self._start_time = 0.0

    def _record_error(self, error: Exception) -> None:
        error_type = type(error).__name__

        self.errors_by_type[error_type] = (
                self.errors_by_type.get(error_type, 0) + 1
        )

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

    def _get_all_errors_by_type(self) -> dict[str, int]:
        all_errors = dict(
            self.retry_strategy.errors_by_type
        )

        for error_type, count in self.errors_by_type.items():
            all_errors[error_type] = (
                    all_errors.get(error_type, 0) + count
            )

        return all_errors

    def get_error_statistics(self) -> dict:
        return {
            **self.retry_strategy.get_statistics(),
            "errors_by_type": self._get_all_errors_by_type(),
            "permanent_error_urls": sorted(self.permanent_error_urls),
            "failed_urls": dict(self.failed_urls),
        }