import asyncio
import aiohttp
import logging
from urllib.parse import urlparse, urlunparse
import random
import time
from datetime import datetime, timezone
import json
from pathlib import Path

from html_parser import HTMLParser
from crawler_queue import CrawlerQueue
from semaphore_manager import SemaphoreManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from retry_strategy import RetryStrategy
from exceptions import TransientError, PermanentError, NetworkError, ParseError
from data_storage import DataStorage
from json_storage import JSONStorage
from csv_storage import CSVStorage
from postgresql_storage import PostgreSQLStorage
from multi_storage import MultiStorage
from crawler_stats import CrawlerStats
from sitemap_parser import SitemapParser

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
        self.status_codes: dict[int, int] = {}

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

    async def _fetch_response_once(
        self,
        url: str,
        attempt: int = 0,
    ) -> dict:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        domain = parsed.netloc

        session = await self._get_session()

        crawl_delay = await self._check_robots(
            url,
            domain,
            base_url,
        )

        if crawl_delay < 0:
            return {
                "html": "",
                "status_code": None,
                "content_type": None,
                "error": "robots_disallowed"
            }

        delay = self._calculate_delay(crawl_delay)

        await self.rate_limiter.acquire(
            domain,
            minimum_delay=delay,
        )

        try:
            self._record_request()

            timeout_multiplier = (
                self.timeout_backoff_factor ** attempt
            )

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

            async with session.get(
                url,
                timeout=request_timeout,
            ) as response:
                status = response.status
                content_type = response.content_type

                self._record_status_code(status)

                self._raise_for_http_status(
                    status,
                    url,
                )

                html = await response.text()

                logger.info(
                    "Успешно загружено: %s, статус: %s",
                    url,
                    status,
                )

                return {
                    "html": html,
                    "status_code": status,
                    "content_type": content_type,
                    "error": None
                }

        except asyncio.TimeoutError as error:
            raise TransientError(
                f"Request timeout for {url}: {error}"
            ) from error

        except aiohttp.ClientResponseError as error:
            self._raise_for_http_status(
                error.status,
                url
            )

            raise NetworkError(
                f"HTTP response error for {url}: {error}"
            ) from error

        except aiohttp.ClientError as error:
            raise NetworkError(
                f"Network error for {url}: {error}"
            ) from error

    @staticmethod
    def _raise_for_http_status(
            status: int | None,
            url: str,
    ) -> None:
        if status is None:
            return

        if status == 429:
            raise TransientError(
                f"HTTP {status} {url}"
            )

        if 400 <= status < 500:
            raise PermanentError(
                f"HTTP {status} {url}"
            )

        if 500 <= status < 600:
            raise TransientError(
                f"HTTP {status} {url}"
            )

    async def _fetch_response(
        self,
        url: str,
    ) -> dict:
        logger.info("Начало загрузки: %s", url)

        attempt = 0

        async def fetch_attempt() -> dict:
            nonlocal attempt

            current_attempt = attempt
            attempt += 1

            return await self._fetch_response_once(
                url,
                attempt=current_attempt,
            )

        try:
            response_data = (
                await self.retry_strategy.execute_with_retry(
                    fetch_attempt
                )
            )

            logger.info(
                "Загрузка завершена успешно: %s",
                url,
            )

            return response_data

        except PermanentError as error:
            self.failed_urls[url] = str(error)
            self.permanent_error_urls.add(url)

            logger.warning(
                "Постоянная ошибка при загрузке %s: %s. "
                "Повтор не будет выполнен.",
                url,
                error,
            )

        except TransientError as error:
            self.failed_urls[url] = str(error)

            logger.error(
                "Не удалось загрузить %s: исчерпан лимит "
                "повторов (%d). Ошибка: %s",
                url,
                self.retry_strategy.max_retries,
                error,
            )

        except NetworkError as error:
            self.failed_urls[url] = str(error)

            logger.error(
                "Не удалось загрузить %s: исчерпан лимит "
                "повторов (%d). Сетевая ошибка: %s",
                url,
                self.retry_strategy.max_retries,
                error,
            )

        except asyncio.CancelledError:
            raise

        except Exception as error:
            self.failed_urls[url] = str(error)

            logger.exception(
                "Непредвиденная ошибка при загрузке %s: %s",
                url,
                error,
            )

        return {
            "html": "",
            "status_code": None,
            "content_type": None,
            "error": self.failed_urls.get(url, "fetch_failed")
        }

    async def fetch_url(self, url: str) -> str:
        response_data = await self._fetch_response(url)

        return response_data["html"]

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
        response_data = await self._fetch_response(url)

        html = response_data["html"]

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
                "error": response_data.get('error', 'fetch_failed'),
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
        result["status_code"] = response_data["status_code"]
        result["content_type"] = response_data["content_type"]

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
        self._start_time = time.monotonic()

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
                parsed.scheme.lower(),
                parsed.netloc.lower(),
                parsed.path,
                parsed.params,
                parsed.query,
                "",
            )
        )

    def _calculate_delay(self, crawl_delay: float) -> float:
        base_delay = 0.0

        if crawl_delay > 0:
            base_delay = crawl_delay

        if self.min_delay > 0:
            base_delay = max(
                base_delay,
                self.min_delay,
            )

        if self.jitter > 0:
            jitter_value = random.uniform(
                0,
                self.jitter,
            )
            base_delay += jitter_value

        return base_delay

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
        self.status_codes.clear()
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

    def _record_status_code(self, status: int) -> None:
        self.status_codes[status] = (
                self.status_codes.get(status, 0) + 1
        )

    def _record_request(self) -> None:
        current_time = time.monotonic()

        self._request_timestamps.append(current_time)
        self._total_requests += 1

        cutoff = current_time - 60.0

        self._request_timestamps = [
            timestamp
            for timestamp in self._request_timestamps
            if timestamp > cutoff
        ]

    def get_current_rps(self) -> float:
        if not self._request_timestamps:
            return 0.0

        now = time.monotonic()
        cutoff = now - 60.0

        self._request_timestamps = [
            timestamp
            for timestamp in self._request_timestamps
            if timestamp > cutoff
        ]

        if not self._request_timestamps:
            return 0.0

        first_request_time = self._request_timestamps[0]
        window_start = max(
            first_request_time,
            cutoff,
        )

        elapsed = now - window_start

        if elapsed <= 0:
            return 0.0

        return len(self._request_timestamps) / elapsed

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

        return time.monotonic() - self._start_time

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


class AdvancedCrawler(AsyncCrawler):
    @classmethod
    def from_config(
            cls,
            filename: str,
    ) -> "AdvancedCrawler":
        path = Path(filename)

        config = json.loads(
            path.read_text(
                encoding="utf-8",
            )
        )

        crawler_config = config["crawler"]
        crawl_config = config["crawl"]

        storage_config = config.get("storage", {})
        storages: list[DataStorage] = []

        json_config = storage_config.get("json")

        if json_config is not None:
            storages.append(
                JSONStorage(
                    filename=json_config["filename"],
                )
            )

        csv_config = storage_config.get("csv")

        if csv_config is not None:
            storages.append(
                CSVStorage(
                    filename=csv_config["filename"],
                )
            )

        postgresql_config = storage_config.get("postgresql")

        if postgresql_config is not None:
            storages.append(
                PostgreSQLStorage(
                    database=postgresql_config["database"],
                    user=postgresql_config["user"],
                    host=postgresql_config["host"],
                    port=postgresql_config["port"],
                )
            )

        storage = MultiStorage(storages) if storages else None

        crawler = cls(
            max_concurrent=crawler_config["max_concurrent"],
            max_depth=crawler_config["max_depth"],
            max_per_domain=crawler_config["max_per_domain"],
            requests_per_second=crawler_config[
                "requests_per_second"
            ],
            rate_limit_per_domain=crawler_config[
                "rate_limit_per_domain"
            ],
            respect_robots=crawler_config["respect_robots"],
            user_agent=crawler_config["user_agent"],
            min_delay=crawler_config["min_delay"],
            jitter=crawler_config["jitter"],
            max_retries=crawler_config["max_retries"],
            backoff_factor=crawler_config["backoff_factor"],
            total_timeout=crawler_config["total_timeout"],
            connect_timeout=crawler_config["connect_timeout"],
            read_timeout=crawler_config["read_timeout"],
            timeout_backoff_factor=crawler_config[
                "timeout_backoff_factor"
            ],
            max_timeout=crawler_config["max_timeout"],
            storage=storage,
        )

        crawler.start_urls = crawl_config["start_urls"]
        crawler.sitemap_urls = crawl_config["sitemap_urls"]
        crawler.max_pages = crawl_config["max_pages"]
        crawler.same_domain_only = crawl_config["same_domain_only"]
        crawler.include_patterns = crawl_config["include_patterns"]
        crawler.exclude_patterns = crawl_config["exclude_patterns"]

        return crawler

    async def crawl(self) -> list[dict]:
        start_urls = list(self.start_urls)

        if self.sitemap_urls:
            session = await self._get_session()
            sitemap_parser = SitemapParser(session)

            for sitemap_url in self.sitemap_urls:
                sitemap_urls = await sitemap_parser.fetch_sitemap(
                    sitemap_url
                )

                start_urls.extend(sitemap_urls)

        return await super().crawl(
            start_urls=start_urls,
            max_pages=self.max_pages,
            same_domain_only=self.same_domain_only,
            exclude_patterns=self.exclude_patterns,
            include_patterns=self.include_patterns,
        )

    def get_stats(self) -> dict:
        stats = CrawlerStats(self)

        return stats.get_stats()

    def export_to_json(self, filename: str) -> None:
        stats = self.get_stats()
        path = Path(filename)

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        path.write_text(
            json.dumps(
                stats,
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

    def export_to_html_report(self, filename: str) -> None:
        stats = self.get_stats()
        path = Path(filename)

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        html = f"""
        <!DOCTYPE html>
        <html lang="ru">
        <head>
          <meta charset="UTF-8">
          <title>Отчёт краулера</title>
        </head>
        <body>
          <h1>Отчёт краулера</h1>

          <table border="1">
              <tr>
                <th>Показатель</th>
                <th>Значение</th>
              </tr>
              <tr>
                <td>Всего обработано</td>
                <td>{stats["total_pages"]}</td>
              </tr>
              <tr>
                <td>Успешно</td>
                <td>{stats["successful"]}</td>
              </tr>
              <tr>
                <td>Ошибок</td>
                <td>{stats["failed"]}</td>
              </tr>
              <tr>
                <td>HTTP-попыток</td>
                <td>{stats["total_requests"]}</td>
              </tr>
              <tr>
                <td>Средняя скорость</td>
                <td>{stats["average_pages_per_second"]:.2f} стр./сек.</td>
              </tr>
              <tr>
                <td>Время работы</td>
                <td>{stats["elapsed_seconds"]:.2f} сек.</td>
              </tr>
          </table>

          <h2>Статусы HTTP</h2>
          <pre>{stats["status_codes"]}</pre>

          <h2>Топ доменов</h2>
          <pre>{stats["top_domains"]}</pre>

          <h2>Ошибки по типам</h2>
          <pre>{stats["errors_by_type"]}</pre>
        </body>
        </html>
        """

        path.write_text(
            html,
            encoding="utf-8",
        )