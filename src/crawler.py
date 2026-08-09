import asyncio
import aiohttp
import logging

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

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()
            logger.info("HTTP-сессия закрыта")