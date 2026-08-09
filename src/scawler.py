class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
    ):
        self.max_concurrent = max_concurrent

    # async def fetch_url(self, url: str) -> str:

    # async def fetch_urls(self, urls: list[str]) -> dict[str, str]:

    # async def close(self):