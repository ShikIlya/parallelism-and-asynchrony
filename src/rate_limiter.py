import time
import asyncio

class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
    ):
        self.requests_per_second: float = requests_per_second
        self.per_domain: bool = per_domain
        self.requests_time: dict[str, float] = {}
        self.last_request_time: float | None = None
        self.min_interval = 1.0 / requests_per_second

    async def acquire(self, domain: str) -> None:
        if self.per_domain:
            await self._handle_per_domain(domain)
        else:
            await self._handle_global()

    async def _apply_limit(self, last_time: float | None) -> float:
        now = time.monotonic()

        if last_time is None:
            return now

        elapsed = now - last_time

        if elapsed < self.min_interval:
            delay = self.min_interval - elapsed
            await asyncio.sleep(delay)
            now = time.monotonic()

        return now

    async def _handle_per_domain(self, domain: str) -> None:
        last_time = self.requests_time.get(domain)
        new_time = await self._apply_limit(last_time)
        self.requests_time[domain] = new_time

    async def _handle_global(self) -> None:
        new_time = await self._apply_limit(self.last_request_time)
        self.last_request_time = new_time