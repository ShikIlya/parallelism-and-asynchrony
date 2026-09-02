import time
import asyncio

class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
    ):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")

        self.requests_per_second: float = requests_per_second
        self.per_domain: bool = per_domain
        self.min_interval = 1.0 / requests_per_second

        self.requests_time: dict[str, float] = {}
        self.last_request_time: float | None = None

        self.global_lock = asyncio.Lock()
        self.domain_locks: dict[str, asyncio.Lock] = {}

    async def acquire(
            self,
            domain: str,
            minimum_delay: float = 0.0,
    ) -> None:
        if self.per_domain:
            await self._handle_per_domain(
                domain,
                minimum_delay
            )
        else:
            await self._handle_global(minimum_delay)

    async def _handle_per_domain(
            self,
            domain: str,
            minimum_delay: float,
    ) -> None:
        lock = self.domain_locks.get(domain)

        if lock is None:
            lock = asyncio.Lock()
            self.domain_locks[domain] = lock

        async with lock:
            last_time = self.requests_time.get(domain)

            interval = max(
                self.min_interval,
                minimum_delay,
            )

            new_time = await self._apply_limit(
                last_time,
                interval
            )

            self.requests_time[domain] = new_time

    async def _handle_global(
            self,
            minimum_delay: float,
    ) -> None:
        async with self.global_lock:
            interval = max(
                self.min_interval,
                minimum_delay,
            )

            new_time = await self._apply_limit(
                self.last_request_time,
                interval
            )

            self.last_request_time = new_time

    async def _apply_limit(
            self,
            last_time: float | None,
            minimum_interval: float,
    ) -> float:
        now = time.monotonic()

        if last_time is None:
            return now

        elapsed = now - last_time

        if elapsed < minimum_interval:
            await asyncio.sleep(
                minimum_interval - elapsed,
            )

            now = time.monotonic()

        return now