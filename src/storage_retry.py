import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")

async def run_with_retry(
    operation: Callable[[], Awaitable[T]],
    retry_exceptions: tuple[type[Exception], ...],
    max_retries: int = 3,
    backoff_factor: float = 0.5,
) -> T:
    for attempt in range(max_retries + 1):
        try:
            return await operation()

        except asyncio.CancelledError:
            raise

        except retry_exceptions:
            if attempt >= max_retries:
                raise

            delay = backoff_factor * (2 ** attempt)

            await asyncio.sleep(delay)

    raise RuntimeError("Retry loop finished unexpectedly")