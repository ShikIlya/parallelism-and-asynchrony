from exceptions import TransientError, NetworkError

import asyncio
import logging

logger = logging.getLogger(__name__)

class RetryStrategy:
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list[type[Exception]] | None = None,
        retry_limits: dict[type[Exception], int] | None = None,
        backoff_factors: dict[type[Exception], float] | None = None,
    ):
        if max_retries < 0:
            raise ValueError("max_retries cannot be negative")

        if backoff_factor < 0:
            raise ValueError("backoff_factor cannot be negative")

        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = retry_on or [
            TransientError,
            NetworkError,
        ]
        self.retry_limits = retry_limits or {}
        self.backoff_factors = backoff_factors or {}

        for error_class, limit in self.retry_limits.items():
            if not isinstance(error_class, type):
                raise TypeError(
                    "retry_limits keys must be exception classes"
                )

            if not issubclass(error_class, Exception):
                raise TypeError(
                    "retry_limits keys must inherit from Exception"
                )

            if not isinstance(limit, int):
                raise TypeError(
                    "retry_limits values must be integers"
                )

            if limit < 0:
                raise ValueError(
                    "retry limit cannot be negative"
                )

        for error_class, factor in self.backoff_factors.items():
            if not isinstance(error_class, type):
                raise TypeError(
                    "backoff_factors keys must be exception classes"
                )

            if not issubclass(error_class, Exception):
                raise TypeError(
                    "backoff_factors keys must inherit from Exception"
                )

            if not isinstance(factor, (int, float)):
                raise TypeError(
                    "backoff_factors values must be numbers"
                )

            if factor < 0:
                raise ValueError(
                    "backoff factor cannot be negative"
                )

        self.errors_by_type: dict[str, int] = {}
        self.successful_retries: int = 0
        self.retry_delays: list[float] = []

    async def execute_with_retry(self, coro, *args, **kwargs):
        retries = 0
        retries_by_type: dict[type[Exception], int] = {}

        while retries <= self.max_retries:
            try:
                result = await coro(*args, **kwargs)

                if retries > 0:
                    self.successful_retries += 1

                return result

            except asyncio.CancelledError:
                raise

            except Exception as error:
                error_type = type(error).__name__
                error_class = type(error)

                self.errors_by_type[error_type] = (
                        self.errors_by_type.get(error_type, 0) + 1
                )

                if not isinstance(error, tuple(self.retry_on)):
                    raise

                type_retries = retries_by_type.get(
                    error_class,
                    0,
                )

                type_retry_limit = self.retry_limits.get(
                    error_class,
                    self.max_retries,
                )

                if retries >= self.max_retries:
                    raise

                if type_retries >= type_retry_limit:
                    raise

                type_backoff_factor = self.backoff_factors.get(
                    error_class,
                    self.backoff_factor,
                )

                backoff_delay = type_backoff_factor * (
                        2 ** type_retries
                )

                self.retry_delays.append(backoff_delay)

                logger.warning(
                    "Повтор %d/%d для %s. Ошибка: %s. "
                    "Следующая попытка через %.2f сек.",
                    retries + 1,
                    self.max_retries,
                    error_type,
                    error,
                    backoff_delay,
                )

                retries_by_type[error_class] = (
                    type_retries + 1
                )

                await asyncio.sleep(backoff_delay)

                retries += 1

        raise RuntimeError(
            "Retry loop exited unexpectedly"
        )

    def get_statistics(self):
        average_retry_delay = (
            sum(self.retry_delays) / len(self.retry_delays)
            if self.retry_delays
            else 0.0
        )

        return {
            "errors_by_type": dict(self.errors_by_type),
            "retry_delays": list(self.retry_delays),
            "successful_retries": self.successful_retries,
            "average_retry_delay": average_retry_delay,
        }

    def reset_statistics(self) -> None:
        self.errors_by_type.clear()
        self.retry_delays.clear()
        self.successful_retries = 0