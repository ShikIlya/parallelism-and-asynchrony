import asyncio
import logging

logger = logging.getLogger(__name__)

class SemaphoreManager:
    def __init__(
            self,
            max_concurrent: int = 10,
            max_per_domain: int = 3

    ):
        self.global_semaphore = asyncio.Semaphore(max_concurrent)
        self.max_per_domain = max_per_domain
        self.domain_semaphores = {}
        self.active_tasks = 0

    def get_domain_semaphore(self, domain: str):
        if domain not in self.domain_semaphores:
            self.domain_semaphores[domain] = asyncio.Semaphore(self.max_per_domain)

        return self.domain_semaphores[domain]

    async def acquire_and_run(self, domain: str, coro_func, *args):
        async with self.global_semaphore:
            async with self.get_domain_semaphore(domain):
                self.active_tasks += 1

                try:
                    return await coro_func(*args)

                except Exception:
                    logger.exception(
                        "Ошибка выполнения задачи для домена %s",
                        domain,
                    )
                    raise

                finally:
                    self.active_tasks -= 1