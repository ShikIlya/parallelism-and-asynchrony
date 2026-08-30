import asyncio

from data_storage import DataStorage

class MultiStorage(DataStorage):
    def __init__(
        self,
        storages: list[DataStorage],
    ) -> None:
        self.storages = storages

    async def save(self, data: dict) -> None:
        results = await asyncio.gather(
            *[
                storage.save(data)
                for storage in self.storages
            ],
            return_exceptions=True,
        )

        errors = [
            result
            for result in results
            if isinstance(result, Exception)
        ]

        if errors:
            raise RuntimeError(
                f"Ошибка сохранения в хранилища: {errors}"
            )

    async def close(self) -> None:
        await asyncio.gather(
            *[
                storage.close()
                for storage in self.storages
            ],
            return_exceptions=True,
        )