from abc import ABC, abstractmethod

class DataStorage(ABC):
    @abstractmethod
    async def save(self, data: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError