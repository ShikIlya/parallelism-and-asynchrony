from pathlib import Path
import json

import aiofiles
import asyncio

from data_storage import DataStorage

class JSONStorage(DataStorage):
    def __init__(
        self,
        filename: str,
        encoding: str = "utf-8",
        indent: int | None = None,
    ):
        self.path = Path(filename)
        self.encoding = encoding
        self.indent = indent

        self._lock = asyncio.Lock()

    async def save(self, data: dict) -> None:
        async with self._lock:
            self.path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            serialized_data = json.dumps(
                data,
                ensure_ascii=False,
                indent=self.indent,
                default=str,
            )

            async with aiofiles.open(
                self.path,
                mode="a",
                encoding=self.encoding,
            ) as file:
                await file.write(
                    serialized_data + "\n"
                )

    async def load_all(self) -> list[dict]:
        async with self._lock:
            if not self.path.exists():
                return []

            async with aiofiles.open(
                    self.path,
                    mode="r",
                    encoding=self.encoding,
            ) as file:
                content = await file.read()

        pages = []

        for line in content.splitlines():
            line = line.strip()

            if not line:
                continue

            pages.append(
                json.loads(line)
            )

        return pages

    async def close(self) -> None:
        return None