from pathlib import Path
import csv
import json
from io import StringIO

import aiofiles
import asyncio

from data_storage import DataStorage

class CSVStorage(DataStorage):
    def __init__(
        self,
        filename: str,
        encoding: str = "utf-8",
    ):
        self.path = Path(filename)
        self.encoding = encoding
        self.fieldnames: list[str] | None = None

        self._lock = asyncio.Lock()

    @staticmethod
    def _prepare_row(data: dict) -> dict:
        row = {}

        for key, value in data.items():
            if isinstance(value, (dict, list)):
                row[key] = json.dumps(
                    value,
                    ensure_ascii=False,
                    default=str,
                )
            elif value is None:
                row[key] = ""
            else:
                row[key] = str(value)

        return row

    async def save(self, data: dict) -> None:
        async with self._lock:
            self.path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            row = self._prepare_row(data)

            is_first_row = self.fieldnames is None

            if is_first_row:
                self.fieldnames = list(row.keys())

            buffer = StringIO(
                newline="",
            )

            writer = csv.DictWriter(
                buffer,
                fieldnames=self.fieldnames,
                extrasaction="ignore",
            )

            if is_first_row:
                writer.writeheader()

            writer.writerow(row)

            async with aiofiles.open(
                self.path,
                mode="a",
                encoding=self.encoding,
                newline="",
            ) as file:
                await file.write(
                    buffer.getvalue()
                )

    @staticmethod
    def _restore_value(value: str):
        if value is None or value == "":
            return ""

        value = value.strip()

        if value.startswith("[") or value.startswith("{"):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value

        return value

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

        reader = csv.DictReader(
            StringIO(content),
        )

        pages = []

        for row in reader:
            page = {
                key: self._restore_value(value)
                for key, value in row.items()
            }

            status_code = page.get("status_code")

            if (
                    isinstance(status_code, str)
                    and status_code.isdigit()
            ):
                page["status_code"] = int(status_code)

            pages.append(page)

        return pages

    async def close(self) -> None:
        return None