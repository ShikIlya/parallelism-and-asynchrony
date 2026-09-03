import asyncpg

import json
from datetime import datetime

from data_storage import DataStorage
from storage_retry import run_with_retry

class PostgreSQLStorage(DataStorage):
    def __init__(
        self,
        database: str = "crawler_db",
        user: str = "ilyashik",
        host: str = "localhost",
        port: int = 5432,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self.database = database
        self.user = user
        self.host = host
        self.port = port
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        self.pool: asyncpg.Pool | None = None

    async def init_db(self) -> None:
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                database=self.database,
                user=self.user,
                host=self.host,
                port=self.port,
                min_size=1,
                max_size=5,
            )

        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                CREATE TABLE IF NOT EXISTS pages (
                    id BIGSERIAL PRIMARY KEY,
                    url TEXT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    text TEXT NOT NULL DEFAULT '',
                    links JSONB NOT NULL DEFAULT '[]'::jsonb,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    crawled_at TIMESTAMPTZ NOT NULL,
                    status_code INTEGER,
                    content_type TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            await connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pages_url
                ON pages (url);
                """
            )

            await connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pages_crawled_at
                ON pages (crawled_at DESC);
                """
            )

            await connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pages_status_code
                ON pages (status_code);
                """
            )

    async def save(self, data: dict) -> None:
        await self.init_db()

        links = json.dumps(
            data.get("links", []),
            ensure_ascii=False,
            default=str,
        )

        metadata = json.dumps(
            data.get("metadata", {}),
            ensure_ascii=False,
            default=str,
        )

        crawled_at = data["crawled_at"]

        if isinstance(crawled_at, str):
            crawled_at = datetime.fromisoformat(
                crawled_at
            )

        query = """
            INSERT INTO pages (
                url,
                title,
                text,
                links,
                metadata,
                crawled_at,
                status_code,
                content_type
            )
            VALUES (
                $1,
                $2,
                $3,
                $4::jsonb,
                $5::jsonb,
                $6,
                $7,
                $8
            );
        """

        async def insert() -> None:
            async with self.pool.acquire() as connection:
                await connection.execute(
                    query,
                    data["url"],
                    data.get("title", ""),
                    data.get("text", ""),
                    links,
                    metadata,
                    crawled_at,
                    data.get("status_code"),
                    data.get("content_type"),
                )

        await run_with_retry(
            operation=insert,
            retry_exceptions=(
                OSError,
                ConnectionError,
                asyncpg.PostgresConnectionError,
            ),
            max_retries=self.max_retries,
            backoff_factor=self.backoff_factor,
        )

    async def load_all(self) -> list[dict]:
        await self.init_db()

        query = """
            SELECT 
                id,
                url,
                title,
                text,
                links,
                metadata,
                crawled_at,
                status_code,
                content_type,
                created_at
            FROM pages
            ORDER BY id;
        """

        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                query
            )

        pages = []

        for row in rows:
            page = dict(row)

            if isinstance(page["links"], str):
                page["links"] = json.loads(
                    page["links"]
                )

            if isinstance(page["metadata"], str):
                page["metadata"] = json.loads(
                    page["metadata"]
                )

            pages.append(page)

        return pages

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None