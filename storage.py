import asyncio
import csv
import io
import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime

import aiofiles
import asyncpg

from errors import StorageError
from retry_strategy import RetryStrategy

logger = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"not JSON serializable: {type(obj).__name__}")


class DataStorage(ABC):
    async def init(self) -> None:
        # setup hook (create tables, open files); default: nothing to do
        pass

    @abstractmethod
    async def save(self, data: dict) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class JSONStorage(DataStorage):
    def __init__(self, path: str, indent: int | None = None) -> None:
        self.path = path
        self.indent = indent
        self._file = None
        self._buffer: list[dict] = []
        self._lock = asyncio.Lock()

    async def _ensure_open(self) -> None:
        if self._file is None:
            self._file = await aiofiles.open(self.path, "a", encoding="utf-8")

    async def save(self, data: dict) -> None:
        if self.indent is not None:
            async with self._lock:
                self._buffer.append(data)
            return

        line = json.dumps(data, ensure_ascii=False, default=_json_default)
        try:
            async with self._lock:
                await self._ensure_open()
                await self._file.write(line + "\n")
        except OSError as error:
            raise StorageError(str(error)) from error

    async def close(self) -> None:
        async with self._lock:
            if self.indent is not None:
                text = json.dumps(
                    self._buffer,
                    ensure_ascii=False,
                    indent=self.indent,
                    default=_json_default,
                )
                try:
                    async with aiofiles.open(
                        self.path, "w", encoding="utf-8"
                    ) as handle:
                        await handle.write(text)
                except OSError as error:
                    raise StorageError(str(error)) from error
            elif self._file is not None:
                await self._file.close()
                self._file = None


class CSVStorage(DataStorage):
    def __init__(self, path: str, encoding: str = "utf-8") -> None:
        self.path = path
        self.encoding = encoding
        self._file = None
        self._headers = None
        self._lock = asyncio.Lock()

    async def _ensure_open(self) -> None:
        if self._file is None:
            self._file = await aiofiles.open(
                self.path, "a", encoding=self.encoding, newline=""
            )

    @staticmethod
    def _format_row(values: list) -> str:
        buffer = io.StringIO()
        csv.writer(buffer).writerow(values)
        return buffer.getvalue()

    @staticmethod
    def _encode(value) -> str:
        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False, default=_json_default)
        if isinstance(value, datetime):
            return value.isoformat()
        if value is None:
            return ""
        return str(value)

    async def save(self, data: dict) -> None:
        async with self._lock:
            try:
                await self._ensure_open()
                if self._headers is None:
                    self._headers = list(data.keys())
                    await self._file.write(self._format_row(self._headers))
                row = [self._encode(data.get(key)) for key in self._headers]
                await self._file.write(self._format_row(row))
            except OSError as error:
                raise StorageError(str(error)) from error

    async def close(self) -> None:
        async with self._lock:
            if self._file is not None:
                await self._file.close()
                self._file = None


class PostgresStorage(DataStorage):
    _COLUMNS = (
        "url",
        "title",
        "text",
        "links",
        "metadata",
        "crawled_at",
        "status_code",
        "content_type",
    )

    def __init__(self, dsn: str, table: str = "pages", batch_size: int = 50) -> None:
        # Table name is interpolated into SQL (identifiers can't be parameterized),
        # so allow only a plain identifier to keep it injection-safe.
        if not _IDENTIFIER.fullmatch(table):
            raise ValueError(f"invalid table name: {table!r}")

        self.dsn = dsn
        self.table = table
        self.batch_size = batch_size
        self.max_buffer = batch_size * 10
        self._pool = None
        self._buffer: list[tuple] = []
        self._lock = asyncio.Lock()
        self._query = self._build_query()

    def _build_query(self) -> str:
        columns = ", ".join(self._COLUMNS)
        placeholders = ", ".join(f"${i}" for i in range(1, len(self._COLUMNS) + 1))
        updates = ", ".join(
            f"{column} = EXCLUDED.{column}"
            for column in self._COLUMNS
            if column != "url"
        )
        return (
            f"INSERT INTO {self.table} ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT (url) DO UPDATE SET {updates}"
        )

    @staticmethod
    async def _init_connection(conn) -> None:
        await conn.set_type_codec(
            "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )

    async def init(self) -> None:
        self._pool = await asyncpg.create_pool(self.dsn, init=self._init_connection)
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    url TEXT PRIMARY KEY,
                    title TEXT,
                    text TEXT,
                    links JSONB,
                    metadata JSONB,
                    crawled_at TIMESTAMPTZ,
                    status_code INT,
                    content_type TEXT
                )
                """
            )
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS {self.table}_crawled_at_idx "
                f"ON {self.table} (crawled_at)"
            )

    def _to_row(self, data: dict) -> tuple:
        return tuple(data.get(column) for column in self._COLUMNS)

    async def save(self, data: dict) -> None:
        async with self._lock:
            self._buffer.append(self._to_row(data))
            if len(self._buffer) >= self.batch_size:
                await self._flush_safely()

    async def _flush(self) -> None:
        if not self._buffer:
            return
        rows = self._buffer[:]
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(self._query, rows)
        except (asyncpg.PostgresError, OSError) as error:
            raise StorageError(str(error)) from error
        self._buffer.clear()

    async def _flush_safely(self) -> None:
        try:
            await self._flush()
        except StorageError as error:
            overflow = len(self._buffer) - self.max_buffer
            if overflow > 0:
                del self._buffer[:overflow]
                logger.error("Postgres buffer full, dropped %d oldest rows", overflow)
            logger.warning(
                "Postgres flush failed, %d rows kept for retry: %s",
                len(self._buffer),
                error,
            )

    async def close(self) -> None:
        async with self._lock:
            await self._flush_safely()
            if self._buffer:
                logger.error(
                    "Postgres closing with %d unsaved rows (data lost)",
                    len(self._buffer),
                )
        if self._pool is not None:
            await self._pool.close()
            self._pool = None


class CompositeStorage(DataStorage):
    def __init__(self, storages) -> None:
        self._storages = list(storages)

    async def init(self) -> None:
        for storage in self._storages:
            await storage.init()

    async def save(self, data: dict) -> None:
        results = await asyncio.gather(
            *(storage.save(data) for storage in self._storages),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            raise StorageError(
                f"{len(failures)}/{len(self._storages)} storages failed: {failures[0]}"
            )

    async def close(self) -> None:
        results = await asyncio.gather(
            *(storage.close() for storage in self._storages),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                logger.warning("storage close failed: %s", result)


class RetryingStorage(DataStorage):
    """Retries a single storage's writes on StorageError.

    Retrying must wrap each leaf storage, not a CompositeStorage: retrying a
    composite would re-write to the storages that already succeeded and create
    duplicates. Wrap the leaves, then compose.
    """

    def __init__(self, inner: DataStorage, retry: RetryStrategy | None = None) -> None:
        self._inner = inner
        self._retry = retry or RetryStrategy(retry_on=[StorageError])

    async def init(self) -> None:
        await self._inner.init()

    async def save(self, data: dict) -> None:
        await self._retry.execute_with_retry(self._inner.save, data)

    async def close(self) -> None:
        await self._inner.close()
