import csv
import json
import os
from datetime import datetime, timezone

import asyncpg
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

import retry_strategy
from async_crawler import AsyncCrawler
from errors import StorageError
from storage import (
    CompositeStorage,
    CSVStorage,
    DataStorage,
    JSONStorage,
    PostgresStorage,
)

PG_DSN = os.environ.get(
    "CRAWLER_TEST_DSN", "postgresql://mac@localhost/web_crawler_test"
)
PG_TABLE = "pages_pytest"


@pytest.fixture
def no_sleep(monkeypatch):
    async def fake_sleep(_seconds):
        pass

    monkeypatch.setattr(retry_strategy.asyncio, "sleep", fake_sleep)


class _RecordingStorage(DataStorage):
    def __init__(self):
        self.saved = []

    async def save(self, data):
        self.saved.append(data)

    async def close(self):
        pass


class _FailingStorage(DataStorage):
    async def save(self, data):
        raise StorageError("boom")

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_json_storage_writes_jsonl(tmp_path):
    path = tmp_path / "out.jsonl"
    storage = JSONStorage(str(path))
    await storage.save(
        {"url": "http://a", "crawled_at": datetime(2026, 8, 26, tzinfo=timezone.utc)}
    )
    await storage.save({"url": "http://b"})
    await storage.close()

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["url"] == "http://a"
    assert "2026-08-26" in json.loads(lines[0])["crawled_at"]


@pytest.mark.asyncio
async def test_json_storage_keeps_unicode(tmp_path):
    path = tmp_path / "u.jsonl"
    storage = JSONStorage(str(path))
    await storage.save({"title": "Тест"})
    await storage.close()

    assert "Тест" in path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_json_storage_pretty_array(tmp_path):
    path = tmp_path / "pretty.json"
    storage = JSONStorage(str(path), indent=2)
    await storage.save({"url": "http://a"})
    await storage.save({"url": "http://b"})
    await storage.close()

    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    assert isinstance(data, list)
    assert [record["url"] for record in data] == ["http://a", "http://b"]
    assert "\n  " in text


@pytest.mark.asyncio
async def test_csv_storage_headers_and_nested(tmp_path):
    path = tmp_path / "out.csv"
    storage = CSVStorage(str(path))
    await storage.save({"url": "http://a", "links": ["http://x", "http://y"]})
    await storage.close()

    rows = list(csv.reader(path.read_text(encoding="utf-8").splitlines()))
    assert rows[0] == ["url", "links"]
    assert rows[1][0] == "http://a"
    assert json.loads(rows[1][1]) == ["http://x", "http://y"]


@pytest.mark.asyncio
async def test_csv_storage_survives_special_chars(tmp_path):
    path = tmp_path / "s.csv"
    storage = CSVStorage(str(path))
    await storage.save({"title": 'a, "b"\nc'})
    await storage.close()

    with open(path, encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))
    assert rows[1][0] == 'a, "b"\nc'


@pytest.mark.asyncio
async def test_composite_saves_to_all_storages():
    first, second = _RecordingStorage(), _RecordingStorage()
    composite = CompositeStorage([first, second])

    await composite.save({"url": "http://a"})

    assert first.saved == [{"url": "http://a"}]
    assert second.saved == [{"url": "http://a"}]


@pytest.mark.asyncio
async def test_composite_one_failure_does_not_stop_others():
    recording = _RecordingStorage()
    composite = CompositeStorage([_FailingStorage(), recording])

    with pytest.raises(StorageError):
        await composite.save({"url": "http://a"})

    assert recording.saved == [{"url": "http://a"}]


@pytest.mark.asyncio
async def test_crawl_saves_standard_record(tmp_path, no_sleep):
    path = tmp_path / "c.jsonl"
    crawler = AsyncCrawler(
        respect_robots=False,
        requests_per_second=1000,
        max_depth=0,
        storage=JSONStorage(str(path)),
    )

    async def fake(url):
        return "<html><head><title>T</title></head><body><a href='http://x'>x</a></body></html>"

    crawler.fetch_url = fake
    await crawler.crawl(["http://site/"], max_pages=1)
    await crawler.close()

    record = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert set(record) == {
        "url",
        "title",
        "text",
        "links",
        "metadata",
        "crawled_at",
        "status_code",
        "content_type",
    }
    assert record["status_code"] == 200
    assert record["links"] == ["http://x"]


@pytest.mark.asyncio
async def test_crawl_captures_real_status_and_content_type(tmp_path):
    async def handler(_request):
        return web.Response(
            text="<html><head><title>T</title></head><body>hi</body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/", handler)
    server = TestServer(app)
    await server.start_server()
    url = str(server.make_url("/"))

    path = tmp_path / "r.jsonl"
    crawler = AsyncCrawler(
        respect_robots=False,
        requests_per_second=1000,
        max_depth=0,
        storage=JSONStorage(str(path)),
    )
    try:
        await crawler.crawl([url], max_pages=1)
    finally:
        await crawler.close()
        await server.close()

    record = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert record["status_code"] == 200
    assert "text/html" in record["content_type"]


@pytest.mark.asyncio
async def test_crawl_continues_when_save_fails(no_sleep):
    crawler = AsyncCrawler(
        respect_robots=False,
        requests_per_second=1000,
        max_depth=0,
        storage=_FailingStorage(),
    )

    async def fake(url):
        return "<html><body>ok</body></html>"

    crawler.fetch_url = fake
    results = await crawler.crawl(["http://site/"], max_pages=1)
    await crawler.close()

    assert "http://site/" in results


@pytest.mark.asyncio
async def test_save_is_retried_on_storage_error(no_sleep):
    class FlakyStorage(DataStorage):
        def __init__(self):
            self.calls = 0
            self.saved = []

        async def save(self, data):
            self.calls += 1
            if self.calls < 3:
                raise StorageError("temporary")
            self.saved.append(data)

        async def close(self):
            pass

    storage = FlakyStorage()
    crawler = AsyncCrawler(
        respect_robots=False,
        requests_per_second=1000,
        max_depth=0,
        storage=storage,
    )

    async def fake(url):
        return "<html><body>ok</body></html>"

    crawler.fetch_url = fake
    await crawler.crawl(["http://site/"], max_pages=1)
    await crawler.close()

    assert storage.calls == 3
    assert len(storage.saved) == 1


async def _pg_or_skip(batch_size=2):
    storage = PostgresStorage(PG_DSN, table=PG_TABLE, batch_size=batch_size)
    try:
        await storage.init()
    except (OSError, asyncpg.PostgresError) as error:
        pytest.skip(f"PostgreSQL not available: {error}")
    async with storage._pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {PG_TABLE}")
    return storage


async def _drop_and_close(storage):
    if storage._pool is not None:
        async with storage._pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {PG_TABLE}")
        await storage.close()


@pytest.mark.asyncio
async def test_pg_save_and_read_back():
    storage = await _pg_or_skip()
    try:
        now = datetime(2026, 8, 26, tzinfo=timezone.utc)
        await storage.save(
            {
                "url": "http://a",
                "title": "A",
                "links": ["http://x"],
                "metadata": {"k": "v"},
                "crawled_at": now,
                "status_code": 200,
            }
        )
        await storage.save({"url": "http://b", "title": "B", "crawled_at": now})

        rows = await storage._pool.fetch(
            f"SELECT url, title, links, metadata FROM {PG_TABLE} ORDER BY url"
        )
        assert [row["url"] for row in rows] == ["http://a", "http://b"]
        assert rows[0]["links"] == ["http://x"]
        assert rows[0]["metadata"] == {"k": "v"}
    finally:
        await _drop_and_close(storage)


@pytest.mark.asyncio
async def test_pg_batch_buffers_until_full():
    storage = await _pg_or_skip(batch_size=3)
    try:
        now = datetime(2026, 8, 26, tzinfo=timezone.utc)
        await storage.save({"url": "http://a", "crawled_at": now})
        await storage.save({"url": "http://b", "crawled_at": now})
        count_before = await storage._pool.fetchval(f"SELECT count(*) FROM {PG_TABLE}")
        await storage.save({"url": "http://c", "crawled_at": now})
        count_after = await storage._pool.fetchval(f"SELECT count(*) FROM {PG_TABLE}")

        assert count_before == 0
        assert count_after == 3
    finally:
        await _drop_and_close(storage)


@pytest.mark.asyncio
async def test_pg_upsert_updates_existing_url():
    storage = await _pg_or_skip()
    try:
        now = datetime(2026, 8, 26, tzinfo=timezone.utc)
        await storage.save({"url": "http://a", "title": "first", "crawled_at": now})
        await storage.save({"url": "http://a", "title": "second", "crawled_at": now})

        rows = await storage._pool.fetch(f"SELECT title FROM {PG_TABLE}")
        assert len(rows) == 1
        assert rows[0]["title"] == "second"
    finally:
        await _drop_and_close(storage)
