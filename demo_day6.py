import asyncio
import json
import logging
import os

import asyncpg

from async_crawler import AsyncCrawler
from storage import CompositeStorage, CSVStorage, JSONStorage, PostgresStorage

DSN = os.environ.get("CRAWLER_DSN", "postgresql://mac@localhost/web_crawler")
JSON_PATH = "results.jsonl"
CSV_PATH = "results.csv"

PAGES = {
    "http://demo.local/": (
        "<html><head><title>Home</title>"
        "<meta name='description' content='demo home page'></head>"
        "<body><h1>Home</h1><p>Welcome to the demo site.</p>"
        "<a href='http://demo.local/about'>about</a>"
        "<a href='http://demo.local/blog'>blog</a></body></html>"
    ),
    "http://demo.local/about": (
        "<html><head><title>About</title></head>"
        "<body><p>About us page.</p>"
        "<a href='http://demo.local/'>home</a></body></html>"
    ),
    "http://demo.local/blog": (
        "<html><head><title>Blog</title></head>"
        "<body><p>Latest posts.</p>"
        "<a href='http://demo.local/blog/post-1'>post</a></body></html>"
    ),
    "http://demo.local/blog/post-1": (
        "<html><head><title>Post 1</title></head>"
        "<body><p>First post body.</p></body></html>"
    ),
}


async def fake_fetch(url: str) -> str:
    return PAGES.get(url, "<html><body>page</body></html>")


async def build_storage() -> tuple[CompositeStorage, bool]:
    storages = [JSONStorage(JSON_PATH), CSVStorage(CSV_PATH)]

    pg_available = False
    try:
        conn = await asyncpg.connect(DSN)
        await conn.execute("DROP TABLE IF EXISTS pages")
        await conn.close()
        storages.append(PostgresStorage(DSN, batch_size=5))
        pg_available = True
    except (OSError, asyncpg.PostgresError) as error:
        print(f"PostgreSQL unavailable, using JSON+CSV only: {error}")

    return CompositeStorage(storages), pg_available


async def read_back(pg_available: bool) -> None:
    print("\n=== Read back saved data ===")

    with open(JSON_PATH, encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle]
    print(f"JSONL records : {len(records)}")
    for record in records:
        print(f"  {record['url']} -> {record['title']} ({len(record['links'])} links)")

    if not pg_available:
        return

    conn = await asyncpg.connect(DSN)
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    total = await conn.fetchval("SELECT count(*) FROM pages")
    row = await conn.fetchrow(
        "SELECT url, title, links FROM pages ORDER BY crawled_at LIMIT 1"
    )
    print(f"Postgres rows : {total}")
    print(f"  sample: {row['url']} -> {row['title']} links={row['links']}")
    await conn.close()


async def main() -> None:
    for path in (JSON_PATH, CSV_PATH):
        if os.path.exists(path):
            os.remove(path)

    storage, pg_available = await build_storage()
    crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=2,
        respect_robots=False,
        requests_per_second=1000,
        storage=storage,
    )
    crawler.fetch_url = fake_fetch

    results = await crawler.crawl(["http://demo.local/"], max_pages=20)
    await crawler.close()

    print("\n=== Storage summary ===")
    print(f"Processed pages : {len(results)}")
    print(f"JSON file       : {JSON_PATH}")
    print(f"CSV file        : {CSV_PATH}")
    print(f"PostgreSQL      : {'pages table' if pg_available else 'skipped'}")

    await read_back(pg_available)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(main())
