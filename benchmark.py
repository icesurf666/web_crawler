import asyncio
import time
import tracemalloc
from urllib.request import urlopen

from aiohttp import web
from aiohttp.test_utils import TestServer

from async_crawler import AsyncCrawler


# Simulated per-request network latency; async hides it, sync pays it in full.
LATENCY = 0.005


async def start_server(page_count: int) -> TestServer:
    async def hub(_request):
        await asyncio.sleep(LATENCY)
        links = "".join(f'<a href="/p{i}">{i}</a>' for i in range(page_count))
        return web.Response(
            text=f"<html><body>{links}</body></html>", content_type="text/html"
        )

    async def leaf(_request):
        await asyncio.sleep(LATENCY)
        return web.Response(
            text="<html><body>leaf page</body></html>", content_type="text/html"
        )

    app = web.Application()
    app.router.add_get("/", hub)
    app.router.add_get("/p{i}", leaf)
    server = TestServer(app)
    await server.start_server()
    return server


async def bench_async(base_url: str, page_count: int) -> tuple[int, float]:
    crawler = AsyncCrawler(
        max_concurrent=50,
        max_depth=1,
        per_domain_limit=50,
        requests_per_second=1_000_000,
        respect_robots=False,
        allow_private_hosts=True,
    )
    started = time.monotonic()
    await crawler.crawl([base_url], max_pages=page_count + 1, same_domain_only=True)
    elapsed = time.monotonic() - started
    pages = len(crawler.processed_urls)
    await crawler.close()
    return pages, elapsed


def bench_sync(urls: list[str]) -> float:
    started = time.monotonic()
    for url in urls:
        try:
            urlopen(url, timeout=10).read()
        except OSError:
            pass
    return time.monotonic() - started


async def run_scale(page_count: int) -> None:
    server = await start_server(page_count)
    base_url = str(server.make_url("/"))
    urls = [base_url] + [str(server.make_url(f"/p{i}")) for i in range(page_count)]

    tracemalloc.start()
    pages, async_time = await bench_async(base_url, page_count)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # Run the blocking sync baseline in a thread so the test server keeps serving.
    sync_time = await asyncio.get_running_loop().run_in_executor(
        None, bench_sync, urls
    )

    await server.close()

    async_rate = pages / async_time if async_time else 0
    sync_rate = len(urls) / sync_time if sync_time else 0
    speedup = sync_time / async_time if async_time else 0
    print(
        f"{page_count:>5} pages | "
        f"async {async_time:6.2f}s ({async_rate:8.1f} p/s) | "
        f"sync {sync_time:6.2f}s ({sync_rate:7.1f} p/s) | "
        f"speedup x{speedup:5.1f} | peak {peak / 1e6:5.1f} MB"
    )


async def main() -> None:
    print("Async vs sync crawler benchmark (local server)")
    print("Note: the sync baseline only fetches; the async crawler also parses "
          "HTML, so the comparison is end-to-end crawl, not fetch-for-fetch.\n")
    for page_count in (100, 500, 1000):
        await run_scale(page_count)


if __name__ == "__main__":
    asyncio.run(main())
