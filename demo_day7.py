import asyncio

from aiohttp import web
from aiohttp.test_utils import TestServer

from config import Config
from crawler import AdvancedCrawler, setup_logging

REPORT_PATH = "report.html"
STATS_PATH = "stats.json"

PAGES = {
    "/": "<title>Home</title><a href='/about'>about</a><a href='/blog'>blog</a>",
    "/about": "<title>About</title><a href='/'>home</a>",
    "/blog": "<title>Blog</title><a href='/blog/post-1'>post</a><a href='/missing'>x</a>",
    "/blog/post-1": "<title>Post 1</title>first post",
}


async def start_demo_server() -> TestServer:
    async def handler(request):
        path = request.path
        if path not in PAGES:
            return web.Response(status=404, text="not found")
        await asyncio.sleep(0.01)
        return web.Response(
            text=f"<html><head>{PAGES[path]}</head><body>page</body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/{tail:.*}", handler)
    server = TestServer(app)
    await server.start_server()
    return server


async def main() -> None:
    setup_logging(level="WARNING")
    server = await start_demo_server()
    base = str(server.make_url("/"))

    config = Config(
        start_urls=[base],
        max_pages=20,
        max_depth=2,
        max_concurrent=5,
        requests_per_second=1000,
        respect_robots=False,
        same_domain_only=True,
        output_json="results.jsonl",
        allow_private_hosts=True,
    )
    crawler = AdvancedCrawler(config)

    try:
        await crawler.crawl()
    finally:
        await crawler.close()
        await server.close()

    stats = crawler.get_stats()
    print("\n=== Crawl summary ===")
    print(f"Processed : {stats['total_pages']} pages")
    print(f"Successful: {stats['successful']}")
    print(f"Failed    : {stats['failed']}")
    print(f"Speed     : {stats['pages_per_second']} pages/sec")
    print(f"Statuses  : {stats['status_codes']}")
    print(f"Top domain: {stats['top_domains'][0] if stats['top_domains'] else '-'}")

    crawler.export_to_json(STATS_PATH)
    crawler.export_to_html_report(REPORT_PATH)
    print(f"\nStats saved to {STATS_PATH}, report to {REPORT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
