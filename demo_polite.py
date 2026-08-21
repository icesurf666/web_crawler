import asyncio
import logging

from async_crawler import AsyncCrawler

START_URLS = ["https://example.com"]


async def blocking_demo() -> None:
    # Deterministic robots.txt blocking demo on a mocked site.
    async def fake_fetch(url: str) -> str:
        if url.endswith("/robots.txt"):
            return "User-agent: *\nDisallow: /private/\n"
        if url == "http://demo.local/":
            return (
                '<a href="http://demo.local/public">public</a>'
                '<a href="http://demo.local/private/secret">secret</a>'
            )
        return "<html><body>page</body></html>"

    crawler = AsyncCrawler(max_concurrent=5, max_depth=1, requests_per_second=1000)
    crawler.fetch_url = fake_fetch

    results = await crawler.crawl(["http://demo.local/"], max_pages=10)

    print("\n=== Robots blocking demo (simulated) ===")
    print(f"Processed pages   : {len(results)}")
    print(f"Blocked by robots : {len(crawler.blocked_urls)}")
    for url in crawler.blocked_urls:
        print(f"  blocked: {url}")


async def main() -> None:
    crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=1,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.5,
        jitter=0.2,
        user_agent="MyBot/1.0",
    )

    try:
        results = await crawler.crawl(START_URLS, max_pages=15)
    finally:
        await crawler.close()

    stats = crawler.get_speed_stats()

    print("\n=== Polite crawl summary ===")
    print(f"Processed pages   : {len(results)}")
    print(f"Requests made     : {stats['requests']}")
    print(f"Request rate      : {stats['req_per_sec']:.2f} req/sec")
    print(f"Average delay     : {stats['avg_delay']:.2f} s")
    print(f"Blocked by robots : {stats['blocked']}")

    for url in crawler.blocked_urls:
        print(f"  blocked: {url}")

    await blocking_demo()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(main())
