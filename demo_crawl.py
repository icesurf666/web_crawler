import asyncio
import json
import logging
from time import perf_counter

from async_crawler import AsyncCrawler

START_URLS = ["https://example.com"]
OUTPUT_FILE = "crawl_results.json"


def build_report(crawler: AsyncCrawler) -> list[dict]:
    report = []

    for url, data in crawler.processed_urls.items():
        report.append(
            {
                "url": url,
                "title": data["title"],
                "text_length": len(data["text"]),
                "links_count": len(data["links"]),
                "images_count": len(data["images"]),
            }
        )

    return report


def save_results(report: list[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2, ensure_ascii=False)


async def main() -> None:
    crawler = AsyncCrawler(max_concurrent=10, max_depth=2)

    started_at = perf_counter()
    try:
        results = await crawler.crawl(
            start_urls=START_URLS,
            max_pages=30,
            same_domain_only=False,
        )
    finally:
        await crawler.close()

    elapsed = perf_counter() - started_at

    report = build_report(crawler)
    save_results(report, OUTPUT_FILE)

    print("\n=== Crawl summary ===")
    print(f"Processed pages : {len(results)}")
    print(f"Failed          : {len(crawler.failed_urls)}")
    print(f"Total seen URLs : {len(crawler.visited_urls)}")
    print(f"Elapsed         : {elapsed:.2f} s")
    print(f"Saved to        : {OUTPUT_FILE}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(main())
