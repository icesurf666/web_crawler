import asyncio
import json
import logging
from time import perf_counter

from async_crawler import AsyncCrawler

URLS = [
    "https://example.com",
    "https://example.org",
    "https://www.google.com",
    "https://www.python.org",
    "https://www.wikipedia.org",
    "https://github.com",
    "https://www.cloudflare.com",
]


async def fetch_sequentially(urls: list[str]) -> tuple[dict[str, str], float]:
    crawler = AsyncCrawler(max_concurrent=1)
    started_at = perf_counter()

    try:
        results = {}
        for url in urls:
            results[url] = await crawler.fetch_url(url)
    finally:
        await crawler.close()

    return results, perf_counter() - started_at


async def fetch_concurrently(urls: list[str]) -> tuple[dict[str, str], float]:
    crawler = AsyncCrawler(max_concurrent=5)
    started_at = perf_counter()

    try:
        results = await crawler.fetch_urls(urls)
    finally:
        await crawler.close()

    return results, perf_counter() - started_at


def print_results(title: str, results: dict[str, str], elapsed: float) -> None:
    print(f"\n{title}")
    for url, content in results.items():
        status = "success" if content else "error"
        print(f"[{status}] {url}")
    print(f"Total time: {elapsed:.2f} s")


def summarize(parsed: dict) -> dict:
    return {
        "url": parsed["url"],
        "title": parsed["title"],
        "text_length": len(parsed["text"]),
        "links_count": len(parsed["links"]),
        "images_count": len(parsed["images"]),
        "links": parsed["links"][:5],
    }


async def parse_pages(urls: list[str]) -> list[dict]:
    crawler = AsyncCrawler(max_concurrent=5)

    try:
        tasks = [crawler.fetch_and_parse(url) for url in urls]
        parsed_pages = await asyncio.gather(*tasks)
    finally:
        await crawler.close()

    return [summarize(page) for page in parsed_pages]


def print_summaries(title: str, summaries: list[dict]) -> None:
    print(f"\n{title}")
    for summary in summaries:
        print(json.dumps(summary, indent=2, ensure_ascii=False))


async def main() -> None:
    sequential_results, sequential_time = await fetch_sequentially(URLS)
    print_results("Sequential fetching", sequential_results, sequential_time)

    concurrent_results, concurrent_time = await fetch_concurrently(URLS)
    print_results("Concurrent fetching", concurrent_results, concurrent_time)

    if concurrent_time > 0:
        speedup = sequential_time / concurrent_time
        print(f"\nSpeedup: {speedup:.2f}x")

    summaries = await parse_pages(URLS)
    print_summaries("Parsing demo", summaries)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(main())
