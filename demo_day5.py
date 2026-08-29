import asyncio
import json
import logging

from async_crawler import AsyncCrawler
from circuit_breaker import CircuitBreaker
from errors import PermanentError, TransientError
from retry_strategy import RetryStrategy

REPORT_PATH = "error_report.json"


class FlakySite:
    def __init__(self) -> None:
        self._attempts: dict[str, int] = {}
        self.rules: dict[str, tuple] = {
            "http://demo.local/": ("ok_after", 0),
            "http://demo.local/flaky": ("ok_after", 2),
            "http://demo.local/missing": ("permanent", 404),
            "http://demo.local/forbidden": ("permanent", 403),
            "http://down.local/a": ("always_transient",),
            "http://down.local/b": ("always_transient",),
            "http://down.local/c": ("always_transient",),
        }
        self.links = (
            '<a href="http://demo.local/flaky">flaky</a>'
            '<a href="http://demo.local/missing">missing</a>'
            '<a href="http://demo.local/forbidden">forbidden</a>'
            '<a href="http://down.local/a">down-a</a>'
            '<a href="http://down.local/b">down-b</a>'
            '<a href="http://down.local/c">down-c</a>'
        )

    async def fetch(self, url: str) -> str:
        rule = self.rules.get(url)
        if rule is None:
            return "<html><body>page</body></html>"

        attempt = self._attempts.get(url, 0)
        self._attempts[url] = attempt + 1
        kind = rule[0]

        if kind == "permanent":
            raise PermanentError(f"permanent HTTP {rule[1]}", url=url, status=rule[1])
        if kind == "always_transient":
            raise TransientError("service unavailable", url=url, status=503)
        if kind == "ok_after":
            if attempt < rule[1]:
                raise TransientError("temporary glitch", url=url, status=503)
            return self.links if url == "http://demo.local/" else "<html>ok</html>"

        return "<html><body>ok</body></html>"


def print_report(crawler: AsyncCrawler, stats: dict) -> None:
    print("\n=== Error handling summary ===")
    print(f"Processed pages   : {len(crawler.processed_urls)}")
    print(f"Failed URLs       : {len(stats['failed_urls'])}")
    print(f"Errors by type    : {stats['errors_by_type']}")
    print(f"Retries attempted : {stats['retries_attempted']}")
    print(f"Successful retries: {stats['retry_successes']}")
    print(f"Avg retry wait    : {stats['avg_retry_wait']:.2f} s")

    print("\nPermanent failures:")
    for url, reason in stats["final_failures"].items():
        print(f"  {url} -> {reason}")

    if crawler.circuit_breaker is not None:
        print("\nCircuit breaker state:")
        for domain in crawler.circuit_breaker._domains:
            print(f"  {domain} -> {crawler.circuit_breaker.state(domain).value}")


async def main() -> None:
    site = FlakySite()
    crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=1,
        respect_robots=False,
        requests_per_second=1000,
        retry_strategy=RetryStrategy(
            max_retries=3, base_delay=0.1, backoff_factor=2.0, jitter=0.05
        ),
        circuit_breaker=CircuitBreaker(failure_threshold=3, recovery_time=5.0),
    )
    crawler.fetch_url = site.fetch

    await crawler.crawl(["http://demo.local/"], max_pages=20)

    stats = crawler.get_error_stats()
    print_report(crawler, stats)

    with open(REPORT_PATH, "w", encoding="utf-8") as report_file:
        json.dump(stats, report_file, indent=2, ensure_ascii=False)
    print(f"\nError report saved to {REPORT_PATH}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    asyncio.run(main())
