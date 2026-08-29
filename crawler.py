import argparse
import asyncio
import logging
from logging.handlers import RotatingFileHandler

from async_crawler import AsyncCrawler
from circuit_breaker import CircuitBreaker
from config import Config
from crawler_stats import CrawlerStats
from progress import ProgressMonitor
from retry_strategy import RetryStrategy
from sitemap_parser import SitemapParser
from storage import CompositeStorage, CSVStorage, JSONStorage, PostgresStorage

logger = logging.getLogger(__name__)


def setup_logging(log_file: str | None = None, level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())
    root.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


def _build_storage(config: Config):
    storages = []
    if config.output_json:
        storages.append(JSONStorage(config.output_json))
    if config.output_csv:
        storages.append(CSVStorage(config.output_csv))
    if config.postgres_dsn:
        storages.append(PostgresStorage(config.postgres_dsn))

    if not storages:
        return None
    if len(storages) == 1:
        return storages[0]
    return CompositeStorage(storages)


class AdvancedCrawler:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.stats = CrawlerStats()
        self.progress = ProgressMonitor(total=config.max_pages)

        circuit_breaker = None
        if config.circuit_breaker:
            circuit_breaker = CircuitBreaker(
                failure_threshold=config.circuit_failure_threshold,
                recovery_time=config.circuit_recovery_time,
            )

        self.crawler = AsyncCrawler(
            max_concurrent=config.max_concurrent,
            max_depth=config.max_depth,
            per_domain_limit=config.per_domain_limit,
            requests_per_second=config.requests_per_second,
            respect_robots=config.respect_robots,
            user_agent=config.user_agent,
            min_delay=config.min_delay,
            jitter=config.jitter,
            retry_strategy=RetryStrategy(
                max_retries=config.max_retries,
                backoff_factor=config.backoff_factor,
            ),
            circuit_breaker=circuit_breaker,
            connect_timeout=config.connect_timeout,
            read_timeout=config.read_timeout,
            total_timeout=config.total_timeout,
            timeout_growth=config.timeout_growth,
            storage=_build_storage(config),
            stats=self.stats,
            proxy=config.proxy,
            cookies=config.cookies,
        )
        self.sitemap = SitemapParser(self.crawler.fetch_url)

    @classmethod
    def from_config(cls, path: str) -> "AdvancedCrawler":
        return cls(Config.from_file(path))

    async def _seed_urls(self) -> list[str]:
        urls = list(self.config.start_urls)
        if self.config.sitemap_url:
            logger.info("Loading sitemap: %s", self.config.sitemap_url)
            urls.extend(await self.sitemap.fetch_sitemap(self.config.sitemap_url))
        return urls

    async def _monitor(self) -> None:
        while True:
            processed = len(self.crawler.processed_urls) + len(self.crawler.failed_urls)
            self.progress.update(processed, self.crawler.active_requests)
            await asyncio.sleep(0.2)

    async def crawl(self) -> None:
        start_urls = await self._seed_urls()
        if not start_urls:
            raise ValueError("no start URLs (set start_urls or sitemap_url)")

        logger.info("Starting crawl of %d seed URL(s)", len(start_urls))
        self.stats.start()
        self.progress.start()
        monitor = asyncio.create_task(self._monitor())
        try:
            await self.crawler.crawl(
                start_urls,
                max_pages=self.config.max_pages,
                same_domain_only=self.config.same_domain_only,
                exclude_patterns=self.config.exclude_patterns or None,
                include_patterns=self.config.include_patterns or None,
            )
        finally:
            monitor.cancel()
            try:
                await monitor
            except asyncio.CancelledError:
                pass
            self.stats.stop()
            self.progress.update(self.stats.total_pages, 0)
            self.progress.finish()

        logger.info(
            "Crawl finished: %d pages in %.1fs",
            self.stats.total_pages,
            self.stats.elapsed,
        )

    def get_stats(self) -> dict:
        return self.stats.as_dict()

    def export_to_json(self, filename: str) -> None:
        self.stats.export_to_json(filename)

    def export_to_html_report(self, filename: str) -> None:
        self.stats.export_to_html_report(filename)

    async def close(self) -> None:
        await self.crawler.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Async web crawler")
    parser.add_argument("--urls", nargs="+", help="start URLs")
    parser.add_argument("--config", help="YAML or JSON config file")
    parser.add_argument("--max-pages", type=int, help="max pages to crawl")
    parser.add_argument("--max-depth", type=int, help="max crawl depth")
    parser.add_argument("--output", help="results file (JSONL)")
    parser.add_argument("--report", help="HTML stats report file")
    parser.add_argument("--rate-limit", type=float, help="requests per second")
    parser.add_argument(
        "--respect-robots",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="respect robots.txt",
    )
    return parser


def _config_from_args(args: argparse.Namespace) -> Config:
    config = Config.from_file(args.config) if args.config else Config()
    if args.urls:
        config.start_urls = args.urls
    if args.max_pages is not None:
        config.max_pages = args.max_pages
    if args.max_depth is not None:
        config.max_depth = args.max_depth
    if args.output is not None:
        config.output_json = args.output
    if args.rate_limit is not None:
        config.requests_per_second = args.rate_limit
    if args.respect_robots is not None:
        config.respect_robots = args.respect_robots
    return config


async def run_cli(args: argparse.Namespace) -> None:
    config = _config_from_args(args)
    setup_logging(config.log_file, config.log_level)

    crawler = AdvancedCrawler(config)
    try:
        await crawler.crawl()
    finally:
        await crawler.close()

    stats = crawler.get_stats()
    print(f"\nProcessed: {stats['total_pages']} pages")
    print(f"Successful: {stats['successful']}")
    print(f"Failed: {stats['failed']}")
    print(f"Speed: {stats['pages_per_second']} pages/sec")

    if args.report:
        crawler.export_to_html_report(args.report)
        print(f"Report saved to {args.report}")


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(run_cli(args))


if __name__ == "__main__":
    main()
