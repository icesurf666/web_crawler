import asyncio
import logging
from time import perf_counter
from urllib.parse import urlsplit

import aiohttp

from crawler_queue import CrawlerQueue
from html_parser import HTMLParser
from semaphore_manager import SemaphoreManager
from url_filter import URLFilter

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(
        self, max_concurrent: int = 10, max_depth: int = 2, per_domain_limit: int = 2
    ):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be greater than zero")

        self.max_concurrent = max_concurrent
        self.timeout = aiohttp.ClientTimeout(connect=10, sock_read=30)
        self.session: aiohttp.ClientSession | None = None

        self.sem_manager = SemaphoreManager(max_concurrent, per_domain_limit)
        self.max_depth = max_depth
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.url_depth: dict[str, int] = {}

    async def fetch_url(self, url: str) -> str:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=self.timeout)

        logger.info("Fetching started: %s", url)

        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                content = await response.text()

            logger.info("Fetching completed: %s", url)
            return content
        except aiohttp.ClientResponseError as error:
            logger.warning("HTTP error for %s: %s", url, error)
        except asyncio.TimeoutError as error:
            logger.warning("Timeout for %s: %s", url, error)
        except aiohttp.ClientError as error:
            logger.warning("Network error for %s: %s", url, error)

        return ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = []

        for url in urls:
            tasks.append(self.fetch_url(url))

        content = await asyncio.gather(*tasks)

        return dict(zip(urls, content))

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()

        self.session = None

    async def fetch_and_parse(self, url: str) -> dict:
        page = await self.fetch_url(url)
        html = await HTMLParser().parse_html(page, url)

        return html

    async def _process_url(self, url: str, queue: CrawlerQueue) -> dict | None:
        if url in self.visited_urls:
            return None
        self.visited_urls.add(url)

        domain = urlsplit(url).hostname

        async with self.sem_manager.acquire(domain):
            page = await self.fetch_url(url)

        if not page:
            self.failed_urls[url] = "fetch failed"
            queue.mark_failed(url, "fetch failed")
            return None

        result = await HTMLParser().parse_html(page, url)
        self.processed_urls[url] = result
        queue.mark_processed(url)
        return result

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = False,
        exclude_patterns=None,
        include_patterns=None,
    ) -> dict:
        started_at = perf_counter()
        queue = CrawlerQueue()
        url_filter = URLFilter(
            start_urls[0],
            same_domain_only=same_domain_only,
            exclude_patterns=exclude_patterns,
            include_patterns=include_patterns,
        )

        for url in start_urls:
            queue.add_url(url)
            self.url_depth[url] = 0

        depth = 0
        while depth <= self.max_depth:
            wave = []
            while True:
                url = await queue.get_next()
                if url is None:
                    break
                if len(self.visited_urls) + len(wave) >= max_pages:
                    break
                wave.append(url)

            if not wave:
                break

            results = await asyncio.gather(
                *(self._process_url(url, queue) for url in wave)
            )

            if depth < self.max_depth:
                for result in results:
                    if result is None:
                        continue
                    for link in result["links"]:
                        if url_filter.allows(link):
                            queue.add_url(link)
                            if link not in self.url_depth:
                                self.url_depth[link] = depth + 1

            elapsed = perf_counter() - started_at
            speed = len(self.processed_urls) / elapsed if elapsed > 0 else 0

            print(
                f"depth={depth} | processed={len(self.processed_urls)} | "
                f"queued={queue.get_stats()['queued']} | "
                f"failed={len(self.failed_urls)} | {speed:.1f} pages/sec"
            )

            depth += 1

        return self.processed_urls
