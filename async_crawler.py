import asyncio
import logging
from time import perf_counter
from urllib.parse import urlsplit

import aiohttp

from crawler_queue import CrawlerQueue
from html_parser import HTMLParser
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from semaphore_manager import SemaphoreManager
from url_filter import URLFilter

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_depth: int = 2,
        per_domain_limit: int = 2,
        requests_per_second: float = 1.0,
        respect_robots: bool = True,
        user_agent: str = "MyBot/1.0",
        min_delay: float = 0.0,
        jitter: float = 0.0,
    ):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be greater than zero")

        self.max_concurrent = max_concurrent
        self.timeout = aiohttp.ClientTimeout(connect=10, sock_read=30)
        self.session: aiohttp.ClientSession | None = None

        self.sem_manager = SemaphoreManager(max_concurrent, per_domain_limit)
        self.rate_limiter = RateLimiter(
            requests_per_second, min_delay=min_delay, jitter=jitter
        )
        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.robots = RobotsParser(lambda url: self.fetch_url(url))
        self.blocked_urls: list[str] = []
        self.request_times: list[float] = []

        self.max_depth = max_depth
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.url_depth: dict[str, int] = {}

    async def fetch_url(
        self,
        url: str,
        retries: int = 3,
        backoff_base: float = 1.0,
        backoff_factor: float = 2.0,
    ) -> str:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
            )

        logger.debug("Fetching started: %s", url)

        delay = backoff_base
        for attempt in range(retries):
            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    content = await response.text()

                logger.debug("Fetching completed: %s", url)
                return content
            except aiohttp.ClientResponseError as error:
                logger.warning("HTTP error for %s: %s", url, error)
                return ""
            except (asyncio.TimeoutError, aiohttp.ClientError) as error:
                logger.warning(
                    "Transient error for %s (attempt %d/%d): %s",
                    url,
                    attempt + 1,
                    retries,
                    error,
                )
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                    delay *= backoff_factor

        return ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def bounded_fetch(url: str) -> str:
            async with semaphore:
                return await self.fetch_url(url)

        content = await asyncio.gather(*(bounded_fetch(url) for url in urls))

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

        crawl_delay = 0.0
        if self.respect_robots:
            await self.robots.fetch_robots(url)
            if not self.robots.can_fetch(url, self.user_agent):
                logger.info("Blocked by robots.txt: %s", url)
                self.blocked_urls.append(url)
                return None
            crawl_delay = self.robots.get_crawl_delay(url, self.user_agent)

        await self.rate_limiter.acquire(domain, crawl_delay)

        self.request_times.append(perf_counter())

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
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
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

            logger.info(
                "depth=%d | processed=%d | queued=%d | failed=%d | "
                "blocked=%d | %.1f pages/sec",
                depth,
                len(self.processed_urls),
                queue.get_stats()["queued"],
                len(self.failed_urls),
                len(self.blocked_urls),
                speed,
            )

            depth += 1

        return self.processed_urls

    def get_speed_stats(self) -> dict:
        times = self.request_times

        if len(times) >= 2:
            span = times[-1] - times[0]
            req_per_sec = len(times) / span if span > 0 else 0.0
            avg_delay = span / (len(times) - 1)
        else:
            req_per_sec = 0.0
            avg_delay = 0.0

        return {
            "requests": len(times),
            "req_per_sec": req_per_sec,
            "avg_delay": avg_delay,
            "blocked": len(self.blocked_urls),
        }
