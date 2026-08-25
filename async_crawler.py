import asyncio
import logging
from time import perf_counter
from urllib.parse import urlsplit

import aiohttp

from circuit_breaker import CircuitBreaker
from crawler_queue import CrawlerQueue
from errors import (
    CrawlerError,
    NetworkError,
    ParseError,
    TransientError,
    classify_status,
)
from html_parser import HTMLParser
from rate_limiter import RateLimiter
from retry_strategy import RetryStrategy
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
        retry_strategy: RetryStrategy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        total_timeout: float | None = None,
        timeout_growth: float = 1.0,
    ):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be greater than zero")
        if timeout_growth < 1.0:
            raise ValueError("timeout_growth must be at least 1.0")

        self.max_concurrent = max_concurrent
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.total_timeout = total_timeout
        self.timeout_growth = timeout_growth
        self.timeout = aiohttp.ClientTimeout(
            connect=connect_timeout, sock_read=read_timeout, total=total_timeout
        )
        self.session: aiohttp.ClientSession | None = None

        self.sem_manager = SemaphoreManager(max_concurrent, per_domain_limit)
        self.rate_limiter = RateLimiter(
            requests_per_second, min_delay=min_delay, jitter=jitter
        )
        self.retry_strategy = retry_strategy or RetryStrategy()
        self.circuit_breaker = circuit_breaker
        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.robots = RobotsParser(self._fetch_robots_text)
        self.blocked_urls: list[str] = []
        self.request_times: list[float] = []

        self.max_depth = max_depth
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}
        self.url_depth: dict[str, int] = {}

    def _timeout_for_attempt(self, attempt: int) -> aiohttp.ClientTimeout:
        factor = self.timeout_growth**attempt
        return aiohttp.ClientTimeout(
            connect=self.connect_timeout * factor,
            sock_read=self.read_timeout * factor,
            total=self.total_timeout * factor if self.total_timeout else None,
        )

    async def fetch_url(
        self, url: str, timeout: aiohttp.ClientTimeout | None = None
    ) -> str:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
            )

        logger.debug("Fetching started: %s", url)
        get_kwargs = {} if timeout is None else {"timeout": timeout}
        try:
            async with self.session.get(url, **get_kwargs) as response:
                response.raise_for_status()
                content = await response.text()

            logger.debug("Fetching completed: %s", url)
            return content
        except aiohttp.ClientResponseError as error:
            raise classify_status(error.status, url) from error
        except asyncio.TimeoutError as error:
            raise TransientError(f"timeout for {url}", url=url) from error
        except aiohttp.ClientError as error:
            raise NetworkError(str(error), url=url) from error

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

    async def _fetch_robots_text(self, url: str) -> str:
        try:
            return await self.fetch_url(url)
        except CrawlerError as error:
            logger.debug("robots.txt unavailable for %s: %s", url, error)
            return ""

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

        if self.circuit_breaker is not None and not self.circuit_breaker.allow(domain):
            logger.warning("Circuit open, skipping %s", url)
            self.failed_urls[url] = "circuit open"
            queue.mark_failed(url, "circuit open")
            return None

        await self.rate_limiter.acquire(domain, crawl_delay)

        self.request_times.append(perf_counter())

        try:
            async with self.sem_manager.acquire(domain):
                if self.timeout_growth > 1.0:
                    attempt = {"n": 0}

                    async def fetch_with_growing_timeout() -> str:
                        timeout = self._timeout_for_attempt(attempt["n"])
                        attempt["n"] += 1
                        return await self.fetch_url(url, timeout=timeout)

                    page = await self.retry_strategy.execute_with_retry(
                        fetch_with_growing_timeout
                    )
                else:
                    page = await self.retry_strategy.execute_with_retry(
                        self.fetch_url, url
                    )
        except CrawlerError as error:
            reason = f"{type(error).__name__}: {error}"
            logger.warning("Fetch failed for %s: %s", url, reason)
            self.failed_urls[url] = reason
            queue.mark_failed(url, reason)
            if self.circuit_breaker is not None:
                self.circuit_breaker.record_failure(domain)
            return None

        if self.circuit_breaker is not None:
            self.circuit_breaker.record_success(domain)

        try:
            result = await HTMLParser().parse_html(page, url)
        except Exception as error:  # noqa: BLE001
            parse_error = ParseError(str(error), url=url)
            reason = f"{type(parse_error).__name__}: {parse_error}"
            logger.warning("Parse failed for %s: %s", url, error)
            self.retry_strategy.stats.record_error(parse_error)
            self.failed_urls[url] = reason
            queue.mark_failed(url, reason)
            return None

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

    def get_error_stats(self) -> dict:
        stats = self.retry_strategy.stats
        return {
            "errors_by_type": dict(stats.errors_by_type),
            "retries_attempted": stats.retries_attempted,
            "retry_successes": stats.retry_successes,
            "avg_retry_wait": stats.avg_retry_wait,
            "final_failures": dict(stats.final_failures),
            "failed_urls": dict(self.failed_urls),
            "blocked_urls": list(self.blocked_urls),
        }
