import asyncio
import logging
import socket
from datetime import datetime, timezone
from time import perf_counter
from urllib.parse import urlsplit

import aiohttp

from circuit_breaker import CircuitBreaker
from crawler_queue import CrawlerQueue
from crawler_stats import CrawlerStats
from errors import (
    CrawlerError,
    NetworkError,
    ParseError,
    PermanentError,
    TransientError,
    classify_status,
)
from html_parser import HTMLParser
from net_guard import is_blocked_ip
from rate_limiter import RateLimiter
from retry_strategy import RetryStrategy
from robots_parser import RobotsParser
from semaphore_manager import SemaphoreManager
from storage import DataStorage
from url_filter import URLFilter

logger = logging.getLogger(__name__)


class FetchResult(str):
    def __new__(cls, text: str, status: int = 200, content_type: str = ""):
        result = super().__new__(cls, text)
        result.status = status
        result.content_type = content_type
        return result


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
        storage: DataStorage | None = None,
        stats: CrawlerStats | None = None,
        proxy: str | None = None,
        cookies: dict | None = None,
        allow_private_hosts: bool = False,
        max_page_bytes: int = 5_000_000,
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
        self.storage = storage
        self.stats = stats
        self.proxy = proxy
        self.cookies = cookies
        self.allow_private_hosts = allow_private_hosts
        self.max_page_bytes = max_page_bytes
        self._allowed_hosts: set[str] = set()
        self._storage_ready = False
        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.robots = RobotsParser(self._fetch_robots_text)
        self.blocked_urls: list[str] = []
        self.request_times: list[float] = []
        self.active_requests: int = 0

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

    async def _check_host_allowed(self, url: str) -> None:
        # Block requests to private/loopback/metadata addresses so a crawled page
        # can't point us at internal services (SSRF). Not airtight against DNS
        # rebinding, but stops the common cases. Validated hosts are cached.
        if self.allow_private_hosts:
            return
        host = urlsplit(url).hostname
        if host is None or host in self._allowed_hosts:
            return
        if is_blocked_ip(host):
            raise PermanentError(f"blocked non-public host: {host}", url=url)
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(host, None)
        except socket.gaierror:
            return  # let the real request surface a NetworkError
        for info in infos:
            ip = info[4][0]
            if is_blocked_ip(ip):
                raise PermanentError(
                    f"blocked non-public host: {host} -> {ip}", url=url
                )
        self._allowed_hosts.add(host)

    async def _read_capped(self, response: aiohttp.ClientResponse, url: str) -> str:
        length = response.headers.get("Content-Length")
        if length and length.isdigit() and int(length) > self.max_page_bytes:
            raise PermanentError(
                f"page too large ({length} bytes)", url=url, status=response.status
            )
        chunks = []
        total = 0
        async for chunk in response.content.iter_chunked(65536):
            total += len(chunk)
            if total > self.max_page_bytes:
                raise PermanentError(
                    f"page exceeds {self.max_page_bytes} bytes",
                    url=url,
                    status=response.status,
                )
            chunks.append(chunk)
        return b"".join(chunks).decode(response.charset or "utf-8", errors="replace")

    async def _http_get(
        self, url: str, timeout: aiohttp.ClientTimeout | None = None
    ) -> str:
        # Low-level HTTP GET: SSRF guard + error classification only. No robots
        # and no rate limiting — this is what fetches robots.txt itself, and it
        # is the primitive that the polite fetch_url builds on.
        await self._check_host_allowed(url)

        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
                cookies=self.cookies,
            )

        logger.debug("Fetching started: %s", url)
        get_kwargs = {} if timeout is None else {"timeout": timeout}
        if self.proxy is not None:
            get_kwargs["proxy"] = self.proxy
        try:
            async with self.session.get(url, **get_kwargs) as response:
                response.raise_for_status()
                status = response.status
                content_type = response.headers.get("Content-Type", "")
                content = await self._read_capped(response, url)

            logger.debug("Fetching completed: %s", url)
            return FetchResult(content, status, content_type)
        except aiohttp.ClientResponseError as error:
            raise classify_status(error.status, url) from error
        except asyncio.TimeoutError as error:
            raise TransientError(f"timeout for {url}", url=url) from error
        except aiohttp.ClientError as error:
            raise NetworkError(str(error), url=url) from error

    async def fetch_url(
        self, url: str, timeout: aiohttp.ClientTimeout | None = None
    ) -> str:
        # Public fetch: enforce robots.txt (load rules + can_fetch) and rate
        # limits before EVERY request. Because retries and fetch_urls go through
        # here, they stay polite automatically (day 4).
        domain = urlsplit(url).hostname
        crawl_delay = 0.0
        if self.respect_robots:
            await self.robots.fetch_robots(url)
            if not self.robots.can_fetch(url, self.user_agent):
                raise PermanentError("blocked by robots.txt", url=url)
            crawl_delay = self.robots.get_crawl_delay(url, self.user_agent)

        await self.rate_limiter.acquire(domain, crawl_delay)
        return await self._http_get(url, timeout)

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def bounded_fetch(url: str) -> str:
            async with semaphore:
                try:
                    return await self.fetch_url(url)
                except CrawlerError as error:
                    logger.warning("Fetch failed for %s: %s", url, error)
                    return ""

        content = await asyncio.gather(*(bounded_fetch(url) for url in urls))

        return dict(zip(urls, content))

    async def _ensure_storage_ready(self) -> None:
        if self.storage is not None and not self._storage_ready:
            await self.storage.init()
            self._storage_ready = True

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()

        self.session = None

        if self.storage is not None:
            await self.storage.close()

    async def _fetch_robots_text(self, url: str) -> str:
        # Use the low-level GET, not fetch_url: fetch_url would try to load
        # robots.txt to fetch robots.txt (infinite recursion). Still rate-limited
        # here, because it is a real request to the host.
        await self.rate_limiter.acquire(urlsplit(url).hostname)
        try:
            return await self._http_get(url)
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

        if self.respect_robots:
            await self.robots.fetch_robots(url)
            if not self.robots.can_fetch(url, self.user_agent):
                logger.info("Blocked by robots.txt: %s", url)
                self.blocked_urls.append(url)
                return None

        if self.circuit_breaker is not None and not self.circuit_breaker.allow(domain):
            logger.warning("Circuit open, skipping %s", url)
            self.failed_urls[url] = "circuit open"
            queue.mark_failed(url, "circuit open")
            return None

        self.active_requests += 1
        try:
            async with self.sem_manager.acquire(domain):
                attempt = {"n": 0}

                async def fetch_once() -> str:
                    self.request_times.append(perf_counter())
                    if self.timeout_growth > 1.0:
                        timeout = self._timeout_for_attempt(attempt["n"])
                        attempt["n"] += 1
                        return await self.fetch_url(url, timeout=timeout)
                    return await self.fetch_url(url)

                page = await self.retry_strategy.execute_with_retry(fetch_once)
        except CrawlerError as error:
            reason = f"{type(error).__name__}: {error}"
            logger.warning("Fetch failed for %s: %s", url, reason)
            self.failed_urls[url] = reason
            queue.mark_failed(url, reason)
            if self.circuit_breaker is not None:
                self.circuit_breaker.record_failure(domain)
            if self.stats is not None:
                self.stats.record_failure(url, getattr(error, "status", None))
            return None
        finally:
            self.active_requests -= 1

        if self.circuit_breaker is not None:
            self.circuit_breaker.record_success(domain)

        status_code = getattr(page, "status", 200)
        content_type = getattr(page, "content_type", "")

        try:
            result = await HTMLParser().parse_html(page, url)
        except Exception as error:  # noqa: BLE001
            parse_error = ParseError(str(error), url=url)
            reason = f"{type(parse_error).__name__}: {parse_error}"
            logger.warning("Parse failed for %s: %s", url, error)
            self.retry_strategy.stats.record_error(parse_error)
            self.failed_urls[url] = reason
            queue.mark_failed(url, reason)
            if self.stats is not None:
                self.stats.record_failure(url, status_code)
            return None

        self.processed_urls[url] = result
        queue.mark_processed(url)

        if self.stats is not None:
            self.stats.record_success(url, status_code)

        await self._save_record(url, result, status_code, content_type)

        return result

    def _build_record(
        self, url: str, result: dict, status_code: int, content_type: str
    ) -> dict:
        metadata = dict(result.get("metadata", {}))
        for extra in ("headings", "images", "lists", "tables"):
            metadata[extra] = result.get(extra, [])

        return {
            "url": url,
            "title": result.get("title", ""),
            "text": result.get("text", ""),
            "links": result.get("links", []),
            "metadata": metadata,
            "crawled_at": datetime.now(timezone.utc),
            "status_code": status_code,
            "content_type": content_type,
        }

    async def _save_record(
        self, url: str, result: dict, status_code: int, content_type: str
    ) -> None:
        if self.storage is None:
            return

        record = self._build_record(url, result, status_code, content_type)
        try:
            await self.storage.save(record)
        except CrawlerError as error:
            logger.warning("Failed to save %s: %s", url, error)

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = False,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> dict:
        await self._ensure_storage_ready()

        if not start_urls:
            return self.processed_urls

        started_at = perf_counter()
        queue = CrawlerQueue()
        url_filter = URLFilter(
            start_urls,
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
