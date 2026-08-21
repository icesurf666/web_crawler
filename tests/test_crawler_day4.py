import asyncio
import random
import time

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from async_crawler import AsyncCrawler
from rate_limiter import RateLimiter
from robots_parser import RobotsParser


class _FakeResponse:
    def raise_for_status(self) -> None:
        pass

    async def text(self) -> str:
        return "OK"


class _FakeGet:
    def __init__(self, error: Exception | None) -> None:
        self._error = error

    async def __aenter__(self) -> _FakeResponse:
        if self._error is not None:
            raise self._error
        return _FakeResponse()

    async def __aexit__(self, *args) -> bool:
        return False


class _FlakySession:
    def __init__(self, fail_times: int) -> None:
        self.closed = False
        self.calls = 0
        self._fail_times = fail_times

    def get(self, url: str) -> _FakeGet:
        self.calls += 1
        error = aiohttp.ClientConnectionError("boom") if self.calls <= self._fail_times else None
        return _FakeGet(error)

    async def close(self) -> None:
        self.closed = True


def crawler_with_pages(pages: dict[str, str], **kwargs) -> AsyncCrawler:
    kwargs.setdefault("requests_per_second", 1000)
    crawler = AsyncCrawler(max_concurrent=5, max_depth=1, **kwargs)

    async def fake_fetch(url: str) -> str:
        return pages.get(url, "<html><body>page</body></html>")

    crawler.fetch_url = fake_fetch
    return crawler


@pytest.mark.asyncio
async def test_rate_limit_single_domain() -> None:
    rate_limiter = RateLimiter(requests_per_second=10)  # 0.1s between requests

    start = time.monotonic()
    for _ in range(3):
        await rate_limiter.acquire("site.com")
    elapsed = time.monotonic() - start

    # 3 requests -> 2 gaps of 0.1s -> at least ~0.2s
    assert elapsed >= 0.2


@pytest.mark.asyncio
async def test_rate_limit_independent_per_domain() -> None:
    rate_limiter = RateLimiter(requests_per_second=10)

    start = time.monotonic()
    await asyncio.gather(
        rate_limiter.acquire("a.com"),
        rate_limiter.acquire("b.com"),
        rate_limiter.acquire("c.com"),
    )
    elapsed = time.monotonic() - start

    # different domains have independent limits -> no waiting
    assert elapsed < 0.05


@pytest.mark.asyncio
async def test_min_delay_respected() -> None:
    rate_limiter = RateLimiter(requests_per_second=1000, min_delay=0.2)

    start = time.monotonic()
    await rate_limiter.acquire("site.com")
    await rate_limiter.acquire("site.com")
    elapsed = time.monotonic() - start

    assert elapsed >= 0.2


@pytest.mark.asyncio
async def test_robots_parser_reads_rules() -> None:
    async def fake_fetch(url: str) -> str:
        return "User-agent: *\nDisallow: /admin/\nCrawl-delay: 2\n"

    robots = RobotsParser(fake_fetch)
    await robots.fetch_robots("https://site.com/")

    assert robots.can_fetch("https://site.com/admin/x") is False
    assert robots.can_fetch("https://site.com/public") is True
    assert robots.get_crawl_delay("https://site.com/any") == 2.0


@pytest.mark.asyncio
async def test_crawl_blocks_disallowed_urls() -> None:
    pages = {
        "http://site/robots.txt": "User-agent: *\nDisallow: /private/\n",
        "http://site/": (
            '<a href="http://site/ok">ok</a>'
            '<a href="http://site/private/secret">secret</a>'
        ),
    }
    crawler = crawler_with_pages(pages, respect_robots=True)

    results = await crawler.crawl(["http://site/"], max_pages=10)

    assert "http://site/private/secret" in crawler.blocked_urls
    assert "http://site/private/secret" not in results
    assert "http://site/ok" in results


@pytest.mark.asyncio
async def test_backoff_retries_on_transient_error(monkeypatch) -> None:
    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)

    crawler = AsyncCrawler()
    crawler.session = _FlakySession(fail_times=2)

    body = await crawler.fetch_url("http://x", retries=3)

    assert body == "OK"
    assert crawler.session.calls == 3


@pytest.mark.asyncio
async def test_global_rate_limit() -> None:
    rate_limiter = RateLimiter(requests_per_second=10, per_domain=False)

    start = time.monotonic()
    await asyncio.gather(
        rate_limiter.acquire("a.com"),
        rate_limiter.acquire("b.com"),
        rate_limiter.acquire("c.com"),
    )
    elapsed = time.monotonic() - start

    # global mode: all domains share one limit -> serialized -> ~0.2s
    assert elapsed >= 0.2


@pytest.mark.asyncio
async def test_speed_stats_reports_requests() -> None:
    pages = {
        "http://site/": (
            '<a href="http://site/a">a</a><a href="http://site/b">b</a>'
        ),
    }
    crawler = crawler_with_pages(pages, respect_robots=False)

    await crawler.crawl(["http://site/"], max_pages=10)
    stats = crawler.get_speed_stats()

    assert stats["requests"] == 3
    assert stats["requests"] == len(crawler.processed_urls)
    assert stats["blocked"] == 0


@pytest.mark.asyncio
async def test_jitter_adds_delay(monkeypatch) -> None:
    # make the random part deterministic: always return 0.15
    monkeypatch.setattr(random, "uniform", lambda _a, _b: 0.15)

    rate_limiter = RateLimiter(requests_per_second=1000, jitter=0.2)

    start = time.monotonic()
    await rate_limiter.acquire("site.com")
    elapsed = time.monotonic() - start

    # base interval is ~0.001s, so the delay comes from the jitter
    assert elapsed >= 0.15


@pytest.mark.asyncio
async def test_user_agent_header_is_set() -> None:
    async def echo_user_agent(request: web.Request) -> web.Response:
        return web.Response(text=request.headers.get("User-Agent", ""))

    app = web.Application()
    app.router.add_get("/ua", echo_user_agent)

    server = TestServer(app)
    await server.start_server()

    url = str(server.make_url("/ua"))
    crawler = AsyncCrawler(user_agent="TestBot/9.9")

    try:
        body = await crawler.fetch_url(url)
    finally:
        await crawler.close()
        await server.close()

    assert body == "TestBot/9.9"
