import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

import retry_strategy
from async_crawler import AsyncCrawler
from errors import TransientError
from url_filter import URLFilter


@pytest.fixture
def no_backoff_sleep(monkeypatch):
    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(retry_strategy.asyncio, "sleep", fake_sleep)


@pytest.mark.asyncio
async def test_fetch_urls_survives_individual_failures() -> None:
    crawler = AsyncCrawler()

    async def fake(url: str) -> str:
        if "bad" in url:
            raise TransientError("boom", url=url, status=503)
        return "OK"

    crawler.fetch_url = fake

    results = await crawler.fetch_urls(["http://good", "http://bad", "http://good2"])
    await crawler.close()

    # One failing URL must not sink the whole batch; failures map to "".
    assert results == {"http://good": "OK", "http://bad": "", "http://good2": "OK"}


def test_url_filter_allows_all_start_domains() -> None:
    multi = URLFilter(["https://a.com", "https://b.com"], same_domain_only=True)

    assert multi.allows("https://a.com/x") is True
    assert multi.allows("https://b.com/y") is True
    assert multi.allows("https://c.com/z") is False


def test_url_filter_accepts_single_string() -> None:
    single = URLFilter("https://a.com", same_domain_only=True)

    assert single.allows("https://a.com/x") is True
    assert single.allows("https://other.com/x") is False


@pytest.mark.asyncio
async def test_every_request_is_rate_limited(no_backoff_sleep) -> None:
    calls = {"n": 0}

    async def handler(request: web.Request) -> web.Response:
        calls["n"] += 1
        if calls["n"] <= 2:
            return web.Response(status=503, text="down")
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/x", handler)
    server = TestServer(app)
    await server.start_server()
    url = str(server.make_url("/x"))

    crawler = AsyncCrawler(
        respect_robots=False, requests_per_second=1000, allow_private_hosts=True
    )

    acquired = {"n": 0}
    original_acquire = crawler.rate_limiter.acquire

    async def counting_acquire(*args, **kwargs) -> None:
        acquired["n"] += 1
        await original_acquire(*args, **kwargs)

    crawler.rate_limiter.acquire = counting_acquire

    # Rate limiting lives inside fetch_url, so retries go through it too:
    # two 503 retries + one success = three rate-limited requests.
    result = await crawler.retry_strategy.execute_with_retry(crawler.fetch_url, url)
    await crawler.close()
    await server.close()

    assert result == "OK"
    assert acquired["n"] == 3
