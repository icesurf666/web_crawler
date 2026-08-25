import asyncio

import pytest

import circuit_breaker
import retry_strategy
from async_crawler import AsyncCrawler
from circuit_breaker import CircuitBreaker, CircuitState
from errors import (
    NetworkError,
    PermanentError,
    TransientError,
    classify_status,
)
from retry_strategy import RetryStrategy


@pytest.fixture
def recorded_waits(monkeypatch):
    waits: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr(retry_strategy.asyncio, "sleep", fake_sleep)
    return waits


def test_classify_status_maps_codes() -> None:
    assert isinstance(classify_status(503), TransientError)
    assert isinstance(classify_status(429), TransientError)
    assert isinstance(classify_status(500), TransientError)
    assert isinstance(classify_status(404), PermanentError)
    assert isinstance(classify_status(403), PermanentError)


def test_classify_status_unknown_codes_fall_back_by_first_digit() -> None:
    assert isinstance(classify_status(418), PermanentError)
    assert isinstance(classify_status(599), TransientError)


def test_classified_error_carries_context() -> None:
    error = classify_status(404, url="http://x")
    assert error.status == 404
    assert error.url == "http://x"


@pytest.mark.asyncio
async def test_retries_transient_then_succeeds(recorded_waits) -> None:
    calls = {"n": 0}

    async def flaky() -> str:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise TransientError("down", url="http://x", status=503)
        return "OK"

    strategy = RetryStrategy(max_retries=3)
    result = await strategy.execute_with_retry(flaky)

    assert result == "OK"
    assert calls["n"] == 3
    assert strategy.stats.retry_successes == 1


@pytest.mark.asyncio
async def test_permanent_error_is_not_retried(recorded_waits) -> None:
    calls = {"n": 0}

    async def forbidden() -> str:
        calls["n"] += 1
        raise PermanentError("nope", url="http://x", status=403)

    strategy = RetryStrategy(max_retries=3)

    with pytest.raises(PermanentError):
        await strategy.execute_with_retry(forbidden)

    assert calls["n"] == 1
    assert recorded_waits == []


@pytest.mark.asyncio
async def test_network_error_is_retried(recorded_waits) -> None:
    calls = {"n": 0}

    async def flaky() -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise NetworkError("connection refused", url="http://x")
        return "OK"

    strategy = RetryStrategy(max_retries=3)
    assert await strategy.execute_with_retry(flaky) == "OK"
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_exponential_backoff_sequence(recorded_waits) -> None:
    async def always_fail() -> str:
        raise TransientError("down", url="http://x", status=503)

    strategy = RetryStrategy(max_retries=3, base_delay=1.0, backoff_factor=2.0)

    with pytest.raises(TransientError):
        await strategy.execute_with_retry(always_fail)

    assert recorded_waits == [1.0, 2.0, 4.0]


@pytest.mark.asyncio
async def test_backoff_is_capped_by_max_delay(recorded_waits) -> None:
    async def always_fail() -> str:
        raise TransientError("down", url="http://x", status=503)

    strategy = RetryStrategy(
        max_retries=3, base_delay=10.0, backoff_factor=10.0, max_delay=50.0
    )

    with pytest.raises(TransientError):
        await strategy.execute_with_retry(always_fail)

    assert recorded_waits == [10.0, 50.0, 50.0]


@pytest.mark.asyncio
async def test_rate_limited_gets_longer_delay(recorded_waits) -> None:
    async def rate_limited() -> str:
        raise TransientError("slow down", url="http://x", status=429)

    strategy = RetryStrategy(
        max_retries=1, base_delay=1.0, backoff_factor=2.0, rate_limit_multiplier=3.0
    )

    with pytest.raises(TransientError):
        await strategy.execute_with_retry(rate_limited)

    assert recorded_waits == [3.0]


@pytest.mark.asyncio
async def test_stats_track_errors_and_failures(recorded_waits) -> None:
    strategy = RetryStrategy(max_retries=2)

    async def flaky() -> str:
        if not hasattr(flaky, "done"):
            flaky.done = True
            raise TransientError("t", url="http://a", status=503)
        return "OK"

    async def permanent() -> str:
        raise PermanentError("p", url="http://b", status=404)

    await strategy.execute_with_retry(flaky)
    with pytest.raises(PermanentError):
        await strategy.execute_with_retry(permanent)

    stats = strategy.stats
    assert stats.errors_by_type["TransientError"] == 1
    assert stats.errors_by_type["PermanentError"] == 1
    assert stats.retry_successes == 1
    assert stats.avg_retry_wait > 0
    assert stats.final_failures == {"http://b": "PermanentError: p"}


@pytest.mark.asyncio
async def test_per_type_retry_limits(recorded_waits) -> None:
    strategy = RetryStrategy(max_retries={NetworkError: 4, TransientError: 1})

    net_calls = {"n": 0}

    async def network_fail() -> str:
        net_calls["n"] += 1
        raise NetworkError("refused", url="http://x")

    with pytest.raises(NetworkError):
        await strategy.execute_with_retry(network_fail)
    assert net_calls["n"] == 5

    transient_calls = {"n": 0}

    async def transient_fail() -> str:
        transient_calls["n"] += 1
        raise TransientError("down", url="http://y", status=503)

    with pytest.raises(TransientError):
        await strategy.execute_with_retry(transient_fail)
    assert transient_calls["n"] == 2


@pytest.mark.asyncio
async def test_type_absent_from_limits_uses_default(recorded_waits) -> None:
    strategy = RetryStrategy(max_retries={TransientError: 3}, default_retries=0)

    calls = {"n": 0}

    async def network_fail() -> str:
        calls["n"] += 1
        raise NetworkError("refused", url="http://x")

    with pytest.raises(NetworkError):
        await strategy.execute_with_retry(network_fail)
    assert calls["n"] == 1


@pytest.fixture
def fake_clock(monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(circuit_breaker.time, "monotonic", lambda: clock["t"])
    return clock


def test_circuit_opens_after_threshold(fake_clock) -> None:
    breaker = CircuitBreaker(failure_threshold=3, recovery_time=30.0)

    for _ in range(2):
        breaker.record_failure("site.com")
    assert breaker.allow("site.com") is True

    breaker.record_failure("site.com")
    assert breaker.state("site.com") == CircuitState.OPEN
    assert breaker.allow("site.com") is False


def test_circuit_recovers_through_half_open(fake_clock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_time=30.0)

    breaker.record_failure("site.com")
    assert breaker.allow("site.com") is False

    fake_clock["t"] += 31
    assert breaker.allow("site.com") is True
    assert breaker.state("site.com") == CircuitState.HALF_OPEN

    breaker.record_success("site.com")
    assert breaker.state("site.com") == CircuitState.CLOSED


def test_failed_probe_reopens_circuit(fake_clock) -> None:
    breaker = CircuitBreaker(failure_threshold=1, recovery_time=30.0)

    breaker.record_failure("site.com")
    fake_clock["t"] += 31
    breaker.allow("site.com")

    breaker.record_failure("site.com")
    assert breaker.state("site.com") == CircuitState.OPEN
    assert breaker.allow("site.com") is False


def test_circuits_are_isolated_per_domain(fake_clock) -> None:
    breaker = CircuitBreaker(failure_threshold=1)

    breaker.record_failure("bad.com")
    assert breaker.allow("bad.com") is False
    assert breaker.allow("good.com") is True


@pytest.mark.asyncio
async def test_crawler_retries_transient_then_processes(recorded_waits) -> None:
    calls = {"n": 0}

    async def flaky(url: str) -> str:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise TransientError("down", url=url, status=503)
        return "<html><body>ok</body></html>"

    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000, max_depth=0)
    crawler.fetch_url = flaky

    results = await crawler.crawl(["http://site/"], max_pages=1)

    assert "http://site/" in results
    assert crawler.retry_strategy.stats.retry_successes == 1


@pytest.mark.asyncio
async def test_crawler_records_permanent_failure(recorded_waits) -> None:
    async def not_found(url: str) -> str:
        raise PermanentError("missing", url=url, status=404)

    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000, max_depth=0)
    crawler.fetch_url = not_found

    results = await crawler.crawl(["http://site/"], max_pages=1)

    assert results == {}
    assert "http://site/" in crawler.failed_urls
    stats = crawler.get_error_stats()
    assert stats["errors_by_type"].get("PermanentError") == 1


@pytest.mark.asyncio
async def test_crawler_circuit_breaker_skips_failing_domain(recorded_waits) -> None:
    async def always_fail(url: str) -> str:
        raise TransientError("down", url=url, status=503)

    breaker = CircuitBreaker(failure_threshold=1, recovery_time=999.0)
    crawler = AsyncCrawler(
        respect_robots=False,
        requests_per_second=1000,
        max_depth=1,
        retry_strategy=RetryStrategy(max_retries=0),
        circuit_breaker=breaker,
    )
    crawler.fetch_url = always_fail

    await crawler.crawl(["http://site/"], max_pages=5)

    assert breaker.state("site") == CircuitState.OPEN
    assert "http://site/" in crawler.failed_urls


def test_timeout_for_attempt_scales_by_growth() -> None:
    crawler = AsyncCrawler(connect_timeout=1.0, read_timeout=2.0, timeout_growth=2.0)

    assert crawler._timeout_for_attempt(0).sock_read == 2.0
    assert crawler._timeout_for_attempt(1).sock_read == 4.0
    assert crawler._timeout_for_attempt(2).sock_read == 8.0
    assert crawler._timeout_for_attempt(2).connect == 4.0


def test_timeout_growth_disabled_by_default() -> None:
    crawler = AsyncCrawler(read_timeout=2.0)

    assert crawler._timeout_for_attempt(0).sock_read == 2.0
    assert crawler._timeout_for_attempt(3).sock_read == 2.0


@pytest.mark.asyncio
async def test_timeout_grows_across_retries(recorded_waits) -> None:
    seen: list[float] = []

    async def recorder(url: str, timeout=None) -> str:
        seen.append(timeout.sock_read)
        if len(seen) < 3:
            raise TransientError("down", url=url, status=503)
        return "<html>ok</html>"

    crawler = AsyncCrawler(
        respect_robots=False,
        requests_per_second=1000,
        max_depth=0,
        read_timeout=2.0,
        timeout_growth=2.0,
        retry_strategy=RetryStrategy(max_retries=3),
    )
    crawler.fetch_url = recorder

    await crawler.crawl(["http://site/"], max_pages=1)

    assert seen == [2.0, 4.0, 8.0]
