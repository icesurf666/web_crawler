import asyncio
import random
import time


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be greater than zero")

        self.interval = 1.0 / requests_per_second
        self.per_domain = per_domain
        self.min_delay = min_delay
        self.jitter = jitter
        self._last_request: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _get_lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()

        return self._locks[key]

    async def acquire(self, domain: str | None = None, crawl_delay: float = 0.0) -> None:
        key = domain if self.per_domain and domain else "__global__"

        lock = self._get_lock(key)

        async with lock:
            effective = max(self.interval, self.min_delay, crawl_delay)
            now = time.monotonic()
            last = self._last_request.get(key, 0.0)
            wait = effective - (now - last)

            wait = max(wait, 0)
            if self.jitter:
                wait += random.uniform(0, self.jitter)

            if wait > 0:
                await asyncio.sleep(wait)

            self._last_request[key] = time.monotonic()
