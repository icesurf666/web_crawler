import asyncio
import logging
import random
from collections import Counter

from errors import CrawlerError, NetworkError, TransientError

logger = logging.getLogger(__name__)


class RetryStats:
    def __init__(self):
        self.errors_by_type = Counter()
        self.retries_attempted = 0
        self.retry_successes = 0
        self.total_retry_wait = 0.0
        self.final_failures = {}

    def record_error(self, error):
        self.errors_by_type[type(error).__name__] += 1

    def record_retry_wait(self, seconds):
        self.retries_attempted += 1
        self.total_retry_wait += seconds

    def record_retry_success(self):
        self.retry_successes += 1

    def record_final_failure(self, error):
        key = error.url or "<unknown>"
        self.final_failures[key] = f"{type(error).__name__}: {error}"

    @property
    def avg_retry_wait(self):
        if self.retries_attempted == 0:
            return 0.0
        return self.total_retry_wait / self.retries_attempted


class RetryStrategy:
    def __init__(
        self,
        max_retries: int | dict[type[CrawlerError], int] = 3,
        backoff_factor: float = 2.0,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        retry_on: list | None = None,
        jitter: float = 0.0,
        rate_limit_multiplier: float = 3.0,
        default_retries: int = 0,
    ):
        limits = max_retries.values() if isinstance(max_retries, dict) else [max_retries]
        if any(limit < 0 for limit in limits) or default_retries < 0:
            raise ValueError("retry limits must not be negative")

        self.max_retries = max_retries
        self.default_retries = default_retries
        self.backoff_factor = backoff_factor
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retry_on = tuple(retry_on) if retry_on else (TransientError, NetworkError)
        self.jitter = jitter
        self.rate_limit_multiplier = rate_limit_multiplier
        self.stats = RetryStats()

    def _max_for(self, error: CrawlerError) -> int:
        if not isinstance(self.max_retries, dict):
            return self.max_retries
        for error_type, limit in self.max_retries.items():
            if isinstance(error, error_type):
                return limit
        return self.default_retries

    def _delay_for(self, error, attempt: int) -> float:
        delay = self.base_delay * (self.backoff_factor**attempt)
        if getattr(error, "status", None) == 429:
            delay *= self.rate_limit_multiplier
        delay = min(delay, self.max_delay)
        if self.jitter:
            delay += random.uniform(0, self.jitter)
        return delay

    async def execute_with_retry(self, func, *args, **kwargs):
        attempt = 0
        while True:
            try:
                result = await func(*args, **kwargs)
            except self.retry_on as error:
                self.stats.record_error(error)
                max_retries = self._max_for(error)
                if attempt >= max_retries:
                    self.stats.record_final_failure(error)
                    logger.error(
                        "%s giving up on %s after %d attempt(s): %s",
                        type(error).__name__,
                        error.url,
                        attempt + 1,
                        error,
                    )
                    raise
                wait = self._delay_for(error, attempt)
                self.stats.record_retry_wait(wait)
                logger.warning(
                    "%s on %s (attempt %d/%d), retrying in %.2fs: %s",
                    type(error).__name__,
                    error.url,
                    attempt + 1,
                    max_retries + 1,
                    wait,
                    error,
                )
                await asyncio.sleep(wait)
                attempt += 1
            except CrawlerError as error:
                self.stats.record_error(error)
                self.stats.record_final_failure(error)
                logger.info(
                    "%s on %s is not retryable: %s",
                    type(error).__name__,
                    error.url,
                    error,
                )
                raise
            else:
                if attempt > 0:
                    self.stats.record_retry_success()
                return result
