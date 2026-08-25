import logging
import time
from enum import Enum

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class _DomainState:
    def __init__(self):
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.opened_at = 0.0


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_time: float = 30.0):
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")

        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self._domains: dict[str, _DomainState] = {}

    def _get(self, domain: str) -> _DomainState:
        if domain not in self._domains:
            self._domains[domain] = _DomainState()

        return self._domains[domain]

    def allow(self, domain):
        entry = self._get(domain)
        if entry.state == CircuitState.OPEN:
            if time.monotonic() - entry.opened_at >= self.recovery_time:
                entry.state = CircuitState.HALF_OPEN
                logger.info("Circuit half-open for %s (probing)", domain)
                return True
            return False
        return True

    def record_success(self, domain):
        entry = self._get(domain)
        entry.state = CircuitState.CLOSED
        entry.failures = 0

    def _trip(self, entry):
        entry.state = CircuitState.OPEN
        entry.opened_at = time.monotonic()

    def record_failure(self, domain):
        entry = self._get(domain)
        if entry.state == CircuitState.HALF_OPEN:
            self._trip(entry)
            return
        entry.failures += 1
        if entry.failures >= self.failure_threshold:
            self._trip(entry)

    def state(self, domain: str) -> CircuitState:
        return self._get(domain).state
