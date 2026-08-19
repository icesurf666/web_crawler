import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager


class SemaphoreManager:
    def __init__(self, global_limit: int = 10, per_domain_limit: int = 2) -> None:
        self._global = asyncio.Semaphore(global_limit)
        self._per_domain_limit = per_domain_limit
        self._domain_semaphores: dict[str, asyncio.Semaphore] = {}
        self.active = 0

    def _get_domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        if domain not in self._domain_semaphores:
            sem = asyncio.Semaphore(self._per_domain_limit)
            self._domain_semaphores[domain] = sem
        return self._domain_semaphores[domain]

    @asynccontextmanager
    async def acquire(self, domain: str) -> AsyncGenerator[None]:
        domain_sem = self._get_domain_semaphore(domain)
        async with self._global, domain_sem:
            self.active += 1
            try:
                yield
            finally:
                self.active -= 1
