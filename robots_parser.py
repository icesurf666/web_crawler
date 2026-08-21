import asyncio
from collections.abc import Awaitable, Callable
from urllib.parse import urljoin, urlsplit
from urllib.robotparser import RobotFileParser


class RobotsParser:
    def __init__(self, fetcher: Callable[[str], Awaitable[str]]) -> None:
        self.fetcher = fetcher
        self._cache: dict[str, RobotFileParser] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _get_lock(self, domain: str) -> asyncio.Lock:
        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()

        return self._locks[domain]

    # переиспользуемый движок правил (кэшируется), а не снэпшот, не знаю насколько так можно и правильно.
    async def fetch_robots(self, base_url: str) -> RobotFileParser:
        domain = urlsplit(base_url).hostname

        if domain in self._cache:
            return self._cache[domain]

        async with self._get_lock(domain):
            if domain in self._cache:
                return self._cache[domain]

            robots_url = urljoin(base_url, "/robots.txt")
            text = await self.fetcher(robots_url)

            parser = RobotFileParser()
            parser.parse(text.splitlines())
            self._cache[domain] = parser

            return parser

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        domain = urlsplit(url).hostname
        parser = self._cache.get(domain)
        if parser is None:
            return True

        return parser.can_fetch(user_agent, url)

    def get_crawl_delay(self, url: str, user_agent: str = "*") -> float:
        domain = urlsplit(url).hostname
        parser = self._cache.get(domain)
        if parser is None:
            return 0.0

        delay = parser.crawl_delay(user_agent)
        return float(delay) if delay is not None else 0.0
