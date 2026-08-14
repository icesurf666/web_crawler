import asyncio
import logging

import aiohttp

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10):
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be greater than zero")

        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.timeout = aiohttp.ClientTimeout(connect=10, sock_read=30)
        self.session: aiohttp.ClientSession | None = None

    async def fetch_url(self, url: str) -> str:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=self.timeout)

        logger.info("Fetching started: %s", url)

        try:
            async with self.semaphore, self.session.get(url) as response:
                response.raise_for_status()
                content = await response.text()

            logger.info("Fetching completed: %s", url)
            return content
        except aiohttp.ClientResponseError as error:
            logger.warning("HTTP error for %s: %s", url, error)
        except asyncio.TimeoutError as error:
            logger.warning("Timeout for %s: %s", url, error)
        except aiohttp.ClientError as error:
            logger.warning("Network error for %s: %s", url, error)

        return ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        tasks = []

        for url in urls:
            tasks.append(self.fetch_url(url))

        content = await asyncio.gather(*tasks)

        return dict(zip(urls, content))

    async def close(self) -> None:
        if self.session is not None and not self.session.closed:
            await self.session.close()

        self.session = None
