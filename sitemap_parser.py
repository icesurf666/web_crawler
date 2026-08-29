import logging
import xml.etree.ElementTree as ET
from collections.abc import Awaitable, Callable

from errors import CrawlerError

logger = logging.getLogger(__name__)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _find_locs(root: ET.Element) -> list[str]:
    locs = []
    for element in root.iter():
        if _local_name(element.tag) == "loc" and element.text:
            locs.append(element.text.strip())
    return locs


class SitemapParser:
    def __init__(
        self,
        fetcher: Callable[[str], Awaitable[str]],
        max_index_depth: int = 3,
    ) -> None:
        self.fetcher = fetcher
        self.max_index_depth = max_index_depth

    async def fetch_sitemap(self, sitemap_url: str, _depth: int = 0) -> list[str]:
        try:
            text = await self.fetcher(sitemap_url)
        except CrawlerError as error:
            logger.warning("Failed to fetch sitemap %s: %s", sitemap_url, error)
            return []

        if not text:
            return []

        try:
            root = ET.fromstring(text)
        except ET.ParseError as error:
            logger.warning("Malformed sitemap %s: %s", sitemap_url, error)
            return []

        if _local_name(root.tag) == "sitemapindex":
            if _depth >= self.max_index_depth:
                logger.warning("Sitemap index too deep, stopping at %s", sitemap_url)
                return []
            urls = []
            for child_url in _find_locs(root):
                urls.extend(await self.fetch_sitemap(child_url, _depth + 1))
            return urls

        return _find_locs(root)
