import asyncio
import logging
from contextlib import contextmanager
from urllib.parse import urldefrag, urljoin, urlsplit

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@contextmanager
def safe(step):
    try:
        yield
    except Exception as error:  # noqa: BLE001 - parser must stay resilient to any failure
        logger.warning("Parsing step '%s' failed: %s", step, error)


class HTMLParser:
    async def parse_html(self, html: str, url: str) -> dict:
        # BeautifulSoup is synchronous and CPU-bound; run it in a thread so it
        # doesn't block the event loop and stall concurrent fetches.
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._parse_sync, html, url)

    def _parse_sync(self, html: str, url: str) -> dict:
        result = {
            "url": url,
            "title": "",
            "text": "",
            "links": [],
            "tables": [],
            "lists": [],
            "images": [],
            "headings": [],
            "metadata": {
                "title": "",
                "description": "",
                "keywords": "",
            },
        }

        if not html:
            return result

        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as error:  # noqa: BLE001 - malformed input must not crash the parser
            logger.warning("Failed to build soup for %s: %s", url, error)
            return result

        with safe("metadata"):
            result["metadata"] = self.extract_metadata(soup)
        result["title"] = result["metadata"]["title"]

        with safe("text"):
            result["text"] = self.extract_text(soup)
        with safe("links"):
            result["links"] = self.extract_links(soup, url)
        with safe("images"):
            result["images"] = self.extract_images(soup, url)
        with safe("headings"):
            result["headings"] = self.extract_headings(soup)
        with safe("lists"):
            result["lists"] = self.extract_lists(soup)
        with safe("tables"):
            result["tables"] = self.extract_tables(soup)

        return result

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        links = []

        for link_tag in soup.find_all("a"):
            href = link_tag.get("href")
            if not href:
                continue

            absolute_url = urljoin(base_url, href.strip())

            absolute_url = urldefrag(absolute_url).url
            parsed_url = urlsplit(absolute_url)

            if parsed_url.scheme not in ("http", "https"):
                continue

            if parsed_url.hostname is None:
                continue

            if absolute_url not in links:
                links.append(absolute_url)
        return links

    def extract_text(
        self,
        soup: BeautifulSoup,
        selector: str | None = None,
    ) -> str:
        target = soup.body or soup

        if selector is not None:
            target = soup.select_one(selector)

        if target is None:
            return ""

        return target.get_text(" ", strip=True)

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        metadata = {
            "title": "",
            "description": "",
            "keywords": "",
        }

        title_tag = soup.find("title")
        if title_tag is not None:
            metadata["title"] = title_tag.get_text(" ", strip=True)

        for name in ("description", "keywords"):
            meta_tag = soup.find("meta", attrs={"name": name})
            if meta_tag is not None:
                metadata[name] = meta_tag.get("content", "").strip()

        return metadata

    def extract_images(
        self,
        soup: BeautifulSoup,
        base_url: str,
    ) -> list[dict[str, str]]:
        images = []

        for image_tag in soup.find_all("img"):
            src = image_tag.get("src")
            if not src:
                continue

            absolute_src = urljoin(base_url, src.strip())
            parsed_url = urlsplit(absolute_src)

            if parsed_url.scheme not in ("http", "https"):
                continue

            if parsed_url.hostname is None:
                continue

            alt = image_tag.get("alt", "").strip()

            images.append(
                {
                    "src": absolute_src,
                    "alt": alt,
                }
            )

        return images

    def extract_headings(
        self,
        soup: BeautifulSoup,
    ) -> list[dict[str, str]]:
        headings = []

        for heading_tag in soup.find_all(["h1", "h2", "h3"]):
            headings.append(
                {
                    "level": heading_tag.name,
                    "text": heading_tag.get_text(" ", strip=True),
                }
            )

        return headings

    def extract_tables(
        self,
        soup: BeautifulSoup,
    ) -> list[list[list[str]]]:
        tables = []

        for table_tag in soup.find_all("table"):
            table_data = []

            for row_tag in table_tag.find_all("tr"):
                cells = row_tag.find_all(["th", "td"])
                row_data = [cell.get_text(" ", strip=True) for cell in cells]

                if row_data:
                    table_data.append(row_data)

            if table_data:
                tables.append(table_data)

        return tables

    def extract_lists(
        self,
        soup: BeautifulSoup,
    ) -> list[dict]:
        lists = []

        for list_tag in soup.find_all(["ol", "ul"]):
            items = [
                item_tag.get_text(" ", strip=True)
                for item_tag in list_tag.find_all("li")
            ]

            if items:
                lists.append(
                    {
                        "type": list_tag.name,
                        "items": items,
                    }
                )

        return lists
