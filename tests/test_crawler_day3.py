import pytest

from async_crawler import AsyncCrawler
from crawler_queue import CrawlerQueue
from url_filter import URLFilter


def make_html(links: list[str]) -> str:
    anchors = "".join(f'<a href="{link}">link</a>' for link in links)
    return f"<html><body>{anchors}</body></html>"


def fake_crawler(pages: dict[str, list[str]], **kwargs):
    kwargs.setdefault("respect_robots", False)
    kwargs.setdefault("requests_per_second", 1000)
    crawler = AsyncCrawler(**kwargs)
    fetch_counts: dict[str, int] = {}

    async def fake_fetch(url: str) -> str:
        fetch_counts[url] = fetch_counts.get(url, 0) + 1
        return make_html(pages.get(url, []))

    crawler.fetch_url = fake_fetch
    return crawler, fetch_counts


@pytest.mark.asyncio
async def test_queue_orders_by_priority() -> None:
    queue = CrawlerQueue()
    queue.add_url("low", priority=0)
    queue.add_url("high", priority=10)
    queue.add_url("mid", priority=5)

    order = []
    while True:
        url = await queue.get_next()
        if url is None:
            break
        order.append(url)

    assert order == ["high", "mid", "low"]


@pytest.mark.asyncio
async def test_queue_skips_duplicates() -> None:
    queue = CrawlerQueue()
    queue.add_url("http://site/a")
    queue.add_url("http://site/a")

    first = await queue.get_next()
    second = await queue.get_next()

    assert first == "http://site/a"
    assert second is None


def test_url_filter_same_domain_only() -> None:
    url_filter = URLFilter("https://example.com", same_domain_only=True)

    assert url_filter.allows("https://example.com/page") is True
    assert url_filter.allows("https://other.com/page") is False


def test_url_filter_exclude_and_include() -> None:
    excluding = URLFilter("https://example.com", exclude_patterns=[r"\.pdf$"])
    assert excluding.allows("https://example.com/file.pdf") is False
    assert excluding.allows("https://example.com/page") is True

    including = URLFilter("https://example.com", include_patterns=[r"/blog/"])
    assert including.allows("https://example.com/blog/post") is True
    assert including.allows("https://example.com/shop") is False


@pytest.mark.asyncio
async def test_crawl_respects_max_depth() -> None:
    pages = {
        "http://site/0": ["http://site/1"],
        "http://site/1": ["http://site/2"],
        "http://site/2": ["http://site/3"],
    }
    crawler, _ = fake_crawler(pages, max_concurrent=5, max_depth=1)

    processed = await crawler.crawl(["http://site/0"], max_pages=100)

    assert set(processed) == {"http://site/0", "http://site/1"}
    assert "http://site/2" not in crawler.visited_urls


@pytest.mark.asyncio
async def test_crawl_has_no_duplicate_processing() -> None:
    pages = {
        "http://site/a": ["http://site/b", "http://site/c"],
        "http://site/b": ["http://site/a", "http://site/c"],
        "http://site/c": ["http://site/a"],
    }
    crawler, fetch_counts = fake_crawler(pages, max_concurrent=5, max_depth=5)

    await crawler.crawl(["http://site/a"], max_pages=100)

    assert crawler.visited_urls == {"http://site/a", "http://site/b", "http://site/c"}
    assert all(count == 1 for count in fetch_counts.values())
