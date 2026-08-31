import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

import net_guard
from async_crawler import AsyncCrawler
from config import Config
from crawler import AdvancedCrawler, _config_from_args, build_parser
from crawler_stats import CrawlerStats
from errors import PermanentError
from sitemap_parser import SitemapParser
from url_filter import URLFilter

NS = 'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"'


def _sitemap_fetcher(pages: dict):
    async def fetch(url: str) -> str:
        if url not in pages:
            raise PermanentError("404", url=url, status=404)
        return pages[url]

    return fetch


@pytest.mark.asyncio
async def test_sitemap_parses_urlset():
    pages = {
        "http://s/sitemap.xml": (
            f"<urlset {NS}><url><loc>http://s/a</loc></url>"
            f"<url><loc>http://s/b</loc></url></urlset>"
        )
    }
    parser = SitemapParser(_sitemap_fetcher(pages))
    assert await parser.fetch_sitemap("http://s/sitemap.xml") == [
        "http://s/a",
        "http://s/b",
    ]


@pytest.mark.asyncio
async def test_sitemap_index_recurses():
    pages = {
        "http://s/index.xml": (
            f"<sitemapindex {NS}><sitemap><loc>http://s/sm1.xml</loc></sitemap>"
            f"<sitemap><loc>http://s/sm2.xml</loc></sitemap></sitemapindex>"
        ),
        "http://s/sm1.xml": f"<urlset {NS}><url><loc>http://s/1</loc></url></urlset>",
        "http://s/sm2.xml": f"<urlset {NS}><url><loc>http://s/2</loc></url></urlset>",
    }
    parser = SitemapParser(_sitemap_fetcher(pages))
    assert await parser.fetch_sitemap("http://s/index.xml") == [
        "http://s/1",
        "http://s/2",
    ]


@pytest.mark.asyncio
async def test_sitemap_handles_errors():
    parser = SitemapParser(_sitemap_fetcher({"http://s/bad.xml": "<urlset><loc>x"}))
    assert await parser.fetch_sitemap("http://s/bad.xml") == []
    assert await parser.fetch_sitemap("http://s/missing.xml") == []


def test_stats_aggregation():
    stats = CrawlerStats()
    stats.start()
    stats.record_success("http://a.com/1", 200)
    stats.record_success("http://a.com/2", 200)
    stats.record_success("http://b.com/1", 301)
    stats.record_failure("http://c.com/x", 404)
    stats.stop()

    data = stats.as_dict()
    assert data["total_pages"] == 4
    assert data["successful"] == 3
    assert data["failed"] == 1
    assert data["status_codes"] == {200: 2, 301: 1, 404: 1}
    assert data["top_domains"][0] == ("a.com", 2)


def test_stats_exports(tmp_path):
    stats = CrawlerStats()
    stats.start()
    stats.record_success("http://a.com/1", 200)
    stats.stop()

    json_path = tmp_path / "stats.json"
    html_path = tmp_path / "report.html"
    stats.export_to_json(str(json_path))
    stats.export_to_html_report(str(html_path))

    loaded = json.loads(json_path.read_text(encoding="utf-8"))
    assert loaded["successful"] == 1
    html = html_path.read_text(encoding="utf-8")
    assert "<html" in html
    assert "http" not in html.split("<body>")[0]  # self-contained head


def test_config_from_dict_rejects_unknown():
    with pytest.raises(ValueError):
        Config.from_dict({"bogus": 1})


def test_config_from_yaml(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(
        "start_urls:\n  - http://a\nmax_pages: 5\nrequests_per_second: 3.0\n",
        encoding="utf-8",
    )
    config = Config.from_file(str(path))
    assert config.start_urls == ["http://a"]
    assert config.max_pages == 5
    assert config.requests_per_second == 3.0


def test_config_from_json(tmp_path):
    path = tmp_path / "c.json"
    path.write_text('{"start_urls": ["http://a"], "max_depth": 4}', encoding="utf-8")
    config = Config.from_file(str(path))
    assert config.start_urls == ["http://a"]
    assert config.max_depth == 4


def test_cli_args_override_config():
    parser = build_parser()
    args = parser.parse_args(
        ["--urls", "http://a", "http://b", "--max-pages", "50", "--no-respect-robots"]
    )
    config = _config_from_args(args)
    assert config.start_urls == ["http://a", "http://b"]
    assert config.max_pages == 50
    assert config.respect_robots is False


def test_is_blocked_ip():
    assert net_guard.is_blocked_ip("127.0.0.1")
    assert net_guard.is_blocked_ip("10.0.0.5")
    assert net_guard.is_blocked_ip("192.168.1.1")
    assert net_guard.is_blocked_ip("169.254.169.254")  # cloud metadata
    assert net_guard.is_blocked_ip("::1")
    assert not net_guard.is_blocked_ip("8.8.8.8")
    assert not net_guard.is_blocked_ip("example.com")  # not an IP literal


@pytest.mark.asyncio
async def test_fetch_blocks_private_host():
    crawler = AsyncCrawler()  # allow_private_hosts defaults to False
    with pytest.raises(PermanentError):
        await crawler.fetch_url("http://169.254.169.254/latest/meta-data/")
    await crawler.close()


@pytest.mark.asyncio
async def test_fetch_rejects_oversized_page():
    async def big(_request):
        return web.Response(body=b"x" * 10_000, content_type="text/html")

    app = web.Application()
    app.router.add_get("/", big)
    server = TestServer(app)
    await server.start_server()

    crawler = AsyncCrawler(allow_private_hosts=True, max_page_bytes=1000)
    try:
        with pytest.raises(PermanentError):
            await crawler.fetch_url(str(server.make_url("/")))
    finally:
        await crawler.close()
        await server.close()


@pytest.mark.asyncio
async def test_crawl_with_no_urls_returns_empty():
    crawler = AsyncCrawler(respect_robots=False)
    result = await crawler.crawl([])
    await crawler.close()
    assert result == {}


def test_url_filter_rejects_bad_pattern():
    with pytest.raises(ValueError):
        URLFilter("http://x", exclude_patterns=["("])


def test_config_wires_engine_options():
    config = Config(
        max_retries=7,
        backoff_factor=3.0,
        connect_timeout=2.0,
        timeout_growth=2.0,
        circuit_breaker=True,
        circuit_failure_threshold=4,
    )
    crawler = AdvancedCrawler(config)

    engine = crawler.crawler
    assert engine.retry_strategy.max_retries == 7
    assert engine.retry_strategy.backoff_factor == 3.0
    assert engine.connect_timeout == 2.0
    assert engine.timeout_growth == 2.0
    assert engine.circuit_breaker is not None
    assert engine.circuit_breaker.failure_threshold == 4


def test_config_circuit_breaker_off_by_default():
    crawler = AdvancedCrawler(Config())
    assert crawler.crawler.circuit_breaker is None


@pytest.mark.asyncio
async def test_advanced_crawler_runs_and_collects_stats(tmp_path):
    config = Config(
        start_urls=["http://site/"],
        max_pages=5,
        max_depth=1,
        requests_per_second=1000,
        respect_robots=False,
        same_domain_only=True,
        output_json=str(tmp_path / "out.jsonl"),
    )
    crawler = AdvancedCrawler(config)

    async def fake(url):
        pages = {
            "http://site/": "<html><head><title>H</title></head>"
            "<body><a href='http://site/a'>a</a></body></html>",
            "http://site/a": "<html><head><title>A</title></head><body>ok</body></html>",
        }
        return pages.get(url, "<html><body>x</body></html>")

    crawler.crawler.fetch_url = fake
    try:
        await crawler.crawl()
    finally:
        await crawler.close()

    stats = crawler.get_stats()
    assert stats["total_pages"] == 2
    assert stats["successful"] == 2
    assert (tmp_path / "out.jsonl").exists()
