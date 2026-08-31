# Async Web Crawler

A web crawler written with `asyncio` and `aiohttp`. It fetches many pages at once
instead of one at a time, stays polite (honours `robots.txt` and rate limits),
retries when a request fails for a temporary reason, and saves what it finds to
JSON, CSV, or PostgreSQL. When it's done it can print a summary and write an HTML
report.

It was built over seven days, one feature at a time, so each part lives in its own
module and can be read on its own.

## What it does

- Fetches pages concurrently, with a global limit and a smaller per-domain limit
- Obeys `robots.txt` (rules and crawl-delay), rate-limits requests, and can add
  jitter so it doesn't hammer a host in lockstep
- Tells transient failures (timeouts, 503, 429) apart from permanent ones (404,
  403) and only retries the ones worth retrying, backing off between attempts
- Opens a per-domain circuit breaker when a host keeps failing, so it stops
  wasting requests on it for a while
- Saves each page to JSONL, CSV, PostgreSQL, or all of them at once
- Can seed the crawl from a `sitemap.xml` (following sitemap indexes)
- Collects stats — status codes, top domains, speed — and exports them to JSON or
  a standalone HTML report
- Reads its settings from a YAML/JSON file or command-line flags
- Logs to a file and the console, and shows live progress while it runs
- Won't follow links into private/loopback/metadata addresses (SSRF guard),
  and caps how much it reads per page so a huge response can't exhaust memory

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

PostgreSQL storage is optional. If you want it, create a database and point the
crawler at it with `postgres_dsn`:

```bash
createdb web_crawler
```

## Running it

From the command line:

```bash
python crawler.py --urls https://example.com --max-pages 100 --output results.jsonl
python crawler.py --config config.yaml --report report.html
```

From Python:

```python
import asyncio
from crawler import AdvancedCrawler

async def main():
    crawler = AdvancedCrawler.from_config("config.yaml")
    await crawler.crawl()

    stats = crawler.get_stats()
    print(f"Crawled {stats['total_pages']} pages "
          f"({stats['successful']} ok, {stats['failed']} failed)")

    crawler.export_to_html_report("report.html")
    await crawler.close()

asyncio.run(main())
```

## Configuration

Settings live in `config.yaml` (or a `.json` file). Anything you don't set falls
back to the default below. Command-line flags win over the file.

| Key | Default | What it does |
|-----|---------|--------------|
| `start_urls` | `[]` | Where to start |
| `sitemap_url` | `null` | Sitemap to pull extra URLs from |
| `max_pages` | `100` | Stop after this many pages |
| `max_depth` | `2` | How many links deep to follow |
| `max_concurrent` | `10` | Total requests in flight at once |
| `per_domain_limit` | `2` | In-flight requests per domain |
| `requests_per_second` | `1.0` | Rate limit |
| `respect_robots` | `true` | Obey `robots.txt` |
| `user_agent` | `MyBot/1.0` | User-Agent header |
| `min_delay` / `jitter` | `0.0` | Extra delay / random wiggle between requests |
| `same_domain_only` | `false` | Don't leave the starting domain |
| `exclude_patterns` / `include_patterns` | `[]` | URL regex filters |
| `output_json` | `null` | JSONL results file |
| `output_csv` | `null` | CSV results file |
| `postgres_dsn` | `null` | PostgreSQL connection string |
| `connect_timeout` / `read_timeout` | `10` / `30` | Socket timeouts, seconds |
| `total_timeout` | `null` | Cap on the whole request, seconds |
| `timeout_growth` | `1.0` | `>1.0` gives each retry a longer timeout |
| `max_retries` | `3` | Retries for a transient failure |
| `backoff_factor` | `2.0` | How fast the wait grows between retries |
| `circuit_breaker` | `false` | Turn the per-domain breaker on |
| `circuit_failure_threshold` | `5` | Failures before a domain is skipped |
| `circuit_recovery_time` | `30.0` | Seconds before it's given another try |
| `allow_private_hosts` | `false` | Let the crawler reach localhost / internal IPs |
| `max_page_bytes` | `5000000` | Skip pages larger than this |
| `log_file` | `null` | Log file (rotated) |
| `log_level` | `INFO` | Log level |
| `proxy` | `null` | HTTP proxy URL |
| `cookies` | `null` | Cookies to send with every request |

## Command-line flags

| Flag | What it does |
|------|--------------|
| `--urls URL [URL ...]` | Start URLs |
| `--config PATH` | YAML/JSON config file |
| `--max-pages N` | Max pages |
| `--max-depth N` | Max depth |
| `--output PATH` | JSONL results file |
| `--report PATH` | HTML report |
| `--rate-limit R` | Requests per second |
| `--respect-robots` / `--no-respect-robots` | Toggle robots.txt |

Flags override whatever `--config` set.

## What a saved page looks like

```json
{
  "url": "...", "title": "...", "text": "...",
  "links": ["..."], "metadata": { "...": "..." },
  "crawled_at": "2026-08-29T12:00:00+00:00",
  "status_code": 200, "content_type": "text/html"
}
```

## The modules

| Module | What it handles |
|--------|-----------------|
| `crawler.py` | `AdvancedCrawler` and the CLI — the front door |
| `async_crawler.py` | The core engine (`AsyncCrawler`) that does the crawling |
| `crawler_queue.py` | The frontier queue, with dedup and status |
| `url_filter.py` | Domain / include / exclude filtering |
| `rate_limiter.py` | Per-domain rate limiting and jitter |
| `semaphore_manager.py` | Global and per-domain concurrency limits |
| `robots_parser.py` | `robots.txt` rules and crawl-delay |
| `sitemap_parser.py` | `sitemap.xml` and sitemap indexes |
| `html_parser.py` | Pulling title / text / links / metadata out of a page |
| `errors.py` | The error types and HTTP status classification |
| `retry_strategy.py` | The retry-with-backoff policy |
| `circuit_breaker.py` | The per-domain circuit breaker |
| `storage.py` | JSON / CSV / Postgres / composite storage |
| `crawler_stats.py` | Stats and the JSON/HTML export |
| `config.py` | Loading the config file |
| `progress.py` | The live progress line |
| `net_guard.py` | Classifying private/reserved IPs for the SSRF guard |

## Tests

```bash
python -m pytest -q
```

The PostgreSQL tests skip themselves if there's no server to talk to. To point
them at a test database, set `CRAWLER_TEST_DSN` (it defaults to
`postgresql://mac@localhost/web_crawler_test`).

## Benchmark

```bash
python benchmark.py
```

Runs the async crawler against a plain sequential (`urllib`) one at 100 / 500 /
1000 pages, using a local server with a little simulated latency, and reports peak
memory. The async version pulls ahead once there's any latency to hide behind.

## Demos

```bash
python demo_day7.py   # the full crawler on a small local site
python demo_day6.py   # saving to JSON + CSV + PostgreSQL
python demo_polite.py # robots.txt and rate limiting
```
