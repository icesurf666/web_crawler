import json
import time
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlsplit


class CrawlerStats:
    def __init__(self) -> None:
        self.started_at: datetime | None = None
        self.finished_at: datetime | None = None
        self._start_monotonic: float | None = None
        self._elapsed: float = 0.0
        self.successful: int = 0
        self.failed: int = 0
        self.status_codes: Counter[int] = Counter()
        self.domain_counts: Counter[str] = Counter()

    def start(self) -> None:
        self.started_at = datetime.now(timezone.utc)
        self._start_monotonic = time.monotonic()

    def stop(self) -> None:
        self.finished_at = datetime.now(timezone.utc)
        if self._start_monotonic is not None:
            self._elapsed = time.monotonic() - self._start_monotonic

    def record_success(self, url: str, status_code: int) -> None:
        self.successful += 1
        self.status_codes[status_code] += 1
        domain = urlsplit(url).hostname
        if domain:
            self.domain_counts[domain] += 1

    def record_failure(self, url: str, status_code: int | None = None) -> None:
        self.failed += 1
        if status_code is not None:
            self.status_codes[status_code] += 1

    @property
    def total_pages(self) -> int:
        return self.successful + self.failed

    @property
    def elapsed(self) -> float:
        if self._start_monotonic is not None and self.finished_at is None:
            return time.monotonic() - self._start_monotonic
        return self._elapsed

    @property
    def pages_per_second(self) -> float:
        elapsed = self.elapsed
        return self.total_pages / elapsed if elapsed > 0 else 0.0

    def top_domains(self, limit: int = 10) -> list[tuple[str, int]]:
        return self.domain_counts.most_common(limit)

    def as_dict(self) -> dict:
        return {
            "total_pages": self.total_pages,
            "successful": self.successful,
            "failed": self.failed,
            "pages_per_second": round(self.pages_per_second, 2),
            "elapsed_seconds": round(self.elapsed, 2),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "status_codes": dict(sorted(self.status_codes.items())),
            "top_domains": self.top_domains(),
        }

    def export_to_json(self, filename: str) -> None:
        with open(filename, "w", encoding="utf-8") as handle:
            json.dump(self.as_dict(), handle, indent=2, ensure_ascii=False)

    def export_to_html_report(self, filename: str) -> None:
        with open(filename, "w", encoding="utf-8") as handle:
            handle.write(self._render_html())

    def _render_html(self) -> str:
        data = self.as_dict()
        tiles = _render_tiles(data)
        status_rows = _render_bars(
            [(str(code), count) for code, count in data["status_codes"].items()]
        )
        domain_rows = _render_bars(data["top_domains"])

        return _HTML_TEMPLATE.format(
            generated=data["finished_at"] or "",
            tiles=tiles,
            status_rows=status_rows or "<tr><td>no data</td></tr>",
            domain_rows=domain_rows or "<tr><td>no data</td></tr>",
        )


def _render_tiles(data: dict) -> str:
    tiles = [
        ("Total pages", data["total_pages"]),
        ("Successful", data["successful"]),
        ("Failed", data["failed"]),
        ("Pages/sec", data["pages_per_second"]),
        ("Elapsed, s", data["elapsed_seconds"]),
    ]
    return "".join(
        f'<div class="tile"><div class="value">{value}</div>'
        f'<div class="label">{label}</div></div>'
        for label, value in tiles
    )


def _render_bars(items: list[tuple[str, int]]) -> str:
    if not items:
        return ""
    peak = max(count for _, count in items) or 1
    rows = []
    for name, count in items:
        width = int(count / peak * 100)
        rows.append(
            f"<tr><td class='name'>{name}</td>"
            f"<td class='bar-cell'><div class='bar' style='width:{width}%'></div></td>"
            f"<td class='count'>{count}</td></tr>"
        )
    return "".join(rows)


_HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Crawler report</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a2e; background: #f5f6fa; }}
  h1 {{ margin-bottom: 0.25rem; }}
  .generated {{ color: #666; margin-bottom: 1.5rem; font-size: 0.9rem; }}
  .tiles {{ display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 2rem; }}
  .tile {{ background: #fff; border-radius: 10px; padding: 1rem 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.1); min-width: 110px; }}
  .tile .value {{ font-size: 1.8rem; font-weight: 700; }}
  .tile .label {{ color: #666; font-size: 0.85rem; }}
  h2 {{ margin-top: 2rem; }}
  table {{ width: 100%; max-width: 640px; border-collapse: collapse; background: #fff; border-radius: 10px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  td {{ padding: 0.5rem 0.75rem; border-bottom: 1px solid #eee; }}
  .name {{ font-family: monospace; width: 30%; }}
  .count {{ text-align: right; width: 60px; font-variant-numeric: tabular-nums; }}
  .bar-cell {{ width: 100%; }}
  .bar {{ height: 14px; background: linear-gradient(90deg, #5b6ef5, #8b5cf6); border-radius: 4px; }}
</style>
</head>
<body>
<h1>Crawler report</h1>
<div class="generated">Generated: {generated}</div>
<div class="tiles">{tiles}</div>
<h2>Status codes</h2>
<table>{status_rows}</table>
<h2>Top domains</h2>
<table>{domain_rows}</table>
</body>
</html>
"""
