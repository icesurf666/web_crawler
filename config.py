import json
from dataclasses import dataclass, field, fields

import yaml


@dataclass
class Config:
    start_urls: list[str] = field(default_factory=list)
    sitemap_url: str | None = None
    max_pages: int = 100
    max_depth: int = 2
    max_concurrent: int = 10
    per_domain_limit: int = 2
    requests_per_second: float = 1.0
    respect_robots: bool = True
    user_agent: str = "MyBot/1.0"
    min_delay: float = 0.0
    jitter: float = 0.0
    same_domain_only: bool = False
    exclude_patterns: list[str] = field(default_factory=list)
    include_patterns: list[str] = field(default_factory=list)
    output_json: str | None = None
    output_csv: str | None = None
    postgres_dsn: str | None = None
    log_file: str | None = None
    log_level: str = "INFO"
    connect_timeout: float = 10.0
    read_timeout: float = 30.0
    total_timeout: float | None = None
    timeout_growth: float = 1.0
    max_retries: int = 3
    backoff_factor: float = 2.0
    circuit_breaker: bool = False
    circuit_failure_threshold: int = 5
    circuit_recovery_time: float = 30.0
    proxy: str | None = None
    cookies: dict | None = None
    allow_private_hosts: bool = False
    max_page_bytes: int = 5_000_000

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        known = {f.name for f in fields(cls)}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown config keys: {sorted(unknown)}")
        return cls(**data)

    @classmethod
    def from_file(cls, path: str) -> "Config":
        with open(path, encoding="utf-8") as handle:
            if path.endswith((".yaml", ".yml")):
                data = yaml.safe_load(handle)
            elif path.endswith(".json"):
                data = json.load(handle)
            else:
                raise ValueError(f"unsupported config format: {path}")
        return cls.from_dict(data or {})
