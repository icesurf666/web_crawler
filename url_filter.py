import re
from urllib.parse import urlsplit


class URLFilter:
    def __init__(
        self,
        base_url: str | list[str],
        same_domain_only: bool = False,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> None:
        base_urls = [base_url] if isinstance(base_url, str) else list(base_url)
        self.domains = {urlsplit(u).hostname for u in base_urls}
        self.same_domain_only = same_domain_only

        try:
            self._exclude = [re.compile(p) for p in (exclude_patterns or [])]
            self._include = [re.compile(p) for p in (include_patterns or [])]
        except re.error as error:
            raise ValueError(f"invalid URL filter pattern: {error}") from error

    def allows(self, url: str) -> bool:
        if self.same_domain_only and urlsplit(url).hostname not in self.domains:
            return False

        if any(pattern.search(url) for pattern in self._exclude):
            return False

        return not (
            self._include and not any(pattern.search(url) for pattern in self._include)
        )
