import re
from urllib.parse import urlsplit


class URLFilter:
    def __init__(
        self,
        base_url: str,
        same_domain_only: bool = False,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> None:
        self.domain = urlsplit(base_url).hostname
        self.same_domain_only = same_domain_only

        self._exclude = [re.compile(p) for p in (exclude_patterns or [])]
        self._include = [re.compile(p) for p in (include_patterns or [])]

    def allows(self, url: str) -> bool:
        if self.same_domain_only and urlsplit(url).hostname != self.domain:
            return False

        if any(pattern.search(url) for pattern in self._exclude):
            return False

        return not (
            self._include and not any(pattern.search(url) for pattern in self._include)
        )
