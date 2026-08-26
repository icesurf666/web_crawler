class CrawlerError(Exception):
    def __init__(
        self, message: str, *, url: str | None = None, status: int | None = None
    ):
        super().__init__(message)
        self.url = url
        self.status = status


class StorageError(CrawlerError):
    pass


class TransientError(CrawlerError):
    pass


class PermanentError(CrawlerError):
    pass


class NetworkError(CrawlerError):
    pass


class ParseError(CrawlerError):
    pass


TRANSIENT_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
PERMANENT_STATUSES = frozenset({400, 401, 403, 404, 410})


def classify_status(status: int, url: str | None = None) -> CrawlerError:
    if status in TRANSIENT_STATUSES:
        return TransientError(f"transient HTTP {status}", url=url, status=status)
    if status in PERMANENT_STATUSES:
        return PermanentError(f"permanent HTTP {status}", url=url, status=status)
    if 500 <= status < 600:
        return TransientError(f"transient HTTP {status}", url=url, status=status)
    return PermanentError(f"permanent HTTP {status}", url=url, status=status)
