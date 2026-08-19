import heapq
import itertools


class CrawlerQueue:
    def __init__(self) -> None:
        self._heap: list[tuple[int, int, str]] = []
        self._counter = itertools.count()
        self.seen: set[str] = set()
        self.processed: set[str] = set()
        self.failed: dict[str, str] = {}

    def add_url(self, url: str, priority: int = 0) -> None:
        if url in self.seen:
            return

        self.seen.add(url)

        heapq.heappush(self._heap, (-priority, next(self._counter), url))

    async def get_next(self) -> str | None:
        if not self._heap:
            return None

        _priority, _order, url = heapq.heappop(self._heap)
        return url

    def mark_processed(self, url: str) -> None:
        self.processed.add(url)

    def mark_failed(self, url: str, error: str) -> None:
        self.failed[url] = error

    def get_stats(self) -> dict:
        return {
            "queued": len(self._heap),
            "seen": len(self.seen),
            "processed": len(self.processed),
            "failed": len(self.failed),
        }
