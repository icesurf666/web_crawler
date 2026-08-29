import sys
import time


class ProgressMonitor:
    def __init__(self, total: int | None = None, stream=sys.stderr) -> None:
        self.total = total
        self.stream = stream
        self.processed = 0
        self.active = 0
        self._start: float | None = None

    def start(self) -> None:
        self._start = time.monotonic()

    def update(self, processed: int, active: int = 0) -> None:
        self.processed = processed
        self.active = active
        self._render()

    def _render(self) -> None:
        elapsed = time.monotonic() - self._start if self._start else 0.0
        rate = self.processed / elapsed if elapsed > 0 else 0.0

        line = f"\r{self.processed}"
        if self.total:
            percent = self.processed / self.total * 100
            line += f"/{self.total} ({percent:.0f}%)"
        line += f" | {rate:5.1f} pages/sec | active {self.active}"
        if self.total and rate > 0:
            remaining = max(self.total - self.processed, 0) / rate
            line += f" | ETA {remaining:4.0f}s"

        self.stream.write(line.ljust(70))
        self.stream.flush()

    def finish(self) -> None:
        self.stream.write("\n")
        self.stream.flush()
