"""A thread-safe in-memory rate store used only by tests."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


class MemoryRateStore:
    def __init__(self, now_ms: Callable[[], int] | None = None) -> None:
        self.now_ms = now_ms or (lambda: int(time.time() * 1_000))
        self.windows: dict[str, int] = {}
        self._lock = threading.Lock()

    def consume(self, client_id: str, limit: int, window_ms: int) -> dict[str, int | bool]:
        current_ms = self.now_ms()
        window_start = current_ms - (current_ms % window_ms)
        key = f"{client_id}:{window_start}"
        with self._lock:
            count = self.windows.get(key, 0) + 1
            self.windows[key] = count
        return {
            "allowed": count <= limit,
            "count": count,
            "window_start": window_start,
            "retry_after_ms": window_ms - (current_ms - window_start),
        }
