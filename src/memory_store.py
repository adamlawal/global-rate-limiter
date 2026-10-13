"""A thread-safe in-memory rate store used only by tests."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


class MemoryRateStore:
    _SHARD_COUNT = 64

    def __init__(self, now_ms: Callable[[], int] | None = None) -> None:
        self.now_ms = now_ms or (lambda: time.monotonic_ns() // 1_000_000)
        self.windows: dict[tuple[str, int], int] = {}
        self._locks = [threading.Lock() for _ in range(self._SHARD_COUNT)]

    def consume(
        self, client_id: str, limit: int, window_ms: int
    ) -> dict[str, int | bool]:
        current_ms = self.now_ms()
        window_start = current_ms - (current_ms % window_ms)
        key = (client_id, window_start)
        lock = self._locks[hash(key) & (self._SHARD_COUNT - 1)]
        with lock:
            count = self.windows.get(key, 0) + 1
            self.windows[key] = count
        return {
            "allowed": count <= limit,
            "count": count,
            "window_start": window_start,
            "retry_after_ms": window_ms - (current_ms - window_start),
        }
