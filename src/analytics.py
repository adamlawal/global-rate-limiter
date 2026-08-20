"""Non-blocking approval event logging and dashboard usage aggregation."""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any


class AnalyticsWriter:
    def __init__(
        self,
        redis: Any,
        stream: str,
        max_length: int,
        flush_interval_ms: int = 250,
        max_queued: int = 10_000,
    ) -> None:
        self.redis = redis
        self.stream = stream
        self.max_length = max_length
        self.max_queued = max_queued
        self.events: deque[dict[str, Any]] = deque()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._flush_interval_seconds = flush_interval_ms / 1_000
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def enqueue(self, event: dict[str, Any]) -> None:
        with self._lock:
            if len(self.events) < self.max_queued:
                self.events.append(event)

    def flush(self) -> None:
        with self._lock:
            batch = [self.events.popleft() for _ in range(min(500, len(self.events)))]
        if not batch:
            return

        try:
            for event in batch:
                self.redis.command(
                    "XADD",
                    self.stream,
                    "MAXLEN",
                    "~",
                    self.max_length,
                    "*",
                    "clientId",
                    event["client_id"],
                    "endpoint",
                    event["endpoint"],
                    "timestamp",
                    event["timestamp"],
                    "decisionMs",
                    event["decision_ms"],
                )
        except Exception:
            with self._lock:
                for event in reversed(batch):
                    self.events.appendleft(event)
                while len(self.events) > self.max_queued:
                    self.events.pop()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1)
        self.flush()

    def _run(self) -> None:
        while not self._stop.wait(self._flush_interval_seconds):
            self.flush()


def load_usage(
    redis: Any,
    stream: str,
    *,
    client_id: str | None,
    from_ms: int | None,
    to_ms: int | None,
    bucket_ms: int = 60 * 60 * 1_000,
) -> dict[str, Any]:
    rows = redis.command("XRANGE", stream, "-", "+", "COUNT", 10_000) or []
    start = from_ms or int(time.time() * 1_000) - 24 * 60 * 60 * 1_000
    end = to_ms or int(time.time() * 1_000)
    buckets: dict[int, int] = {}
    total = 0
    total_decision_ms = 0.0

    for row in rows:
        fields = dict(zip(row[1][::2], row[1][1::2]))
        timestamp = int(fields["timestamp"])
        if (client_id and fields.get("clientId") != client_id) or not start <= timestamp <= end:
            continue
        total += 1
        total_decision_ms += float(fields.get("decisionMs", 0))
        bucket_start = timestamp - (timestamp % bucket_ms)
        buckets[bucket_start] = buckets.get(bucket_start, 0) + 1

    return {
        "filters": {"clientId": client_id, "from": start, "to": end, "bucketMs": bucket_ms},
        "approvedRequests": total,
        "averageDecisionMs": round(total_decision_ms / total, 3) if total else 0,
        "series": [
            {"timestamp": timestamp, "requests": requests}
            for timestamp, requests in sorted(buckets.items())
        ],
    }
