"""Fixed-window rate limiting with Redis-backed global correctness."""

from __future__ import annotations

import time
import threading
from typing import Any, Protocol

from .redis_client import RedisCommandError


FIXED_WINDOW_LUA = """
local redisTime = redis.call('TIME')
local nowMs = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
local windowMs = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local windowStart = nowMs - (nowMs % windowMs)
local key = KEYS[1] .. windowStart
local count = redis.call('INCR', key)
if count == 1 then
  redis.call('PEXPIRE', key, windowMs + 1000)
end
local allowed = 0
if count <= limit then allowed = 1 end
return { allowed, count, windowStart, windowMs - (nowMs - windowStart) }
"""


class RateStore(Protocol):
    def consume(self, client_id: str, limit: int, window_ms: int) -> dict[str, Any]: ...


class RedisRateStore:
    def __init__(self, redis: Any) -> None:
        self.redis = redis
        self._script_sha: str | None = None
        self._script_lock = threading.Lock()

    def _ensure_script_sha(self) -> str:
        existing = self._script_sha
        if existing is not None:
            return existing
        with self._script_lock:
            if self._script_sha is None:
                self._script_sha = str(self.redis.command("SCRIPT", "LOAD", FIXED_WINDOW_LUA))
            return self._script_sha

    def consume(self, client_id: str, limit: int, window_ms: int) -> dict[str, int | bool]:
        key_prefix = f"rate-limiter:window:{{{client_id}}}:"
        script_sha = self._ensure_script_sha()
        try:
            response = self.redis.command(
                "EVALSHA",
                script_sha,
                1,
                key_prefix,
                window_ms,
                limit,
            )
        except RedisCommandError as error:
            if "NOSCRIPT" not in str(error):
                raise
            with self._script_lock:
                self._script_sha = str(self.redis.command("SCRIPT", "LOAD", FIXED_WINDOW_LUA))
                script_sha = self._script_sha
            response = self.redis.command(
                "EVALSHA",
                script_sha,
                1,
                key_prefix,
                window_ms,
                limit,
            )
        allowed, count, window_start, retry_after_ms = response
        return {
            "allowed": bool(allowed),
            "count": int(count),
            "window_start": int(window_start),
            "retry_after_ms": int(retry_after_ms),
        }


class RateLimiter:
    def __init__(self, store: RateStore, window_ms: int, now: Any = time.time) -> None:
        self.store = store
        self.window_ms = window_ms
        self.now = now

    def check(self, client_id: str, limit: int) -> dict[str, Any]:
        started_at_ns = time.perf_counter_ns()
        try:
            decision = self.store.consume(client_id, limit, self.window_ms)
            count = int(decision["count"])
            decision_ms = (time.perf_counter_ns() - started_at_ns) / 1_000_000
            return {
                "allowed": bool(decision["allowed"]),
                "count": count,
                "window_start": int(decision["window_start"]),
                "retry_after_ms": int(decision["retry_after_ms"]),
                "limit": limit,
                "remaining": max(0, limit - count),
                "decision_ms": decision_ms,
                "degraded": False,
            }
        except Exception as error:  # Fail open when Redis is unavailable.
            decision_ms = (time.perf_counter_ns() - started_at_ns) / 1_000_000
            return {
                "allowed": True,
                "count": None,
                "limit": limit,
                "remaining": None,
                "retry_after_ms": 0,
                "decision_ms": decision_ms,
                "degraded": True,
                "reason": "rate-limit-store-unavailable",
                "error": error,
            }
