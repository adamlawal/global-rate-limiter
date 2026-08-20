"""Fixed-window rate limiting with Redis-backed global correctness."""

from __future__ import annotations

import time
from typing import Any, Protocol


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

    def consume(self, client_id: str, limit: int, window_ms: int) -> dict[str, int | bool]:
        response = self.redis.command(
            "EVAL",
            FIXED_WINDOW_LUA,
            1,
            f"rate-limiter:window:{{{client_id}}}:",
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
        started_at = time.perf_counter()
        try:
            decision = self.store.consume(client_id, limit, self.window_ms)
            return {
                **decision,
                "limit": limit,
                "remaining": max(0, limit - int(decision["count"])),
                "decision_ms": (time.perf_counter() - started_at) * 1_000,
                "degraded": False,
            }
        except Exception as error:  # Fail open when Redis is unavailable.
            return {
                "allowed": True,
                "count": None,
                "limit": limit,
                "remaining": None,
                "retry_after_ms": 0,
                "decision_ms": (time.perf_counter() - started_at) * 1_000,
                "degraded": True,
                "reason": "rate-limit-store-unavailable",
                "error": error,
            }
