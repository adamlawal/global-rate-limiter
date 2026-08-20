from __future__ import annotations

import unittest

from src.memory_store import MemoryRateStore
from src.rate_limiter import RateLimiter


class RateLimiterTests(unittest.TestCase):
    def test_allows_exact_client_limit_then_returns_retry_time(self) -> None:
        now = 120_000
        limiter = RateLimiter(MemoryRateStore(lambda: now), 60_000)

        results = [limiter.check("client-a", 5) for _ in range(6)]

        self.assertEqual(sum(result["allowed"] for result in results), 5)
        self.assertFalse(results[5]["allowed"])
        self.assertEqual(results[5]["remaining"], 0)
        self.assertEqual(results[5]["retry_after_ms"], 60_000)

        now += 60_000
        self.assertTrue(limiter.check("client-a", 5)["allowed"])

    def test_fails_open_when_shared_store_times_out(self) -> None:
        class UnavailableStore:
            def consume(self, *_args: object) -> None:
                raise TimeoutError("Redis down")

        result = RateLimiter(UnavailableStore(), 60_000).check("client-a", 5)

        self.assertTrue(result["allowed"])
        self.assertTrue(result["degraded"])
        self.assertEqual(result["reason"], "rate-limit-store-unavailable")


if __name__ == "__main__":
    unittest.main()
