from __future__ import annotations

import concurrent.futures
import unittest

from src.memory_store import MemoryRateStore
from src.rate_limiter import RateLimiter


class RaceConditionTests(unittest.TestCase):
    def test_concurrent_checks_cannot_exceed_shared_limit(self) -> None:
        limiter = RateLimiter(MemoryRateStore(lambda: 180_000), 60_000)
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            results = list(
                executor.map(lambda _: limiter.check("race-client", 100), range(500))
            )

        self.assertEqual(sum(result["allowed"] for result in results), 100)
        self.assertEqual(sum(not result["allowed"] for result in results), 400)


if __name__ == "__main__":
    unittest.main()
