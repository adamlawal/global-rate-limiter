from __future__ import annotations

import statistics
import time
import unittest

from src.memory_store import MemoryRateStore
from src.rate_limiter import RateLimiter


class LoadPerformanceTests(unittest.TestCase):
    def test_decision_path_remains_below_a_few_milliseconds_in_process(self) -> None:
        limiter = RateLimiter(MemoryRateStore(lambda: 240_000), 60_000)
        durations = []
        for index in range(5_000):
            started = time.perf_counter()
            limiter.check(f"load-client-{index % 200}", 10_000)
            durations.append((time.perf_counter() - started) * 1_000)

        p95 = statistics.quantiles(durations, n=100)[94]
        self.assertLess(p95, 10, f"p95 should be below 10 ms, received {p95:.3f} ms")


if __name__ == "__main__":
    unittest.main()
