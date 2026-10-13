"""Integration checks against real Redis and independent application processes.

Run with: python3.11 -m unittest test.test_redis_integration -v
Requires redis-server on PATH. Each test starts an isolated local Redis instance.
"""

from __future__ import annotations

import concurrent.futures
import multiprocessing
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
import uuid

from src.rate_limiter import RateLimiter, RedisRateStore
from src.redis_client import RedisClient


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _start_redis(port: int, data_dir: str) -> subprocess.Popen[bytes]:
    process = subprocess.Popen(
        [
            "redis-server", "--bind", "127.0.0.1", "--port", str(port),
            "--save", "", "--appendonly", "no", "--dir", data_dir,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError("Redis exited before becoming ready")
        try:
            probe = RedisClient("127.0.0.1", port, timeout_ms=200)
            ready = probe.command("PING") == "PONG"
            probe.close()
            if ready:
                return process
        except (OSError, TimeoutError):
            pass
        time.sleep(0.02)
    process.terminate()
    raise RuntimeError("Redis did not become ready")


def _run_checks(port: int, client_id: str, count: int) -> tuple[int, int]:
    # This function is executed in a separate spawned application process.
    redis = RedisClient("127.0.0.1", port, timeout_ms=2000)
    limiter = RateLimiter(RedisRateStore(redis), window_ms=600_000)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            decisions = list(pool.map(lambda _: limiter.check(client_id, 100), range(count)))
        return (sum(bool(item["allowed"]) for item in decisions),
                sum(bool(item["degraded"]) for item in decisions))
    finally:
        redis.close()


@unittest.skipUnless(shutil.which("redis-server"), "redis-server is required")
class RedisIntegrationTests(unittest.TestCase):
    def test_six_processes_share_one_exact_redis_limit(self) -> None:
        port = _free_port()
        with tempfile.TemporaryDirectory() as data_dir:
            server = _start_redis(port, data_dir)
            try:
                client_id = "integration-" + uuid.uuid4().hex
                context = multiprocessing.get_context("spawn")
                with concurrent.futures.ProcessPoolExecutor(
                    max_workers=6, mp_context=context
                ) as pool:
                    results = list(pool.map(
                        _run_checks,
                        [port] * 6,
                        [client_id] * 6,
                        [100] * 6,
                    ))
                self.assertEqual(sum(allowed for allowed, _ in results), 100)
                self.assertEqual(sum(degraded for _, degraded in results), 0)
            finally:
                server.terminate()
                server.wait(timeout=5)

    def test_redis_outage_is_reported_and_client_recovers(self) -> None:
        port = _free_port()
        with tempfile.TemporaryDirectory() as data_dir:
            server = _start_redis(port, data_dir)
            redis = RedisClient("127.0.0.1", port, timeout_ms=200)
            limiter = RateLimiter(RedisRateStore(redis), window_ms=600_000)
            client_id = "recovery-" + uuid.uuid4().hex
            try:
                first = limiter.check(client_id, 100)
                self.assertTrue(first["allowed"])
                self.assertFalse(first["degraded"])

                server.terminate()
                server.wait(timeout=5)
                outage = limiter.check(client_id, 100)
                self.assertTrue(outage["allowed"])
                self.assertTrue(outage["degraded"])

                server = _start_redis(port, data_dir)
                recovered = limiter.check(client_id, 100)
                self.assertTrue(recovered["allowed"])
                self.assertFalse(recovered["degraded"])
            finally:
                redis.close()
                if server.poll() is None:
                    server.terminate()
                    server.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
