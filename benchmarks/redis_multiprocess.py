"""Local, reproducible real-Redis benchmark; not a production SLO.

Run: python3 -m benchmarks.redis_multiprocess --checks-per-process 1000
"""

from __future__ import annotations

import argparse
import concurrent.futures
import multiprocessing
import platform
import shutil
import socket
import subprocess
import tempfile
import time
import uuid

from src.rate_limiter import RateLimiter, RedisRateStore
from src.redis_client import RedisClient


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def worker(port: int, client_id: str, count: int, threads: int, limit: int):
    client = RedisClient("127.0.0.1", port, timeout_ms=2000)
    limiter = RateLimiter(RedisRateStore(client), window_ms=600_000)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            decisions = list(pool.map(lambda _: limiter.check(client_id, limit), range(count)))
        return (
            [float(decision["decision_ms"]) for decision in decisions],
            sum(bool(decision["allowed"]) for decision in decisions),
            sum(bool(decision["degraded"]) for decision in decisions),
        )
    finally:
        client.close()


def percentile(sorted_values: list[float], percent: float) -> float:
    index = min(len(sorted_values) - 1, int((len(sorted_values) - 1) * percent))
    return sorted_values[index]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processes", type=int, default=6)
    parser.add_argument("--checks-per-process", type=int, default=1000)
    parser.add_argument("--threads-per-process", type=int, default=20)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if min(args.processes, args.checks_per_process, args.threads_per_process, args.limit) <= 0:
        parser.error("all numeric options must be positive")
    if not shutil.which("redis-server"):
        parser.error("redis-server is required on PATH")
    port = free_port()
    with tempfile.TemporaryDirectory() as data_dir:
        server = subprocess.Popen(
            ["redis-server", "--bind", "127.0.0.1", "--port", str(port),
             "--save", "", "--appendonly", "no", "--dir", data_dir],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(100):
                try:
                    probe = RedisClient("127.0.0.1", port, timeout_ms=200)
                    if probe.command("PING") == "PONG":
                        probe.close()
                        break
                    probe.close()
                except (OSError, TimeoutError):
                    time.sleep(0.02)
            else:
                raise RuntimeError("Redis did not become ready")
            client_id = "benchmark-" + uuid.uuid4().hex
            context = multiprocessing.get_context("spawn")
            started = time.perf_counter()
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=args.processes, mp_context=context
            ) as pool:
                results = list(pool.map(
                    worker, [port] * args.processes, [client_id] * args.processes,
                    [args.checks_per_process] * args.processes,
                    [args.threads_per_process] * args.processes,
                    [args.limit] * args.processes,
                ))
            elapsed = time.perf_counter() - started
            latencies = sorted(value for values, _, _ in results for value in values)
            total = len(latencies)
            allowed = sum(item[1] for item in results)
            degraded = sum(item[2] for item in results)
            print(f"platform={platform.platform()} python={platform.python_version()}")
            print(f"processes={args.processes} threads/process={args.threads_per_process} "
                  f"checks={total} limit={args.limit} approved={allowed} degraded={degraded}")
            print(f"wall_seconds={elapsed:.3f} throughput_checks_per_s={total / elapsed:.1f}")
            print(f"client_decision_ms p50={percentile(latencies, 0.50):.3f} "
                  f"p95={percentile(latencies, 0.95):.3f} "
                  f"p99={percentile(latencies, 0.99):.3f}")
            if allowed != args.limit or degraded:
                raise RuntimeError("quota or Redis availability invariant failed")
        finally:
            server.terminate()
            server.wait(timeout=5)


if __name__ == "__main__":
    main()
