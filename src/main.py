"""Application entry point."""

from __future__ import annotations

import logging
import signal
import threading
from pathlib import Path

from .analytics import AnalyticsWriter
from .config import read_config
from .http_server import RateLimitHTTPServer
from .rate_limiter import RateLimiter, RedisRateStore
from .redis_client import RedisClient


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    config = read_config()
    redis = RedisClient(config.redis_host, config.redis_port, config.request_timeout_ms)
    limiter = RateLimiter(RedisRateStore(redis), config.window_ms)
    analytics = AnalyticsWriter(
        redis, config.analytics_stream, config.analytics_stream_max_len
    )
    public_dir = Path(__file__).resolve().parent.parent / "public"
    server = RateLimitHTTPServer(
        ("", config.port), config, limiter, analytics, redis, public_dir
    )

    def stop(signal_number: int, _frame: object) -> None:
        logging.info("Signal %s received; draining analytics queue.", signal_number)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logging.info("Rate limiter listening on %s", config.port)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        analytics.close()
        redis.close()


if __name__ == "__main__":
    main()
