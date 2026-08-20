"""Configuration parsing for the rate limiter."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Mapping


DEFAULT_LIMITS = {"client-a": 100, "client-b": 5_000, "default": 60}


def positive_integer(value: object, fallback: int) -> int:
    """Return a positive integer or the supplied safe fallback."""
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return fallback
    return parsed if parsed > 0 else fallback


@dataclass(frozen=True)
class Config:
    port: int
    redis_host: str
    redis_port: int
    request_timeout_ms: int
    client_limits: dict[str, int]
    window_ms: int
    analytics_stream: str
    analytics_stream_max_len: int


def read_config(environment: Mapping[str, str] | None = None) -> Config:
    """Read environment variables, falling back to production-safe defaults."""
    environment = environment or os.environ
    client_limits = DEFAULT_LIMITS.copy()
    raw_limits = environment.get("CLIENT_LIMITS_JSON", "")

    if raw_limits:
        try:
            supplied_limits = json.loads(raw_limits)
            if not isinstance(supplied_limits, dict):
                raise ValueError("CLIENT_LIMITS_JSON must be an object")
            client_limits.update(supplied_limits)
        except (TypeError, ValueError, json.JSONDecodeError):
            logging.warning("CLIENT_LIMITS_JSON is invalid; using built-in limits.")

    for client_id, limit in client_limits.items():
        client_limits[client_id] = positive_integer(limit, DEFAULT_LIMITS["default"])

    return Config(
        port=positive_integer(environment.get("PORT"), 8080),
        redis_host=environment.get("REDIS_HOST", "127.0.0.1"),
        redis_port=positive_integer(environment.get("REDIS_PORT"), 6379),
        request_timeout_ms=positive_integer(environment.get("REDIS_TIMEOUT_MS"), 40),
        client_limits=client_limits,
        window_ms=60_000,
        analytics_stream=environment.get("ANALYTICS_STREAM", "rate-limiter:approvals"),
        analytics_stream_max_len=positive_integer(
            environment.get("ANALYTICS_STREAM_MAXLEN"), 100_000
        ),
    )


def limit_for(client_id: str, config: Config) -> int:
    return config.client_limits.get(client_id, config.client_limits["default"])
