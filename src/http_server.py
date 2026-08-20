"""HTTP API and static dashboard server."""

from __future__ import annotations

import json
import logging
import re
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .analytics import load_usage
from .config import Config, limit_for


JSON_HEADERS = {"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store"}
CLIENT_ID_RE = re.compile(r"^[a-zA-Z0-9._-]{1,128}$")
ALLOWED_BUCKETS = {60_000, 900_000, 3_600_000, 86_400_000}
CONTENT_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
    ".html": "text/html; charset=utf-8",
}


class RateLimitHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        config: Config,
        limiter: Any,
        analytics: Any,
        analytics_redis: Any,
        public_dir: Path,
    ) -> None:
        super().__init__(address, RateLimitRequestHandler)
        self.config = config
        self.limiter = limiter
        self.analytics = analytics
        self.analytics_redis = analytics_redis
        self.public_dir = public_dir.resolve()


class RateLimitRequestHandler(BaseHTTPRequestHandler):
    server: RateLimitHTTPServer

    def do_GET(self) -> None:  # noqa: N802 - HTTP method names are stdlib API.
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/health":
                self._send_json(HTTPStatus.OK, {"status": "ok", "rateLimitMode": "redis-with-fail-open"})
            elif parsed.path == "/api/usage":
                self._handle_usage(parse_qs(parsed.query, keep_blank_values=True))
            else:
                self._serve_static(parsed.path)
        except Exception:
            logging.exception("Request failed")
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal-server-error"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/v1/limit/check":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not-found"})
            return
        try:
            self._handle_rate_check()
        except ValueError as error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
        except Exception:
            logging.exception("Request failed")
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal-server-error"})

    def _handle_rate_check(self) -> None:
        body = self._read_json()
        client_id = self.headers.get("X-Client-Id") or body.get("clientId")
        if not isinstance(client_id, str) or not CLIENT_ID_RE.fullmatch(client_id):
            raise ValueError("x-client-id header (or clientId body) is required")

        endpoint = body.get("endpoint")
        if not isinstance(endpoint, str):
            endpoint = self.headers.get("X-Target-Api", "unspecified")
        endpoint = endpoint[:200]
        result = self.server.limiter.check(client_id, limit_for(client_id, self.server.config))

        headers = {
            "X-RateLimit-Limit": str(result["limit"]),
            "X-RateLimit-Remaining": str(result["remaining"] if result["remaining"] is not None else "unknown"),
            "X-RateLimit-Reset": str(int(result["retry_after_ms"])),
        }
        if not result["allowed"]:
            headers["Retry-After"] = str(max(1, -(-int(result["retry_after_ms"]) // 1_000)))
        if result["allowed"]:
            self.server.analytics.enqueue(
                {
                    "client_id": client_id,
                    "endpoint": endpoint,
                    "timestamp": int(time.time() * 1_000),
                    "decision_ms": result["decision_ms"],
                }
            )

        response = {
            "allowed": result["allowed"],
            "clientId": client_id,
            "limit": result["limit"],
            "remaining": result["remaining"],
            "retryAfterMs": result["retry_after_ms"],
            "degraded": result["degraded"],
        }
        if result["degraded"]:
            response["warning"] = "Rate-limit store unavailable; request allowed by fail-safe policy."
        self._send_json(HTTPStatus.OK if result["allowed"] else HTTPStatus.TOO_MANY_REQUESTS, response, headers)

    def _handle_usage(self, query: dict[str, list[str]]) -> None:
        bucket_ms = _query_int(query, "bucketMs")
        if bucket_ms not in ALLOWED_BUCKETS:
            bucket_ms = 3_600_000
        try:
            usage = load_usage(
                self.server.analytics_redis,
                self.server.config.analytics_stream,
                client_id=_query_value(query, "clientId"),
                from_ms=_query_int(query, "from"),
                to_ms=_query_int(query, "to"),
                bucket_ms=bucket_ms,
            )
        except Exception:
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "analytics-temporarily-unavailable"})
            return
        self._send_json(HTTPStatus.OK, usage)

    def _read_json(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            content_length = int(raw_length)
        except ValueError as error:
            raise ValueError("Invalid Content-Length") from error
        if content_length > 16_384:
            raise ValueError("Request body too large")
        if content_length <= 0:
            return {}
        raw = self.rfile.read(content_length)
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _serve_static(self, pathname: str) -> None:
        requested = "index.html" if pathname == "/" else pathname.lstrip("/")
        file_path = (self.server.public_dir / requested).resolve()
        try:
            file_path.relative_to(self.server.public_dir)
        except ValueError:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not-found"})
            return
        if not file_path.is_file():
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not-found"})
            return
        content = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", CONTENT_TYPES.get(file_path.suffix, "application/octet-stream"))
        self.send_header("Cache-Control", "public, max-age=300")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def _send_json(self, status: HTTPStatus, body: dict[str, Any], headers: dict[str, str] | None = None) -> None:
        content = json.dumps(body, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        for key, value in JSON_HEADERS.items():
            self.send_header(key, value)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format: str, *args: Any) -> None:
        logging.info("%s - %s", self.address_string(), format % args)


def _query_value(query: dict[str, list[str]], key: str) -> str | None:
    value = query.get(key, [None])[0]
    return value or None


def _query_int(query: dict[str, list[str]], key: str) -> int | None:
    value = _query_value(query, key)
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None
