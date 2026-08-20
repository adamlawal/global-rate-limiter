"""A small, dependency-free RESP2 client for Redis."""

from __future__ import annotations

import socket
import threading
from typing import Any


class RedisCommandError(RuntimeError):
    """A Redis server returned an error response."""


class RedisClient:
    """Serializes commands over one reconnecting TCP connection.

    Redis sends responses in request order, so a lock around write/read keeps a
    shared client correct when the threaded HTTP server handles many requests.
    """

    def __init__(self, host: str, port: int, timeout_ms: int = 40) -> None:
        self.host = host
        self.port = port
        self.timeout_seconds = timeout_ms / 1_000
        self._socket: socket.socket | None = None
        self._reader: Any = None
        self._lock = threading.Lock()

    def command(self, *parts: object) -> Any:
        payload = _encode_command(parts)
        with self._lock:
            try:
                self._connect()
                assert self._socket is not None
                self._socket.sendall(payload)
                return self._read_response()
            except (OSError, EOFError, TimeoutError):
                self._disconnect()
                raise

    def close(self) -> None:
        with self._lock:
            self._disconnect()

    def _connect(self) -> None:
        if self._socket is not None:
            return
        connection = socket.create_connection(
            (self.host, self.port), timeout=self.timeout_seconds
        )
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        connection.settimeout(self.timeout_seconds)
        self._socket = connection
        self._reader = connection.makefile("rb")

    def _disconnect(self) -> None:
        if self._reader is not None:
            try:
                self._reader.close()
            except OSError:
                pass
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
        self._reader = None
        self._socket = None

    def _read_response(self) -> Any:
        assert self._reader is not None
        prefix = self._reader.read(1)
        if not prefix:
            raise EOFError("Redis connection closed")
        line = self._read_line()

        if prefix == b"+":
            return line.decode("utf-8")
        if prefix == b"-":
            raise RedisCommandError(line.decode("utf-8"))
        if prefix == b":":
            return int(line)
        if prefix == b"$":
            length = int(line)
            if length == -1:
                return None
            value = self._reader.read(length)
            terminator = self._reader.read(2)
            if len(value) != length or terminator != b"\r\n":
                raise EOFError("Incomplete Redis bulk response")
            return value.decode("utf-8")
        if prefix == b"*":
            length = int(line)
            if length == -1:
                return None
            return [self._read_response() for _ in range(length)]
        raise RedisCommandError(f"Unsupported Redis RESP type: {prefix!r}")

    def _read_line(self) -> bytes:
        assert self._reader is not None
        line = self._reader.readline()
        if not line.endswith(b"\r\n"):
            raise EOFError("Incomplete Redis response")
        return line[:-2]


def _encode_command(parts: tuple[object, ...]) -> bytes:
    encoded_parts = [str(part).encode("utf-8") for part in parts]
    chunks = [f"*{len(encoded_parts)}\r\n".encode("ascii")]
    for part in encoded_parts:
        chunks.extend((f"${len(part)}\r\n".encode("ascii"), part, b"\r\n"))
    return b"".join(chunks)
