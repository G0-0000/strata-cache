"""Shared test helpers: a real server subprocess and JSON-over-Unix clients.

The server runs as a subprocess so that SIGKILL and SIGTERM mean what they
mean, and so that the descriptor passing happens for real, between two
processes.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

#: Wire protocol this test suite speaks.
PROTOCOL = 1

SRC = Path(__file__).resolve().parents[1] / "src"
READY_TIMEOUT_S = 15.0


def manifest(root: Path) -> list[tuple[str, int, int, int]]:
    """Every entry below *root*: relative path, size, mtime_ns, mode."""
    rows: list[tuple[str, int, int, int]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        for name in sorted(dirnames) + sorted(filenames):
            path = Path(dirpath) / name
            info = path.lstat()
            rows.append(
                (str(path.relative_to(root)), info.st_size, info.st_mtime_ns, info.st_mode)
            )
    return sorted(rows)


def free_socket_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "sock"
    directory.mkdir(mode=0o700)
    os.chmod(directory, 0o700)
    return directory


def private_dir(parent: Path, name: str) -> Path:
    directory = parent / name
    directory.mkdir(mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)
    return directory


class Server:
    """A serve subprocess, plus the stderr file that explains a failure."""

    def __init__(self, socket_dir: Path, *args: str) -> None:
        self.socket_path = socket_dir / "cache.sock"
        self.stderr_path = socket_dir / "server.stderr"
        self._stderr = open(self.stderr_path, "w+b")
        env = dict(os.environ, PYTHONPATH=str(SRC))
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "strata_cache_plugin.cli",
                "serve",
                "--socket",
                str(self.socket_path),
                *args,
            ],
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=self._stderr,
        )

    def stderr_text(self) -> str:
        try:
            self._stderr.flush()
            return self.stderr_path.read_text(errors="replace")
        except OSError:
            return ""

    def wait_ready(self) -> str:
        """Block until hello answers; return that server's epoch."""
        deadline = time.monotonic() + READY_TIMEOUT_S
        last: Exception | None = None
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(
                    f"server exited with {self.proc.returncode}: {self.stderr_text()}"
                )
            try:
                with connect(self.socket_path) as conn:
                    reply = exchange(conn, {"id": "ready", "op": "hello", "protocol": PROTOCOL})
                if reply.get("ok") and reply["result"].get("protocol") == PROTOCOL:
                    return reply["result"]["epoch"]
            except (OSError, AssertionError) as exc:  # not up yet
                last = exc
            time.sleep(0.02)
        raise AssertionError(f"server never became ready ({last}): {self.stderr_text()}")

    def wait_listening(self, timeout: float = READY_TIMEOUT_S) -> None:
        """Block until the socket accepts a connection, whatever it then answers."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(
                    f"server exited with {self.proc.returncode}: {self.stderr_text()}"
                )
            try:
                with connect(self.socket_path):
                    return
            except OSError:
                time.sleep(0.02)
        raise AssertionError(f"socket never appeared: {self.stderr_text()}")

    def terminate(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - would be a real bug
            self.proc.kill()
            self.proc.wait(timeout=10)

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self._stderr.close()

    def __enter__(self) -> "Server":
        self.wait_ready()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def connect(path: Path, timeout: float = 10.0) -> socket.socket:
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.settimeout(timeout)
    conn.connect(str(path))
    return conn


def send(conn: socket.socket, payload: dict[str, Any]) -> None:
    conn.sendall(json.dumps(payload).encode("utf-8") + b"\n")


def read_line(conn: socket.socket) -> dict[str, Any]:
    buffer = b""
    while b"\n" not in buffer:
        chunk = conn.recv(65536)
        if not chunk:
            raise AssertionError("server closed the connection without a reply")
        buffer += chunk
    line, _, _ = buffer.partition(b"\n")
    return json.loads(line.decode("utf-8"))


def exchange(conn: socket.socket, payload: dict[str, Any]) -> dict[str, Any]:
    send(conn, payload)
    return read_line(conn)


class FdClient:
    """One connection that collects the descriptor a reply may carry.

    The reply and its descriptor arrive in one sendmsg, so a single recv_fds
    per chunk is enough: the descriptor is never separated from its line.
    """

    def __init__(self, path: Path) -> None:
        self.conn = connect(path)
        self._buffer = b""
        self._fds: list[int] = []
        self._sequence = 0
        self.epoch: str | None = None

    def __enter__(self) -> "FdClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        for fd in self._fds:
            try:
                os.close(fd)
            except OSError:
                pass
        self._fds.clear()
        self.conn.close()

    def hello(self) -> dict[str, Any]:
        """Handshake; keeps the epoch for later calls. Returns hello's result."""
        reply, fd = self.call("hello", protocol=PROTOCOL)
        assert fd is None, "hello must not carry a descriptor"
        assert reply["ok"] is True, reply
        self.epoch = reply["result"]["epoch"]
        return reply["result"]

    def call(self, op: str, **fields: Any) -> tuple[dict[str, Any], int | None]:
        payload: dict[str, Any] = {"id": f"r{self._sequence}", "op": op}
        self._sequence += 1
        if op != "hello" and self.epoch is not None:
            payload["epoch"] = self.epoch
        payload.update(fields)
        self.conn.sendall(json.dumps(payload).encode("utf-8") + b"\n")
        return self.read()

    def send_raw(self, raw: bytes) -> None:
        self.conn.sendall(raw)

    def read(self) -> tuple[dict[str, Any], int | None]:
        while b"\n" not in self._buffer:
            data, fds, _flags, _address = socket.recv_fds(self.conn, 65536, 1)
            if not data:
                raise AssertionError("server closed the connection without a reply")
            self._buffer += data
            if fds:
                self._fds.extend(fds)
        line, _, self._buffer = self._buffer.partition(b"\n")
        fd = self._fds.pop(0) if self._fds else None
        return json.loads(line.decode("utf-8")), fd
