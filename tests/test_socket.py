"""Socket tests: protocol shape, serial handling, disconnect and kill/restart.

The server runs as a real subprocess so that SIGKILL and SIGTERM mean what they
mean. Every test uses a private socket directory under ``tmp_path``.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from strata_cache_plugin import cli, socket_server

SRC = Path(__file__).resolve().parents[1] / "src"
READY_TIMEOUT_S = 15.0
QUIET_TIMEOUT_S = 0.75


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


class Server:
    """A ``serve`` subprocess, plus the stderr file that explains a failure."""

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
                    reply = exchange(conn, {"id": "ready", "op": "hello"})
                if reply.get("ok") and reply["result"].get("protocol") == "v0":
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


def send(conn: socket.socket, payload: dict[str, object]) -> None:
    conn.sendall(json.dumps(payload).encode("utf-8") + b"\n")


def read_line(conn: socket.socket) -> dict[str, object]:
    buffer = b""
    while b"\n" not in buffer:
        chunk = conn.recv(65536)
        if not chunk:
            raise AssertionError("server closed the connection without a reply")
        buffer += chunk
    line, _, _ = buffer.partition(b"\n")
    return json.loads(line.decode("utf-8"))


def exchange(conn: socket.socket, payload: dict[str, object]) -> dict[str, object]:
    send(conn, payload)
    return read_line(conn)


@pytest.fixture()
def server(tmp_path: Path) -> Server:
    with Server(free_socket_dir(tmp_path)) as running:
        yield running


def test_hello_scan_and_stats(server: Server, tmp_path: Path) -> None:
    epoch = server.wait_ready()
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "a.sess").write_bytes(b"x" * 7)
    (cache_dir / "a.meta").write_bytes(b"y" * 11)
    (cache_dir / ".leaf.00ff.tmp").write_bytes(b"z" * 3)
    (cache_dir / "stray.txt").write_bytes(b"s")

    with connect(server.socket_path) as conn:
        hello = exchange(conn, {"id": "1", "op": "hello"})
        assert hello["ok"] is True
        assert hello["result"]["epoch"] == epoch
        assert hello["result"]["protocol"] == "v0"
        assert "lease_read" in hello["result"]["not_used_in_p1"]
        assert "scan" in hello["result"]["capabilities"]

        reply = exchange(
            conn, {"id": "2", "op": "scan", "epoch": epoch, "dir": str(cache_dir)}
        )
        assert reply["ok"] is True, reply
        report = reply["result"]
        assert report["root"] == str(cache_dir)
        assert report["entry_count"] == 4
        assert report["temporary_count"] == 1
        assert report["unknown_count"] == 1
        assert report["bytes_by_suffix"][".sess"] == 7
        assert report["bytes_by_suffix"][".meta"] == 11

        stats = exchange(conn, {"id": "3", "op": "stats", "epoch": epoch})
        assert stats["ok"] is True
        assert stats["result"]["requests_total"] >= 3
        assert stats["result"]["epoch"] == epoch


def test_epoch_is_required_and_must_match(server: Server) -> None:
    epoch = server.wait_ready()
    with connect(server.socket_path) as conn:
        missing = exchange(conn, {"id": "1", "op": "stats"})
        assert missing["ok"] is False
        assert missing["error"]["code"] == "epoch_mismatch"

        stale = exchange(conn, {"id": "2", "op": "stats", "epoch": "deadbeefdeadbeef"})
        assert stale["ok"] is False
        assert stale["error"]["code"] == "epoch_mismatch"

        good = exchange(conn, {"id": "3", "op": "stats", "epoch": epoch})
        assert good["ok"] is True


def test_bad_requests_do_not_kill_the_server(server: Server) -> None:
    epoch = server.wait_ready()
    with connect(server.socket_path) as conn:
        conn.sendall(b"not json\n")
        broken = read_line(conn)
        assert broken["ok"] is False
        assert broken["error"]["code"] == "bad_json"
        assert broken["id"] is None

        no_id = exchange(conn, {"op": "stats", "epoch": epoch})
        assert no_id["error"]["code"] == "bad_request"

        unknown = exchange(conn, {"id": "u", "op": "nope", "epoch": epoch})
        assert unknown["error"]["code"] == "unknown_op"

        lease = exchange(conn, {"id": "l", "op": "lease_read", "epoch": epoch})
        assert lease["error"]["code"] == "not_implemented"

        release = exchange(conn, {"id": "r", "op": "release", "epoch": epoch,
                                  "lease_id": "L-1"})
        assert release["ok"] is True
        assert release["result"]["released"] is False

        still_alive = exchange(conn, {"id": "a", "op": "stats", "epoch": epoch})
        assert still_alive["ok"] is True


def test_dry_run_over_the_socket(server: Server) -> None:
    epoch = server.wait_ready()
    with connect(server.socket_path) as conn:
        reply = exchange(
            conn,
            {
                "id": "d",
                "op": "dry_run",
                "epoch": epoch,
                "mode": "lru",
                "budget_bytes": 1000,
                "bytes_per_token": 100,
                "entries": [
                    {"name": "old", "tokens": 10, "last_used": 0},
                    {"name": "new", "tokens": 10, "last_used": 5},
                ],
            },
        )
        assert reply["ok"] is True, reply
        assert reply["result"]["victims"] == ["old"]
        assert reply["result"]["kept"] == ["new"]

        missing_budget = exchange(
            conn,
            {"id": "d2", "op": "dry_run", "epoch": epoch, "entries": []},
        )
        assert missing_budget["error"]["code"] == "policy_error"


def test_protected_root_is_refused_by_default(server: Server) -> None:
    epoch = server.wait_ready()
    with connect(server.socket_path) as conn:
        reply = exchange(conn, {"id": "p", "op": "scan", "epoch": epoch, "dir": "/models"})
        assert reply["ok"] is False
        assert reply["error"]["code"] == "refused_by_policy"
        assert "/models" in reply["error"]["message"]

        no_dir = exchange(conn, {"id": "n", "op": "scan", "epoch": epoch})
        assert no_dir["error"]["code"] == "scan_failed"


def test_second_connection_is_served_only_after_the_first_finishes(server: Server) -> None:
    epoch = server.wait_ready()
    idle = connect(server.socket_path)  # connects, never sends a newline
    try:
        blocked = connect(server.socket_path)
        blocked.settimeout(QUIET_TIMEOUT_S)
        send(blocked, {"id": "b", "op": "hello"})
        with pytest.raises(socket.timeout):
            blocked.recv(65536)

        idle.close()  # now the queue clears
        blocked.settimeout(10.0)
        reply = read_line(blocked)
        assert reply["ok"] is True
        assert reply["result"]["epoch"] == epoch
        blocked.close()
    finally:
        idle.close()


def test_client_disconnect_mid_line_does_not_kill_the_server(server: Server) -> None:
    epoch = server.wait_ready()
    partial = connect(server.socket_path)
    partial.sendall(b'{"id":"x","op":"he')
    partial.close()

    with connect(server.socket_path) as conn:
        reply = exchange(conn, {"id": "1", "op": "hello"})
        assert reply["ok"] is True
        assert reply["result"]["epoch"] == epoch


def test_oversized_line_is_rejected_then_connection_dropped(tmp_path: Path) -> None:
    with Server(free_socket_dir(tmp_path), "--max-request-bytes", "64") as running:
        with connect(running.socket_path) as conn:
            conn.sendall(b"{" + b"a" * 200)
            reply = read_line(conn)
            assert reply["ok"] is False
            assert reply["error"]["code"] == "request_too_large"
        # The server is still alive for the next client.
        with connect(running.socket_path) as conn:
            assert exchange(conn, {"id": "1", "op": "hello"})["ok"] is True


def test_socket_directory_and_file_modes(tmp_path: Path) -> None:
    socket_dir = free_socket_dir(tmp_path)
    with Server(socket_dir) as running:
        assert stat.S_IMODE(os.stat(socket_dir).st_mode) == 0o700
        assert stat.S_IMODE(os.lstat(running.socket_path).st_mode) == 0o600


def test_world_writable_socket_dir_is_refused(tmp_path: Path) -> None:
    socket_dir = tmp_path / "open-sock"
    socket_dir.mkdir()
    os.chmod(socket_dir, 0o777)
    running = Server(socket_dir)
    try:
        running.proc.wait(timeout=READY_TIMEOUT_S)
        assert running.proc.returncode == 1
        assert "not private" in running.stderr_text()
        assert stat.S_IMODE(os.stat(socket_dir).st_mode) == 0o777  # untouched
    finally:
        running.close()


def test_sigkill_then_restart_on_the_same_socket(tmp_path: Path) -> None:
    socket_dir = free_socket_dir(tmp_path)
    first = Server(socket_dir)
    try:
        epoch_before = first.wait_ready()
        first.proc.kill()
        first.proc.wait(timeout=10)
        assert first.proc.returncode == -signal.SIGKILL
        # A killed server cannot unlink its socket: the file is left behind.
        assert stat.S_ISSOCK(os.lstat(first.socket_path).st_mode)
    finally:
        first.close()

    second = Server(socket_dir)
    try:
        epoch_after = second.wait_ready()
        assert epoch_after != epoch_before
        with connect(second.socket_path) as conn:
            assert exchange(conn, {"id": "1", "op": "hello"})["result"]["epoch"] == epoch_after
    finally:
        second.close()


def test_sigterm_with_a_hung_client_exits_cleanly(tmp_path: Path) -> None:
    socket_dir = free_socket_dir(tmp_path)
    running = Server(socket_dir)
    try:
        running.wait_ready()
        hung = connect(running.socket_path)  # never sends a newline
        try:
            # Serial by design: nobody else is served while this client holds the slot.
            blocked = connect(running.socket_path)
            blocked.settimeout(QUIET_TIMEOUT_S)
            send(blocked, {"id": "b", "op": "hello"})
            with pytest.raises(socket.timeout):
                blocked.recv(65536)
            blocked.close()

            running.proc.send_signal(signal.SIGTERM)
            running.proc.wait(timeout=10)
            assert running.proc.returncode == 0
            assert not running.socket_path.exists()
        finally:
            hung.close()
    finally:
        running.close()


def test_deeply_nested_json_does_not_kill_the_server(server: Server) -> None:
    epoch = server.wait_ready()
    with connect(server.socket_path) as conn:
        conn.sendall(b"[" * 2000 + b"]" * 2000 + b"\n")
        reply = read_line(conn)
        assert reply["ok"] is False
        # 2000 levels still parse on CPython 3.12, so this is "not an object";
        # a build that recurses earlier answers bad_json. Either way: an error.
        assert reply["error"]["code"] in {"bad_json", "bad_request"}
        assert reply["id"] is None

        after = exchange(conn, {"id": "1", "op": "hello"})
        assert after["ok"] is True
        assert after["result"]["epoch"] == epoch


def test_nesting_past_the_interpreter_limit_is_bad_json(server: Server) -> None:
    epoch = server.wait_ready()
    with connect(server.socket_path) as conn:
        # 20000 levels raise RecursionError inside json.loads.
        conn.sendall(b"[" * 20000 + b"]" * 20000 + b"\n")
        reply = read_line(conn)
        assert reply["ok"] is False
        assert reply["error"]["code"] == "bad_json"
        assert "RecursionError" in reply["error"]["message"]

        after = exchange(conn, {"id": "1", "op": "hello"})
        assert after["ok"] is True
        assert after["result"]["epoch"] == epoch


def test_integer_literal_past_the_conversion_limit_is_bad_json(server: Server) -> None:
    epoch = server.wait_ready()
    line = b'{"id":"big","n":' + b"1" * 5000 + b"}"
    with connect(server.socket_path) as conn:
        conn.sendall(line + b"\n")
        reply = read_line(conn)
        assert reply["ok"] is False
        assert reply["error"]["code"] == "bad_json"
        assert reply["id"] is None

        after = exchange(conn, {"id": "1", "op": "hello"})
        assert after["ok"] is True
        assert after["result"]["epoch"] == epoch


def test_oversized_line_with_newline_in_the_same_chunk(tmp_path: Path) -> None:
    """The cap must hold even when the newline arrives with the overflow."""
    with Server(free_socket_dir(tmp_path), "--max-request-bytes", "64") as running:
        with connect(running.socket_path) as conn:
            conn.sendall(b'{"id":"x","op":"hello","pad":"')  # 28 bytes, no newline
            conn.sendall(b"a" * 100 + b'"}' + b"\n")  # newline lands with the overflow
            reply = read_line(conn)
            assert reply["ok"] is False
            assert reply["error"]["code"] == "request_too_large"

        with connect(running.socket_path) as conn:
            assert exchange(conn, {"id": "1", "op": "hello"})["ok"] is True


def test_peer_credentials_are_enforced(tmp_path: Path) -> None:
    """A uid that is not allowed gets one rejection, then nothing."""
    running = Server(free_socket_dir(tmp_path), "--allow-uid", "4242424")
    try:
        running.wait_listening()
        with connect(running.socket_path) as conn:
            reply = read_line(conn)
            assert reply["ok"] is False
            assert reply["error"]["code"] == "peer_rejected"
            assert reply["id"] is None
            assert conn.recv(65536) == b""  # disconnected after the single reply
    finally:
        running.close()


def test_socket_path_in_a_protected_root_is_refused(
    capsys: pytest.CaptureFixture[str],
) -> None:
    for path in (
        "/models/strata-cache-plugin/cache.sock",
        "/home/g0/strata-v100/run/cache.sock",
    ):
        server = socket_server.SocketServer(path)  # constructing touches nothing
        with pytest.raises(socket_server.ServerError, match="protected root"):
            server.start()
        assert server._listener is None
        assert cli.main(["serve", "--socket", path]) == cli.EXIT_ERROR
    assert "protected root" in capsys.readouterr().err


def test_scan_depth_above_the_server_cap_is_refused(server: Server, tmp_path: Path) -> None:
    epoch = server.wait_ready()
    with connect(server.socket_path) as conn:
        too_deep = exchange(
            conn,
            {"id": "d", "op": "scan", "epoch": epoch, "dir": str(tmp_path), "max_depth": 3},
        )
        assert too_deep["ok"] is False
        assert too_deep["error"]["code"] == "refused_by_policy"
        assert "cap" in too_deep["error"]["message"]

        at_cap = exchange(
            conn,
            {"id": "d2", "op": "scan", "epoch": epoch, "dir": str(tmp_path), "max_depth": 1},
        )
        assert at_cap["ok"] is True

        zero = exchange(
            conn,
            {"id": "d3", "op": "scan", "epoch": epoch, "dir": str(tmp_path), "max_depth": 0},
        )
        assert zero["error"]["code"] == "scan_failed"


def test_scanning_a_live_socket_directory_changes_nothing(server: Server) -> None:
    epoch = server.wait_ready()
    socket_dir = server.socket_path.parent
    before = manifest(socket_dir)
    with connect(server.socket_path) as conn:
        reply = exchange(
            conn, {"id": "s", "op": "scan", "epoch": epoch, "dir": str(socket_dir)}
        )
        assert reply["ok"] is True
        assert any(entry["name"] == "cache.sock" for entry in reply["result"]["entries"])
    assert manifest(socket_dir) == before


def test_live_server_is_not_replaced(tmp_path: Path) -> None:
    socket_dir = free_socket_dir(tmp_path)
    with Server(socket_dir) as running:
        second = Server(socket_dir)
        try:
            second.proc.wait(timeout=READY_TIMEOUT_S)
            assert second.proc.returncode == 1
            assert "already listening" in second.stderr_text()
        finally:
            second.close()
        # The first server is untouched and still answers.
        with connect(running.socket_path) as conn:
            assert exchange(conn, {"id": "1", "op": "hello"})["ok"] is True
