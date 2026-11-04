"""Unix socket server: newline-delimited JSON, one request per line.

See PROTOCOL.md for the wire format. P1 serves every connection from a single
accept loop on purpose: the protocol is serial by design, and a hung client
blocks the queue until it is killed - a known, documented P1 limit.

Snapshot bytes never cross this socket. Requests name files; nothing is read
into the request or the reply.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import stat as stat_mod
import struct
import time
from typing import Any, Mapping

from . import PROTOCOL_VERSION, __version__
from . import policy, scan

DEFAULT_MAX_REQUEST_BYTES = 1 << 20  # 1 MiB of JSON per line
SOCKET_DIR_MODE = 0o700
SOCKET_FILE_MODE = 0o600
ACCEPT_TIMEOUT_S = 0.25
CLIENT_TIMEOUT_S = 0.25

ERROR_BAD_JSON = "bad_json"
ERROR_BAD_REQUEST = "bad_request"
ERROR_UNKNOWN_OP = "unknown_op"
ERROR_EPOCH_MISMATCH = "epoch_mismatch"
ERROR_PEER_REJECTED = "peer_rejected"
ERROR_REFUSED_BY_POLICY = "refused_by_policy"
ERROR_SCAN_FAILED = "scan_failed"
ERROR_POLICY_ERROR = "policy_error"
ERROR_NOT_IMPLEMENTED = "not_implemented"
ERROR_REQUEST_TOO_LARGE = "request_too_large"
ERROR_INTERNAL = "internal_error"

#: Operations that may be sent before an owner epoch has been presented.
UNGUARDED_OPS = frozenset({"hello"})


class ServerError(RuntimeError):
    """The server cannot start or cannot go on."""


class RefusedByPolicy(Exception):
    """The request would touch a protected root; P1 will not do it."""


class _Shutdown(Exception):
    """Raised from the SIGTERM/SIGINT handler to unwind the accept loop."""


class SocketServer:
    """P1 server: sequential accept loop, one JSON reply per JSON request line."""

    def __init__(
        self,
        socket_path: str | os.PathLike[str],
        *,
        allow_production_readonly: bool = False,
        allowed_uids: set[int] | None = None,
        max_request_bytes: int = DEFAULT_MAX_REQUEST_BYTES,
        scan_max_depth: int = 1,
        epoch: str | None = None,
    ) -> None:
        if max_request_bytes <= 0:
            raise ServerError("max_request_bytes must be > 0")
        if scan_max_depth < 1:
            raise ServerError("scan_max_depth must be >= 1")
        self.socket_path = os.path.abspath(os.fspath(socket_path))
        self.allow_production_readonly = allow_production_readonly
        self.allowed_uids = set(allowed_uids) if allowed_uids else {os.getuid()}
        self.max_request_bytes = max_request_bytes
        self.scan_max_depth = scan_max_depth
        #: Owner epoch: regenerated on every start, so a stale client cannot own
        #: this server's cache state after a restart.
        self.epoch = epoch or os.urandom(8).hex()

        self._listener: socket.socket | None = None
        self._owns_socket_file = False
        self._stopping = False
        self._started_at = time.monotonic()
        self._counters: dict[str, int] = {
            "connections_total": 0,
            "requests_total": 0,
            "errors_total": 0,
            "peer_rejections_total": 0,
        }
        self._requests_by_op: dict[str, int] = {}

    # ------------------------------------------------------------------ setup

    def start(self) -> None:
        """Create the socket directory, bind, listen. Raises ServerError."""
        if self._listener is not None:
            raise ServerError("server is already started")
        if not hasattr(socket, "SO_PEERCRED"):
            raise ServerError("SO_PEERCRED is unavailable on this platform; P1 refuses to run")
        self._prepare_directory()
        self._clear_stale_socket()
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(self.socket_path)
            self._owns_socket_file = True
            os.chmod(self.socket_path, SOCKET_FILE_MODE)
            listener.listen(16)
            listener.settimeout(ACCEPT_TIMEOUT_S)
        except OSError as exc:
            listener.close()
            raise ServerError(f"cannot listen on {self.socket_path}: {exc}") from exc
        self._listener = listener

    def run(self) -> None:
        """Start, serve until signalled or stopped, then clean up."""
        previous = self._install_signal_handlers()
        try:
            self.start()
            self.serve_forever()
        except _Shutdown:
            pass
        finally:
            self.close()
            self._restore_signal_handlers(previous)

    def serve_forever(self) -> None:
        if self._listener is None:
            raise ServerError("server is not started")
        while not self._stopping:
            try:
                connection, _ = self._listener.accept()
            except socket.timeout:
                continue
            except _Shutdown:
                return
            except OSError as exc:
                if self._stopping:
                    return
                raise ServerError(f"accept failed on {self.socket_path}: {exc}") from exc
            self._counters["connections_total"] += 1
            connection.settimeout(CLIENT_TIMEOUT_S)
            try:
                self._serve_connection(connection)
            except _Shutdown:
                raise
            except OSError:
                # A dying client is not a server failure.
                self._counters["errors_total"] += 1
            finally:
                try:
                    connection.close()
                except OSError:
                    pass

    def stop(self) -> None:
        """Ask the accept loop to return. Safe to call from a signal handler."""
        self._stopping = True

    def close(self) -> None:
        """Stop, close the listener and unlink our socket file."""
        self.stop()
        listener, self._listener = self._listener, None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        if not self._owns_socket_file:
            return
        self._owns_socket_file = False
        try:
            if stat_mod.S_ISSOCK(os.lstat(self.socket_path).st_mode):
                os.unlink(self.socket_path)
        except OSError:
            pass

    # ------------------------------------------------------------- internals

    def _prepare_directory(self) -> None:
        directory = os.path.dirname(self.socket_path) or "."
        if os.path.isdir(directory):
            mode = stat_mod.S_IMODE(os.stat(directory).st_mode)
            if mode & 0o077:
                raise ServerError(
                    f"socket directory {directory} is not private (mode {mode:04o});"
                    f" expected {SOCKET_DIR_MODE:04o}. Refusing to chmod a directory"
                    " this process did not create."
                )
            return
        try:
            os.makedirs(directory, mode=SOCKET_DIR_MODE, exist_ok=True)
        except OSError as exc:
            raise ServerError(f"cannot create socket directory {directory}: {exc}") from exc

    def _clear_stale_socket(self) -> None:
        """Unlink a dead socket file; refuse when a live server answers."""
        try:
            existing = os.lstat(self.socket_path)
        except FileNotFoundError:
            return
        if not stat_mod.S_ISSOCK(existing.st_mode):
            raise ServerError(
                f"refusing to replace {self.socket_path}: it exists and is not a socket"
            )
        if existing.st_uid != os.getuid():
            raise ServerError(
                f"refusing to replace {self.socket_path}: owned by uid {existing.st_uid}"
            )
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(1.0)
            probe.connect(self.socket_path)
        except OSError:
            os.unlink(self.socket_path)
            return
        finally:
            probe.close()
        raise ServerError(f"another server is already listening on {self.socket_path}")

    def _serve_connection(self, connection: socket.socket) -> None:
        _peer_pid, uid, _peer_gid = peer_credentials(connection)
        if uid not in self.allowed_uids:
            self._counters["peer_rejections_total"] += 1
            self._counters["errors_total"] += 1
            self._send(
                connection,
                _error_envelope(
                    None,
                    ERROR_PEER_REJECTED,
                    f"uid {uid} is not allowed to use this socket",
                ),
            )
            return

        buffer = b""
        while not self._stopping:
            try:
                chunk = connection.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                return
            if not chunk:
                # Client is gone. A half-written line is simply dropped.
                return
            buffer += chunk
            if b"\n" not in buffer and len(buffer) > self.max_request_bytes:
                self._counters["errors_total"] += 1
                self._send(
                    connection,
                    _error_envelope(
                        None,
                        ERROR_REQUEST_TOO_LARGE,
                        f"request line exceeds {self.max_request_bytes} bytes",
                    ),
                )
                return
            while b"\n" in buffer:
                raw, _, buffer = buffer.partition(b"\n")
                reply = self.handle_line(raw)
                if not self._send(connection, reply):
                    return

    def handle_line(self, raw: bytes) -> dict[str, Any]:
        """Turn one request line into one reply envelope. Never raises."""
        try:
            text = raw.decode("utf-8").strip()
        except UnicodeDecodeError:
            return self._error_reply(None, ERROR_BAD_JSON, "request is not valid UTF-8")
        if not text:
            return self._error_reply(None, ERROR_BAD_REQUEST, "empty request line")
        try:
            request = json.loads(text)
        except json.JSONDecodeError as exc:
            return self._error_reply(None, ERROR_BAD_JSON, f"cannot parse JSON: {exc.msg}")
        if not isinstance(request, Mapping):
            return self._error_reply(None, ERROR_BAD_REQUEST, "request must be a JSON object")
        return self.handle_request(request)

    def handle_request(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Dispatch one decoded request. Counters are updated here."""
        request_id = request.get("id")
        if not isinstance(request_id, str) or not request_id:
            return self._error_reply(None, ERROR_BAD_REQUEST, "id must be a non-empty string")
        op = request.get("op")
        if not isinstance(op, str) or not op:
            return self._error_reply(
                request_id, ERROR_BAD_REQUEST, "op must be a non-empty string"
            )

        self._counters["requests_total"] += 1
        self._requests_by_op[op] = self._requests_by_op.get(op, 0) + 1

        if op not in UNGUARDED_OPS:
            epoch = request.get("epoch")
            if not isinstance(epoch, str) or not epoch:
                return self._error_reply(
                    request_id, ERROR_EPOCH_MISMATCH, "epoch is required; send hello first"
                )
            if epoch != self.epoch:
                return self._error_reply(
                    request_id,
                    ERROR_EPOCH_MISMATCH,
                    "epoch does not belong to this server instance; send hello again",
                )

        try:
            if op == "hello":
                return self._ok(request_id, self._op_hello())
            if op == "scan":
                return self._ok(request_id, self._op_scan(request))
            if op == "stats":
                return self._ok(request_id, self._op_stats())
            if op == "dry_run":
                return self._ok(request_id, self._op_dry_run(request))
            if op == "lease_read":
                return self._error_reply(
                    request_id,
                    ERROR_NOT_IMPLEMENTED,
                    "lease_read is not used in P1: no snapshot bytes cross this socket",
                )
            if op == "release":
                return self._ok(
                    request_id,
                    {"released": False, "lease_id": request.get("lease_id"), "note": "no-op in P1"},
                )
            return self._error_reply(
                request_id, ERROR_UNKNOWN_OP, f"unknown op {op!r}"
            )
        except RefusedByPolicy as exc:
            return self._error_reply(request_id, ERROR_REFUSED_BY_POLICY, str(exc))
        except scan.ScanError as exc:
            return self._error_reply(request_id, ERROR_SCAN_FAILED, str(exc))
        except policy.PolicyError as exc:
            return self._error_reply(request_id, ERROR_POLICY_ERROR, str(exc))
        except Exception as exc:  # noqa: BLE001 - a bad request must not kill P1
            return self._error_reply(
                request_id, ERROR_INTERNAL, f"{type(exc).__name__}: {exc}"
            )

    # ---------------------------------------------------------- operations

    def _op_hello(self) -> dict[str, Any]:
        return {
            "protocol": PROTOCOL_VERSION,
            "version": __version__,
            "epoch": self.epoch,
            "pid": os.getpid(),
            "uid": os.getuid(),
            "socket": self.socket_path,
            "allow_production_readonly": self.allow_production_readonly,
            "capabilities": ["scan", "stats", "dry_run", "release"],
            "not_used_in_p1": ["lease_read"],
        }

    def _op_scan(self, request: Mapping[str, Any]) -> dict[str, Any]:
        directory = request.get("dir")
        if not isinstance(directory, str) or not directory:
            raise scan.ScanError("dir is required and must be a non-empty string")
        if scan.is_protected_path(directory) and not self.allow_production_readonly:
            raise RefusedByPolicy(
                f"{scan.lexical_path(directory)} is a protected root;"
                " start the server with --allow-production-readonly to stat it"
            )
        max_depth = request.get("max_depth", self.scan_max_depth)
        if isinstance(max_depth, bool) or not isinstance(max_depth, int):
            raise scan.ScanError("max_depth must be an integer")
        return scan.scan_directory(directory, max_depth=max_depth).to_dict()

    def _op_stats(self) -> dict[str, Any]:
        return {
            "protocol": PROTOCOL_VERSION,
            "version": __version__,
            "epoch": self.epoch,
            "pid": os.getpid(),
            "socket": self.socket_path,
            "started_at": self._started_at,
            "uptime_s": round(time.monotonic() - self._started_at, 3),
            "connections_total": self._counters["connections_total"],
            "requests_total": self._counters["requests_total"],
            "errors_total": self._counters["errors_total"],
            "peer_rejections_total": self._counters["peer_rejections_total"],
            "requests_by_op": dict(self._requests_by_op),
            "allow_production_readonly": self.allow_production_readonly,
        }

    def _op_dry_run(self, request: Mapping[str, Any]) -> dict[str, Any]:
        entries = request.get("entries")
        if entries is None:
            raise policy.PolicyError(
                "entries is required: P1 sends descriptors inline and never reads client files"
            )
        mode = request.get("mode", policy.MODE_LRU)
        if mode not in policy.MODES:
            raise policy.PolicyError(f"mode must be one of {list(policy.MODES)}")
        budget_bytes = request.get("budget_bytes")
        if budget_bytes is None:
            raise policy.PolicyError("budget_bytes is required")
        decision = policy.evaluate_fixture(
            entries,
            mode=mode,
            budget_bytes=budget_bytes,
            slots=request.get("slots", policy.DEFAULT_SLOT_CAP),
            bytes_per_token=request.get(
                "bytes_per_token", policy.DEFAULT_BYTES_PER_TOKEN
            ),
        )
        return decision.to_dict()

    # ------------------------------------------------------------- plumbing

    def _ok(self, request_id: str | None, result: Any) -> dict[str, Any]:
        return {"id": request_id, "ok": True, "result": result}

    def _error_reply(self, request_id: str | None, code: str, message: str) -> dict[str, Any]:
        self._counters["errors_total"] += 1
        return _error_envelope(request_id, code, message)

    def _send(self, connection: socket.socket, envelope: Mapping[str, Any]) -> bool:
        payload = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")) + "\n"
        try:
            connection.sendall(payload.encode("utf-8"))
        except OSError:
            return False
        return True

    def _install_signal_handlers(self) -> dict[int, Any]:
        previous: dict[int, Any] = {}

        def handler(signum: int, frame: Any) -> None:  # noqa: ARG001 - signal API
            self.stop()
            raise _Shutdown()

        for signum in (signal.SIGTERM, signal.SIGINT):
            try:
                previous[signum] = signal.signal(signum, handler)
            except (ValueError, OSError):  # not the main thread
                pass
        return previous

    def _restore_signal_handlers(self, previous: Mapping[int, Any]) -> None:
        for signum, handler in previous.items():
            try:
                signal.signal(signum, handler)
            except (ValueError, OSError):
                pass


def peer_credentials(connection: socket.socket) -> tuple[int, int, int]:
    """(pid, uid, gid) of the connected peer, straight from SO_PEERCRED."""
    raw = connection.getsockopt(
        socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
    )
    pid, uid, gid = struct.unpack("3i", raw)
    return pid, uid, gid


def _error_envelope(request_id: str | None, code: str, message: str) -> dict[str, Any]:
    return {"id": request_id, "ok": False, "error": {"code": code, "message": message}}
