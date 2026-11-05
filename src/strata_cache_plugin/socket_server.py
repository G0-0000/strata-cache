"""Unix socket server: newline-delimited JSON, one request per line.

See PROTOCOL.md for the wire format (v1). Every connection is served from a
single accept loop on purpose: the protocol is serial by design, and a hung
client blocks the queue until it is killed - a known, documented limit.

A line without an "id" is a notification: lease, unlease, touch, set_pin or
remove, applied and never answered. A line with an "id" is a request and gets
exactly one reply. Both kinds share one connection and one serial loop, so the
engine can interleave a notification with its own reads without a second
socket.

Snapshot bytes never cross this socket. Requests name files and carry numbers;
the only thing that crosses besides JSON is one descriptor per reply, attached
with SCM_RIGHTS so the engine reads and writes the file itself.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import stat as stat_mod
import struct
import sys
import time
from dataclasses import dataclass
from typing import Any, Mapping

from . import PROTOCOL_VERSION, __version__
from . import policy, scan, store_v1

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

#: v1 store refusals, taken from the module that raises them.
ERROR_INVALID = store_v1.ERROR_INVALID
ERROR_MISS = store_v1.ERROR_MISS
ERROR_BUSY = store_v1.ERROR_BUSY
ERROR_IO = store_v1.ERROR_IO

#: Operations that may be sent before an owner epoch has been presented.
UNGUARDED_OPS = frozenset({"hello"})

#: Operations that need a store directory; without one they answer not_implemented.
STORE_OPS = frozenset(
    {
        "lease_read",
        "release",
        "stage_write",
        "stage_append",
        "commit_write",
        "abort_write",
        "touch",
        "set_pin",
        "remove",
        "stat",
        "list",
        "evict",
    }
)

#: What a line without an id may ask for. Nothing else is a notification.
NOTIFICATION_OPS = store_v1.NOTIFICATION_OPS

#: What this build serves, as announced by hello.
FEATURES: tuple[str, ...] = (
    "hello",
    "scan",
    "dry_run",
    "stats",
    "lease_read",
    "release",
    "stage_write",
    "stage_append",
    "commit_write",
    "abort_write",
    "touch",
    "set_pin",
    "remove",
    "stat",
    "list",
    "evict",
    "notify",
)


class ServerError(RuntimeError):
    """The server cannot start or cannot go on."""


class RefusedByPolicy(Exception):
    """The request would touch a protected root; P1 will not do it."""


class _Shutdown(Exception):
    """Raised from the SIGTERM/SIGINT handler to unwind the accept loop."""


@dataclass(slots=True)
class Reply:
    """One reply envelope, the descriptor it may carry, and whether to close."""

    envelope: dict[str, Any]
    fd: int | None = None
    close: bool = False


class SocketServer:
    """Sequential accept loop: one JSON reply per JSON request line, in order."""

    def __init__(
        self,
        socket_path: str | os.PathLike[str],
        *,
        allow_production_readonly: bool = False,
        allowed_uids: set[int] | None = None,
        max_request_bytes: int = DEFAULT_MAX_REQUEST_BYTES,
        scan_max_depth: int = 1,
        epoch: str | None = None,
        disk_dir: str | os.PathLike[str] | None = None,
        slot_dir: str | os.PathLike[str] | None = None,
        lease_ttl_s: float = store_v1.LEASE_TTL_S,
        grace_s: float = store_v1.GRACE_S,
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
        #: The directories this server owns, or None for a scan-only server.
        self.store: store_v1.Store | None
        if slot_dir is not None and disk_dir is None:
            raise ServerError("slot_dir needs a disk_dir: the store has no directory to write to")
        try:
            self.store = (
                store_v1.Store(
                    disk_dir, slot_dir=slot_dir, lease_ttl_s=lease_ttl_s, grace_s=grace_s
                )
                if disk_dir is not None
                else None
            )
        except ValueError as exc:
            raise ServerError(str(exc)) from exc
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
            "notifications_total": 0,
            "notifications_ignored": 0,
            "errors_total": 0,
            "peer_rejections_total": 0,
        }
        self._requests_by_op: dict[str, int] = {}
        self._notifications_by_op: dict[str, int] = {}
        #: op -> how many notifications of that op were not applied.
        self._ignored_by_op: dict[str, int] = {}
        #: The first ignored notification is written to stderr; the rest are counted.
        self._warned_ignored = False

    # ------------------------------------------------------------------ setup

    def start(self) -> None:
        """Create the socket directory, bind, listen. Raises ServerError."""
        if self._listener is not None:
            raise ServerError("server is already started")
        if not hasattr(socket, "SO_PEERCRED"):
            raise ServerError("SO_PEERCRED is unavailable on this platform; P1 refuses to run")
        if scan.is_protected_path(self.socket_path):
            raise ServerError(
                f"refusing to create a socket at {self.socket_path}:"
                " it lies inside a protected root"
            )
        if self.store is not None:
            try:
                self.store.prepare()
            except store_v1.StoreError as exc:
                raise ServerError(str(exc)) from exc
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
            while b"\n" in buffer:
                raw, _, buffer = buffer.partition(b"\n")
                if len(raw) > self.max_request_bytes:
                    # The newline arrived in the same chunk as the overflow, so the
                    # early check below cannot see it: enforce the cap here too.
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
                reply = self.handle_line(raw)
                if reply is None:
                    # A notification: applied, and never answered.
                    continue
                if not self._deliver(connection, reply) or reply.close:
                    return
            if len(buffer) > self.max_request_bytes:
                # A partial line with no newline yet is already too long.
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

    def handle_line(self, raw: bytes) -> Reply | None:
        """Turn one line into one reply, or into None when it is a notification.

        Never raises. A line that cannot be parsed at all is answered with an
        error, because there is no way to tell a broken request from a broken
        notification; a line that parses as an object without an "id" is a
        notification and is never answered.
        """
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
        except (ValueError, RecursionError) as exc:
            # ValueError: an integer literal over the 4300-digit conversion limit.
            # RecursionError: nesting deeper than the interpreter will parse.
            return self._error_reply(
                None, ERROR_BAD_JSON, f"cannot parse JSON: {type(exc).__name__}: {str(exc)[:200]}"
            )
        if not isinstance(request, Mapping):
            return self._error_reply(None, ERROR_BAD_REQUEST, "request must be a JSON object")
        if "id" not in request:
            self.handle_notification(request)
            return None
        return self.handle_request(request)

    def handle_notification(self, request: Mapping[str, Any]) -> None:
        """Apply one fire-and-forget notification. No reply, no epoch, never raises.

        The engine sends these with a single non-blocking send() and reads
        nothing back, so a notification that is malformed, duplicated, dropped
        on the way or sent to a server with no store is counted, never answered:
        the sender cannot be told, so the first one of the run goes to stderr and
        the rest only to stats. Nothing here may raise and nothing may write to
        the connection.
        """
        self._counters["notifications_total"] += 1
        op = request.get("op")
        if not isinstance(op, str) or not op:
            self._ignore_notification("(none)", "op must be a non-empty string")
            return
        self._notifications_by_op[op] = self._notifications_by_op.get(op, 0) + 1
        if self.store is None:
            # No store: there is no state to keep, and nobody to tell.
            self._ignore_notification(op, "this server has no store")
            return
        try:
            applied = self.store.notify(op, request.get("name"), request.get("on"))
        except Exception as exc:  # noqa: BLE001 - a notification is never worth a crash
            self._ignore_notification(op, f"the store raised {type(exc).__name__}: {exc}")
            return
        if not applied:
            self._ignore_notification(op, "the store refused the line")

    def _ignore_notification(self, op: str, why: str) -> None:
        """Count one notification that was not applied, and say so once for the run.

        A notification is never answered, so its sender cannot learn that the
        plugin dropped it: the first one of a run is written to stderr - the same
        thing the engine does with a line its socket would not take - and every one
        is counted, so an operator sees a pattern without the log filling up.
        """
        self._counters["notifications_ignored"] += 1
        self._ignored_by_op[op] = self._ignored_by_op.get(op, 0) + 1
        if self._warned_ignored:
            return
        self._warned_ignored = True
        print(
            f"strata-cache-plugin: ignored a notification (op {op}): {why};"
            " further ignored notifications are counted only",
            file=sys.stderr,
            flush=True,
        )

    def handle_request(self, request: Mapping[str, Any]) -> Reply:
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

        if op in STORE_OPS and self.store is None:
            return self._error_reply(
                request_id,
                ERROR_NOT_IMPLEMENTED,
                "this server was started without --disk-dir;"
                " the v1 store operations need a directory to hold artifacts",
            )

        try:
            if op == "hello":
                return self._reply_hello(request_id, request)
            if op == "scan":
                return self._ok(request_id, self._op_scan(request))
            if op == "stats":
                return self._ok(request_id, self._op_stats())
            if op == "dry_run":
                return self._ok(request_id, self._op_dry_run(request))
            if op == "lease_read":
                return self._op_lease_read(request_id, request)
            if op == "release":
                return self._op_release(request_id, request)
            if op == "stage_write":
                return self._op_stage_write(request_id, request)
            if op == "stage_append":
                return self._op_stage_append(request_id, request)
            if op == "commit_write":
                return self._op_commit_write(request_id, request)
            if op == "abort_write":
                return self._op_abort_write(request_id, request)
            if op == "touch":
                return self._op_touch(request_id, request)
            if op == "set_pin":
                return self._op_set_pin(request_id, request)
            if op == "remove":
                return self._op_remove(request_id, request)
            if op == "stat":
                return self._op_stat(request_id, request)
            if op == "list":
                return self._op_list(request_id, request)
            if op == "evict":
                return self._op_evict(request_id, request)
            return self._error_reply(
                request_id, ERROR_UNKNOWN_OP, f"unknown op {op!r}"
            )
        except store_v1.BusyArtifact as exc:
            return self._error_reply(request_id, ERROR_BUSY, str(exc))
        except store_v1.MissingArtifact as exc:
            return self._error_reply(request_id, ERROR_MISS, str(exc))
        except store_v1.InvalidRequest as exc:
            return self._error_reply(request_id, ERROR_INVALID, str(exc))
        except store_v1.StoreError as exc:
            return self._error_reply(request_id, ERROR_IO, str(exc))
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

    def _reply_hello(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        """Handshake. Anything but this server's protocol is refused, and the connection closed.

        A missing protocol is refused the same way as a foreign one: a client
        that does not say which protocol it speaks is a v0 client or a broken
        one, and answering bad_request would leave it on a connection whose
        later lines are not the ones it knows how to write.
        """
        protocol = request.get("protocol")
        if (
            isinstance(protocol, bool)
            or not isinstance(protocol, int)
            or protocol != PROTOCOL_VERSION
        ):
            return self._error_reply(
                request_id,
                ERROR_NOT_IMPLEMENTED,
                f"protocol {protocol!r} is not supported;"
                f" this server speaks protocol {PROTOCOL_VERSION}",
                close=True,
            )
        return self._ok(
            request_id,
            {
                "protocol": PROTOCOL_VERSION,
                "version": __version__,
                "epoch": self.epoch,
                "pid": os.getpid(),
                "uid": os.getuid(),
                "socket": self.socket_path,
                "disk_dir": self.store.disk_dir if self.store is not None else None,
                "slot_dir": self.store.slot_dir if self.store is not None else None,
                "grace_s": self.store.grace_s if self.store is not None else None,
                "allow_production_readonly": self.allow_production_readonly,
                "features": list(FEATURES),
                "notifications": list(NOTIFICATION_OPS),
            },
        )

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
        if max_depth < 1:
            raise scan.ScanError("max_depth must be >= 1")
        if max_depth > self.scan_max_depth:
            raise RefusedByPolicy(
                f"max_depth {max_depth} exceeds the server cap {self.scan_max_depth};"
                " the cap is a limit, not a default"
            )
        return scan.scan_directory(directory, max_depth=max_depth).to_dict()

    def _op_stats(self) -> dict[str, Any]:
        store_stats: dict[str, Any] = (
            self.store.stats()
            if self.store is not None
            else {
                "disk_dir": None,
                "slot_dir": None,
                "leases_live": 0,
                "notified_leases_live": 0,
                "writes_pending": 0,
                "removes_refused": 0,
            }
        )
        snapshot: dict[str, Any] = {
            "protocol": PROTOCOL_VERSION,
            "version": __version__,
            "epoch": self.epoch,
            "pid": os.getpid(),
            "socket": self.socket_path,
            "started_at": self._started_at,
            "uptime_s": round(time.monotonic() - self._started_at, 3),
            "connections_total": self._counters["connections_total"],
            "requests_total": self._counters["requests_total"],
            "notifications_total": self._counters["notifications_total"],
            "notifications_ignored": self._counters["notifications_ignored"],
            "errors_total": self._counters["errors_total"],
            "peer_rejections_total": self._counters["peer_rejections_total"],
            "requests_by_op": dict(self._requests_by_op),
            "notifications_by_op": dict(self._notifications_by_op),
            "notifications_ignored_by_op": dict(self._ignored_by_op),
            "allow_production_readonly": self.allow_production_readonly,
        }
        snapshot.update(store_stats)
        return snapshot

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

    # ------------------------------------------------------- v1 store operations

    def _op_lease_read(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        opened = self.store.lease(request.get("name"))
        reply = self._ok(
            request_id,
            {
                "name": opened.name,
                "bytes": opened.size,
                "lease": opened.lease_id,
                "fd": True,
            },
        )
        reply.fd = opened.fd
        return reply

    def _op_release(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        return self._ok(request_id, {"released": self.store.release(request.get("lease"))})

    def _op_stage_write(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        staged, aborted = self.store.stage_write(request.get("name"))
        reply = self._ok(
            request_id,
            {
                "name": staged.name,
                "token": staged.token,
                "aborted": aborted,
                "fd": True,
            },
        )
        reply.fd = staged.fd
        return reply

    def _op_stage_append(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        staged = self.store.stage_append(request.get("name"), request.get("committed"))
        reply = self._ok(
            request_id,
            {
                "name": staged.name,
                "token": staged.token,
                "committed": staged.committed,
                "fd": True,
            },
        )
        reply.fd = staged.fd
        return reply

    def _op_commit_write(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        published, committed = self.store.commit_write(
            request.get("name"), request.get("token"), request.get("bytes")
        )
        return self._ok(
            request_id,
            {
                "name": store_v1.validate_name(request.get("name")),
                "token": request.get("token"),
                "published": published,
                "committed": committed,
            },
        )

    def _op_abort_write(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        return self._ok(
            request_id,
            {"aborted": self.store.abort_write(request.get("name"), request.get("token"))},
        )

    def _op_touch(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        touched = self.store.touch(request.get("name"))
        return self._ok(
            request_id, {"name": store_v1.validate_name(request.get("name")), "touched": touched}
        )

    def _op_set_pin(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        pinned = self.store.set_pin(request.get("name"), request.get("on"))
        return self._ok(
            request_id, {"name": store_v1.validate_name(request.get("name")), "pinned": pinned}
        )

    def _op_remove(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        removed = self.store.remove(request.get("name"))
        return self._ok(
            request_id, {"name": store_v1.validate_name(request.get("name")), "removed": removed}
        )

    def _op_stat(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        exists, size = self.store.stat(request.get("name"))
        return self._ok(
            request_id,
            {"name": store_v1.validate_name(request.get("name")), "exists": exists, "bytes": size},
        )

    def _op_list(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        return self._ok(request_id, {"files": self.store.list_artifacts()})

    def _op_evict(self, request_id: str, request: Mapping[str, Any]) -> Reply:
        budget = request.get("budget_bytes")
        if budget is None:
            raise store_v1.InvalidRequest("budget_bytes is required")
        return self._ok(request_id, self.store.evict(budget))

    # ------------------------------------------------------------- plumbing

    def _ok(self, request_id: str | None, result: Any) -> Reply:
        return Reply({"id": request_id, "ok": True, "result": result})

    def _error_reply(
        self, request_id: str | None, code: str, message: str, *, close: bool = False
    ) -> Reply:
        self._counters["errors_total"] += 1
        return Reply(_error_envelope(request_id, code, message), close=close)

    def _deliver(self, connection: socket.socket, reply: Reply) -> bool:
        """Send one reply and close our copy of its descriptor, sent or not."""
        fd, reply.fd = reply.fd, None
        try:
            return self._send(connection, reply.envelope, fd)
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

    def _send(
        self,
        connection: socket.socket,
        envelope: Mapping[str, Any],
        fd: int | None = None,
    ) -> bool:
        payload = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")) + "\n"
        data = payload.encode("utf-8")
        try:
            if fd is None:
                connection.sendall(data)
                return True
            # One sendmsg carries the line and the descriptor, as the protocol asks.
            sent = socket.send_fds(connection, [data], [fd])
            if sent < len(data):
                connection.sendall(data[sent:])
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
