"""v1 store: leases, staged writes, index queries and the notification state.

The store holds what the engine writes and nothing else. It owns two
directories - the disk directory holding the spill artifacts, and an optional
slot directory holding session files - and it only ever lists, stats, removes
and tracks names in them; a write only ever lands in the disk directory. It
opens an artifact only to hand the engine a descriptor; no artifact byte is
read into this process and no artifact content is ever parsed.

Two paths delete an artifact: remove() answers an explicit request, and evict()
deletes on the store's own initiative. Eviction is bounded by the grace
period: an entry whose lease is unknown - which is exactly what a dropped
notification leaves behind - is never removed while its last_used, or its file
mtime when the plugin has no record, is younger than grace_s. In exchange a
dropped notification costs retention, never a live read. A staged temporary is
also dropped on our own initiative, but a temporary is never an artifact and is
never listed or served.

Every open uses O_NOFOLLOW and every name goes through validate_name().
See PROTOCOL.md for the operations and the name rule.
"""

from __future__ import annotations

import errno
import os
import secrets
import stat as stat_mod
import time
from dataclasses import dataclass
from typing import Any, Callable

from . import scan

#: Wire error codes. socket_server forwards these verbatim.
ERROR_INVALID = "invalid"
ERROR_MISS = "miss"
ERROR_BUSY = "busy"
ERROR_IO = "io"

#: A name is a flat basename of at most this many UTF-8 bytes.
MAX_NAME_BYTES = 200

#: Lease lifetime in seconds; refreshed by lease_read on the same name and by touch.
#: The same TTL bounds a notification reference count: a count nobody has
#: refreshed for this long is forgotten, which is what keeps a dropped unlease
#: from pinning an entry for the life of the process.
LEASE_TTL_S = 120.0

#: How long an entry with no known lease is still spared by eviction. Longer
#: than LEASE_TTL_S on purpose: a notification may be dropped, so "no lease" is
#: "the plugin does not know", and the entry has to age before that is treated
#: as "the engine is done with it".
GRACE_S = 300.0

#: What a line without an id may ask for. Nothing else is a notification.
NOTIFICATION_OPS = ("lease", "unlease", "touch", "set_pin")

#: Staged temporary: ".<name>.stage-<token>". Never listed, never served.
STAGE_INFIX = ".stage-"
STAGE_FILE_MODE = 0o600
STORE_DIR_MODE = 0o700

_OPEN_FLAGS_READ = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
_OPEN_FLAGS_WRITE = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
_OPEN_FLAGS_STAGE = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC


class StoreError(Exception):
    """A refused store operation. code holds the wire error code."""

    code = ERROR_IO


class InvalidRequest(StoreError):
    """A name, a token or a field the request got wrong."""

    code = ERROR_INVALID


class MissingArtifact(StoreError):
    """The name does not exist in the store."""

    code = ERROR_MISS


class BusyArtifact(StoreError):
    """A live lease holds the name."""

    code = ERROR_BUSY


class StoreIOError(StoreError):
    """The filesystem refused an operation."""

    code = ERROR_IO


def validate_name(name: object) -> str:
    """Return name when it obeys the name rule, else raise InvalidRequest.

    Flat basename, non-empty, at most MAX_NAME_BYTES UTF-8 bytes, no slash, no
    leading dot, no NUL. The leading-dot rule is also what keeps staged
    temporaries out of reach of every operation here.
    """
    if not isinstance(name, str) or not name:
        raise InvalidRequest("name is required and must be a non-empty string")
    if "\x00" in name:
        raise InvalidRequest("name must not contain a NUL byte")
    if "/" in name:
        raise InvalidRequest(f"name {name!r} is not a flat basename: it contains a slash")
    if name.startswith("."):
        raise InvalidRequest(f"name {name!r} starts with a dot, which the name rule forbids")
    size = len(name.encode("utf-8"))
    if size > MAX_NAME_BYTES:
        raise InvalidRequest(f"name is {size} bytes; the limit is {MAX_NAME_BYTES}")
    return name


def _require_count(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidRequest(f"{field} must be an integer")
    if value < 0:
        raise InvalidRequest(f"{field} must be >= 0")
    return value


def _require_bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise InvalidRequest(f"{field} must be a boolean")
    return value


@dataclass(frozen=True)
class LeaseOpen:
    """One live lease plus the read-only descriptor that backs it."""

    lease_id: str
    name: str
    size: int
    fd: int


@dataclass(frozen=True)
class StagedWrite:
    """A write token plus the descriptor the engine writes through."""

    token: str
    name: str
    fd: int
    temp: str | None
    committed: int | None


@dataclass
class _WriteRecord:
    token: str
    name: str
    temp: str | None
    committed: int | None


@dataclass
class _RefCount:
    """How many notifications say they hold a name, and when that lapses."""

    count: int
    expires_at: float


class Store:
    """The plugin's own records over the directories it owns.

    Two directories: disk_dir holds the spill artifacts, slot_dir (optional)
    holds session files. Both are only listed, stat-ed, removed and tracked by
    name; neither is parsed, and a write lands in disk_dir and nowhere else. A
    name present in both resolves to disk_dir.

    All state lives in memory and dies with the process: a restart starts with
    an empty lease table, an empty write-token table, an empty pin set and an
    empty last_used map. The last_used map falls back to the file's mtime, so a
    restart does not hand every entry to the eviction at once.

    Two clocks, because they answer different questions: clock() is monotonic
    and stamps the expiries this process sets, wall_clock() reads the calendar
    and is what a file's mtime can be compared with.
    """

    def __init__(
        self,
        disk_dir: str | os.PathLike[str],
        *,
        slot_dir: str | os.PathLike[str] | None = None,
        lease_ttl_s: float = LEASE_TTL_S,
        grace_s: float = GRACE_S,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if lease_ttl_s <= 0:
            raise ValueError("lease_ttl_s must be > 0")
        if grace_s <= 0:
            raise ValueError("grace_s must be > 0")
        self.disk_dir = os.path.abspath(os.fspath(disk_dir))
        self.slot_dir = None if slot_dir is None else os.path.abspath(os.fspath(slot_dir))
        self.lease_ttl_s = float(lease_ttl_s)
        self.grace_s = float(grace_s)
        self._clock = clock
        self._wall = wall_clock
        self._leases: dict[str, dict[str, float]] = {}
        self._writes: dict[str, _WriteRecord] = {}
        self._pins: set[str] = set()
        #: name -> how many notifications claim it, with the TTL of that claim.
        self._notified: dict[str, _RefCount] = {}
        #: name -> wall-clock time the plugin last saw the name used.
        self._last_used: dict[str, float] = {}
        self.counters: dict[str, int] = {
            "leases_total": 0,
            "writes_staged_total": 0,
            "writes_committed_total": 0,
            "writes_aborted_total": 0,
            "removes_total": 0,
            "removes_refused": 0,
            "notifications_applied": 0,
            "evictions_total": 0,
        }
        self._prepared = False

    # ---------------------------------------------------------------- setup

    @property
    def roots(self) -> tuple[str, ...]:
        """The directories the store owns, disk first."""
        return (self.disk_dir,) if self.slot_dir is None else (self.disk_dir, self.slot_dir)

    def prepare(self) -> None:
        """Check both owned directories and create them when missing. Idempotent."""
        for directory in self.roots:
            self._prepare_root(directory)
        self._prepared = True

    def _prepare_root(self, directory: str) -> None:
        if scan.is_protected_path(directory):
            raise StoreError(
                f"refusing to use {scan.lexical_path(directory)} as a store:"
                " it lies inside a protected root"
            )
        try:
            info = os.lstat(directory)
        except FileNotFoundError:
            try:
                os.makedirs(directory, mode=STORE_DIR_MODE, exist_ok=True)
            except OSError as exc:
                raise StoreError(
                    f"cannot create store directory {directory}: {exc.strerror or exc}"
                ) from exc
            try:
                info = os.lstat(directory)
            except OSError as exc:
                raise StoreError(
                    f"cannot stat store directory {directory}: {exc.strerror or exc}"
                ) from exc
        except OSError as exc:
            raise StoreError(
                f"cannot stat store directory {directory}: {exc.strerror or exc}"
            ) from exc
        if stat_mod.S_ISLNK(info.st_mode):
            raise StoreError(
                f"refusing to use {directory} as a store: it is a symlink,"
                " and a resolved store would defeat the protected-root check"
            )
        if not stat_mod.S_ISDIR(info.st_mode):
            raise StoreError(f"store path {directory} is not a directory")

    # ---------------------------------------------------------- lease table

    def expire(self) -> int:
        """Drop every expired lease and return how many were dropped."""
        now = self._clock()
        dropped = 0
        for name in list(self._leases):
            live = {lease: expiry for lease, expiry in self._leases[name].items() if expiry > now}
            dropped += len(self._leases[name]) - len(live)
            if live:
                self._leases[name] = live
            else:
                del self._leases[name]
        return dropped

    @property
    def leases_live(self) -> int:
        self.expire()
        return sum(len(leases) for leases in self._leases.values())

    @property
    def writes_pending(self) -> int:
        return len(self._writes)

    def leases_on(self, name: str) -> int:
        self.expire()
        return len(self._leases.get(name, ()))

    def _refresh(self, name: str) -> None:
        """Push every live lease on *name* out to a full TTL from now."""
        now = self._clock()
        for lease_id in self._leases.get(name, {}):
            self._leases[name][lease_id] = now + self.lease_ttl_s

    # ----------------------------------------------------- notification state

    def notify(self, op: object, name: object, on: object = None) -> bool:
        """Apply one fire-and-forget notification; False means it was ignored.

        A notification carries no id, no epoch and no answer, and it may be
        dropped before it arrives. So nothing here raises, and nothing is read
        as a negative fact: a missing unlease does not prove the engine is done
        with an entry, and the TTL is what forgets a count nobody refreshed.

        This method counts what it applied. How many lines arrived, and how
        many of those it refused, is the socket server's count: that is where
        "arrived" is observable.
        """
        if not isinstance(op, str):
            return False
        try:
            clean = validate_name(name)
        except InvalidRequest:
            return False
        now = self._clock()
        if op == "lease":
            record = self._notified.get(clean)
            count = 0 if record is None else record.count
            self._notified[clean] = _RefCount(count=count + 1, expires_at=now + self.lease_ttl_s)
        elif op == "unlease":
            record = self._notified.get(clean)
            if record is None:
                # An unlease for a lease we never heard of. Nothing to drop, and
                # nothing to conclude: the lease may have been dropped on the way.
                pass
            elif record.count <= 1:
                del self._notified[clean]
            else:
                record.count -= 1
                record.expires_at = now + self.lease_ttl_s
        elif op == "touch":
            record = self._notified.get(clean)
            if record is not None:
                record.expires_at = now + self.lease_ttl_s
        elif op == "set_pin":
            if not isinstance(on, bool):
                return False
            if on:
                self._pins.add(clean)
            else:
                self._pins.discard(clean)
            self.counters["notifications_applied"] += 1
            return True
        else:
            return False
        # A lease, an unlease and a touch all say the engine is working with the
        # name right now, so each one restarts the grace period. A pin says what
        # to keep, not what is being used, so it does not.
        self._mark_used(clean)
        self.counters["notifications_applied"] += 1
        return True

    def expire_notifications(self) -> int:
        """Forget every notification count past its TTL and return how many."""
        now = self._clock()
        stale = [name for name, record in self._notified.items() if record.expires_at <= now]
        for name in stale:
            del self._notified[name]
        return len(stale)

    def notified_leases(self, name: str) -> int:
        """How many live notifications hold *name* right now."""
        self.expire_notifications()
        record = self._notified.get(name)
        return 0 if record is None else record.count

    @property
    def notified_leases_live(self) -> int:
        self.expire_notifications()
        return sum(record.count for record in self._notified.values())

    def _mark_used(self, name: str) -> None:
        self._last_used[name] = self._wall()

    def last_used(self, name: object) -> float:
        """When the plugin last saw name in use, as a wall-clock time.

        The plugin's own record comes first. A name it has no record of - a
        fresh start, or a notification that never arrived - falls back to the
        file's mtime, so a restart cannot hand every entry to the eviction at
        once. A name no owned directory holds answers -inf: there is no
        evidence it was ever used, so nothing is gained by sparing it.
        """
        clean = validate_name(name)
        known = self._last_used.get(clean)
        if known is not None:
            return known
        try:
            info = os.stat(self.path_of(clean), follow_symlinks=False)
        except OSError:
            return float("-inf")
        return info.st_mtime

    # ------------------------------------------------------------ operations

    def lease(self, name: object) -> LeaseOpen:
        """Open name read-only and record a lease on it."""
        clean = validate_name(name)
        path = self.path_of(clean)
        fd = self._open(path, clean, _OPEN_FLAGS_READ)
        try:
            info = os.fstat(fd)
            if not stat_mod.S_ISREG(info.st_mode):
                raise InvalidRequest(f"{clean} is not a regular file")
        except BaseException:
            os.close(fd)
            raise
        # "Refreshed by lease_read on the same name": a repeat read keeps the
        # leases already held on that name alive for another full TTL.
        self._refresh(clean)
        self._mark_used(clean)
        lease_id = self._unique_lease_id()
        self._leases.setdefault(clean, {})[lease_id] = self._clock() + self.lease_ttl_s
        self.counters["leases_total"] += 1
        return LeaseOpen(lease_id=lease_id, name=clean, size=info.st_size, fd=fd)

    def release(self, lease_id: object) -> bool:
        """Drop one lease. Idempotent: an unknown or expired lease answers False."""
        if not isinstance(lease_id, str) or not lease_id:
            raise InvalidRequest("lease is required and must be a non-empty string")
        self.expire()
        holder = next((name for name, live in self._leases.items() if lease_id in live), None)
        if holder is None:
            return False
        live = self._leases[holder]
        del live[lease_id]
        if not live:
            del self._leases[holder]
        return True

    def touch(self, name: object) -> bool:
        """Refresh the leases on name; report whether the artifact exists."""
        clean = validate_name(name)
        self.expire()
        self._refresh(clean)
        exists, _ = self.stat(clean)
        if exists:
            self._mark_used(clean)
        return exists

    def set_pin(self, name: object, on: object) -> bool:
        """Pin or unpin name. Returns the new pin state."""
        clean = validate_name(name)
        wanted = _require_bool(on, "on")
        if wanted:
            exists, _ = self.stat(clean)
            if not exists:
                raise MissingArtifact(f"no such artifact: {clean}")
            self._pins.add(clean)
            return True
        self._pins.discard(clean)
        return False

    def is_pinned(self, name: str) -> bool:
        return name in self._pins

    def remove(self, name: object) -> bool:
        """Delete name unless a lease holds it. Never touches a temporary."""
        clean = validate_name(name)
        self.expire()
        if self._leases.get(clean):
            self.counters["removes_refused"] += 1
            raise BusyArtifact(
                f"{clean} has {len(self._leases[clean])} live lease(s); release them first"
            )
        try:
            os.unlink(self.path_of(clean))
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise StoreIOError(f"cannot remove {clean}: {exc.strerror or exc}") from exc
        self._pins.discard(clean)
        self._notified.pop(clean, None)
        self._last_used.pop(clean, None)
        self.counters["removes_total"] += 1
        return True

    # -------------------------------------------------------------- writes

    def stage_write(self, name: object) -> tuple[StagedWrite, list[str]]:
        """Create ".<name>.stage-<token>"; abort a pending write on the same name.

        The second element lists the tokens whose temporaries this call
        discarded, in the order they were discarded.
        """
        clean = validate_name(name)
        aborted: list[str] = []
        for record in list(self._writes.values()):
            if record.name == clean:
                self._discard(record)
                aborted.append(record.token)
        token = self._unique_token()
        temp = f".{clean}{STAGE_INFIX}{token}"
        path = self._temporary_path(temp)
        try:
            fd = os.open(path, _OPEN_FLAGS_STAGE, STAGE_FILE_MODE)
        except OSError as exc:
            raise StoreIOError(f"cannot stage a write for {clean}: {exc.strerror or exc}") from exc
        self._writes[token] = _WriteRecord(token=token, name=clean, temp=temp, committed=None)
        self.counters["writes_staged_total"] += 1
        return (
            StagedWrite(token=token, name=clean, fd=fd, temp=temp, committed=None),
            aborted,
        )

    def stage_append(self, name: object, committed: object) -> StagedWrite:
        """Open name read-write and truncate it to committed."""
        clean = validate_name(name)
        length = _require_count(committed, "committed")
        path = self._write_path(clean)
        fd = self._open(path, clean, _OPEN_FLAGS_WRITE)
        try:
            info = os.fstat(fd)
            if not stat_mod.S_ISREG(info.st_mode):
                raise InvalidRequest(f"{clean} is not a regular file")
            if length > info.st_size:
                raise InvalidRequest(
                    f"committed {length} exceeds the size of {clean} ({info.st_size} bytes)"
                )
            os.ftruncate(fd, length)
        except BaseException:
            os.close(fd)
            raise
        token = self._unique_token()
        self._writes[token] = _WriteRecord(token=token, name=clean, temp=None, committed=length)
        self.counters["writes_staged_total"] += 1
        return StagedWrite(token=token, name=clean, fd=fd, temp=None, committed=length)

    def commit_write(
        self, name: object, token: object, size: object = None
    ) -> tuple[bool, int | None]:
        """Publish a staged write. Returns (published, committed).

        published False means the rename happened but the directory flush
        failed: the name is visible and not durable, which is what the protocol
        asks the caller to assume.
        """
        clean = validate_name(name)
        record = self._write_record(clean, token)
        if size is not None:
            _require_count(size, "bytes")
        target = self._write_path(clean)
        if record.temp is not None:
            try:
                os.rename(self._temporary_path(record.temp), target)
            except OSError as exc:
                raise StoreIOError(f"cannot publish {clean}: {exc.strerror or exc}") from exc
        del self._writes[record.token]
        published = self._sync_directory(os.path.dirname(target))
        self._mark_used(clean)
        self.counters["writes_committed_total"] += 1
        return published, record.committed

    def abort_write(self, name: object, token: object) -> bool:
        """Unlink the staged temporary of token. Never removes name."""
        clean = validate_name(name)
        record = self._writes.get(token) if isinstance(token, str) else None
        if record is None or record.name != clean:
            return False
        self._discard(record)
        self.counters["writes_aborted_total"] += 1
        return True

    # ---------------------------------------------------------- index reads

    def stat(self, name: object) -> tuple[bool, int]:
        """(exists, bytes) for name, without opening it. Regular files only."""
        clean = validate_name(name)
        try:
            info = os.stat(self.path_of(clean), follow_symlinks=False)
        except FileNotFoundError:
            return False, 0
        except OSError as exc:
            raise StoreIOError(f"cannot stat {clean}: {exc.strerror or exc}") from exc
        if not stat_mod.S_ISREG(info.st_mode):
            return False, 0
        return True, info.st_size

    def list_artifacts(self) -> list[dict[str, Any]]:
        """Every regular artifact the store will serve, with its size.

        Both owned directories are listed. A name present in both is reported
        once, from the directory it resolves to - disk first. Names outside the
        name rule are skipped, which is what keeps staged temporaries and stray
        dot files out of the listing.
        """
        files: list[dict[str, Any]] = []
        seen: set[str] = set()
        for directory in self.roots:
            try:
                with os.scandir(directory) as iterator:
                    entries = sorted(iterator, key=lambda item: item.name)
            except OSError as exc:
                raise StoreIOError(
                    f"cannot list store directory {directory}: {exc.strerror or exc}"
                ) from exc
            for entry in entries:
                if entry.name in seen:
                    continue
                try:
                    validate_name(entry.name)
                except InvalidRequest:
                    continue
                try:
                    info = entry.stat(follow_symlinks=False)
                except OSError as exc:
                    raise StoreIOError(
                        f"cannot stat {entry.name}: {exc.strerror or exc}"
                    ) from exc
                if not stat_mod.S_ISREG(info.st_mode):
                    continue
                seen.add(entry.name)
                files.append({"name": entry.name, "bytes": info.st_size})
        return sorted(files, key=lambda item: item["name"])

    def evict(self, budget_bytes: object) -> dict[str, Any]:
        """Delete least-recently-used artifacts until the store fits *budget_bytes*.

        This is the one place the store deletes an artifact without being told
        to, so the grace period is the whole safety argument. A notification can
        be dropped when the plugin's socket is busy, so "no live lease" does not
        mean "the engine is done with it": it means the plugin does not know.
        The rule that follows:

        - An entry with no live wire lease and no live notification count is
          still spared while its last_used - the plugin's own record, or the
          file's mtime when the plugin has no record of that name - is younger
          than grace_s (default 300 s).
        - A pinned entry is never evicted, a leased entry is never evicted, and
          an entry inside the grace period is never evicted, whatever the
          budget asks for. Eviction gives up on the budget before it breaks
          either rule.
        - Only regular files whose names obey the name rule are candidates: an
          artifact is deleted at the path the store would serve it from, and
          that path is built from a validated name, so a temporary and a stray
          dot file are out of reach.

        remove() is the other path, and it answers an explicit request. Those
        two are the only paths that delete an artifact.
        """
        budget = _require_count(budget_bytes, "budget_bytes")
        self.expire()
        self.expire_notifications()
        horizon = self._wall() - self.grace_s
        files = self.list_artifacts()
        held = sum(item["bytes"] for item in files)
        skipped = {"pinned": 0, "leased": 0, "in_grace": 0}
        candidates: list[tuple[float, str, int]] = []
        for item in files:
            name = item["name"]
            if self.is_pinned(name):
                skipped["pinned"] += 1
                continue
            if self._leases.get(name) or self._notified.get(name):
                skipped["leased"] += 1
                continue
            used = self.last_used(name)
            if used > horizon:
                skipped["in_grace"] += 1
                continue
            candidates.append((used, name, item["bytes"]))
        candidates.sort()
        removed: list[str] = []
        freed = 0
        for _used, name, size in candidates:
            if held <= budget:
                break
            try:
                os.unlink(self.path_of(name))
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise StoreIOError(f"cannot remove {name}: {exc.strerror or exc}") from exc
            self._pins.discard(name)
            self._notified.pop(name, None)
            self._last_used.pop(name, None)
            removed.append(name)
            freed += size
            held -= size
        self.counters["evictions_total"] += len(removed)
        return {
            "removed": removed,
            "freed_bytes": freed,
            "bytes": held,
            "budget_bytes": budget,
            "grace_s": self.grace_s,
            "skipped": skipped,
        }

    def stats(self) -> dict[str, Any]:
        """Gauges and counters, as reported by the stats operation."""
        snapshot: dict[str, Any] = {
            "disk_dir": self.disk_dir,
            "slot_dir": self.slot_dir,
            "lease_ttl_s": self.lease_ttl_s,
            "grace_s": self.grace_s,
            "leases_live": self.leases_live,
            "notified_leases_live": self.notified_leases_live,
            "writes_pending": self.writes_pending,
            "pins": len(self._pins),
        }
        snapshot.update(self.counters)
        return snapshot

    # ------------------------------------------------------------- plumbing

    def path_of(self, name: str) -> str:
        """Absolute path of an already-validated artifact name.

        The first owned directory that holds the name, disk first, so a name
        present in both resolves to exactly one of them. A name neither holds
        answers with the disk directory's path, which is where a new artifact
        is created.
        """
        for directory in self.roots:
            candidate = os.path.join(directory, name)
            if os.path.lexists(candidate):
                return candidate
        return os.path.join(self.disk_dir, name)

    def _write_path(self, name: str) -> str:
        """Absolute path of an already-validated name a write may touch: writes stay in the disk directory."""
        return os.path.join(self.disk_dir, name)

    def _temporary_path(self, name: str) -> str:
        """Absolute path of a temporary: this module builds those names, and they live in the disk directory."""
        return os.path.join(self.disk_dir, name)

    def _open(self, path: str, name: str, flags: int) -> int:
        try:
            return os.open(path, flags)
        except OSError as exc:
            if exc.errno in (errno.ENOENT, errno.ENOTDIR):
                raise MissingArtifact(f"no such artifact: {name}") from exc
            if exc.errno == errno.ELOOP:
                raise InvalidRequest(
                    f"{name} is a symlink; the store opens with O_NOFOLLOW and does not follow it"
                ) from exc
            raise StoreIOError(f"cannot open {name}: {exc.strerror or exc}") from exc

    def _write_record(self, name: str, token: object) -> _WriteRecord:
        if not isinstance(token, str) or not token:
            raise InvalidRequest("token is required and must be a non-empty string")
        record = self._writes.get(token)
        if record is None:
            raise InvalidRequest(f"no pending write for token {token!r}")
        if record.name != name:
            raise InvalidRequest(f"token {token!r} belongs to {record.name}, not {name}")
        return record

    def _discard(self, record: _WriteRecord) -> None:
        """Forget a write token and unlink its temporary when it has one."""
        self._writes.pop(record.token, None)
        if record.temp is None:
            return
        try:
            os.unlink(self._temporary_path(record.temp))
        except FileNotFoundError:
            return
        except OSError as exc:
            raise StoreIOError(
                f"cannot remove the temporary of {record.name}: {exc.strerror or exc}"
            ) from exc

    def _sync_directory(self, directory: str) -> bool:
        """Flush the directory a rename landed in, and the disk directory when it was the source."""
        flushed = self._fsync_directory(directory)
        if directory != self.disk_dir:
            flushed = self._fsync_directory(self.disk_dir) and flushed
        return flushed

    def _fsync_directory(self, directory: str) -> bool:
        try:
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        except OSError:
            return False
        try:
            os.fsync(fd)
            return True
        except OSError:
            return False
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    def _unique_lease_id(self) -> str:
        self.expire()
        while True:
            lease_id = "L-" + secrets.token_hex(4)
            if not any(lease_id in live for live in self._leases.values()):
                return lease_id

    def _unique_token(self) -> str:
        while True:
            token = secrets.token_hex(8)
            if token not in self._writes:
                return token
