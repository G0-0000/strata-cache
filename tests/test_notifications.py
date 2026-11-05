"""Notification tests: fire-and-forget lines, the lease count and the grace period.

The engine sends notifications with one non-blocking send() and reads nothing
back, so these tests assert on two things only: what the connection received
(nothing, for a notification) and what the store now believes, read through a
request on the same connection. The dropped-notification case is the reason the
grace period exists, so it is tested by never sending the notification at all.
"""

from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path

import pytest

from support import FdClient, Server, connect, exchange, free_socket_dir, private_dir

QUIET_TIMEOUT_S = 0.5
OLD_MTIME_S = 3600.0


def age(path: Path, seconds: float = OLD_MTIME_S) -> None:
    """Push a file's mtime into the past, so the grace period has run out."""
    stamp = time.time() - seconds
    os.utime(path, (stamp, stamp))


def notify(conn: socket.socket, op: str, name: str | None = None, on: object = None) -> None:
    """Send one notification: no id, no epoch, and nothing to read back."""
    payload: dict[str, object] = {"op": op}
    if name is not None:
        payload["name"] = name
    if on is not None:
        payload["on"] = on
    conn.sendall(json.dumps(payload).encode("utf-8") + b"\n")


def expect_silence(conn: socket.socket) -> None:
    """Assert that the server wrote nothing back for a while."""
    conn.settimeout(QUIET_TIMEOUT_S)
    with pytest.raises(socket.timeout):
        conn.recv(65536)
    conn.settimeout(10.0)


def stats(conn: socket.socket, epoch: str) -> dict[str, object]:
    """Read the store's own view through a request, on the same connection."""
    reply = exchange(conn, {"id": "s", "op": "stats", "epoch": epoch})
    assert reply["ok"] is True, reply
    return reply["result"]


@pytest.fixture()
def disk_dir(tmp_path: Path) -> Path:
    return private_dir(tmp_path, "disk")


@pytest.fixture()
def slot_dir(tmp_path: Path) -> Path:
    return private_dir(tmp_path, "slots")


def test_a_notification_is_applied_and_never_answered(tmp_path: Path, disk_dir: Path) -> None:
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "lease", "conv-3.sess")
            expect_silence(conn)  # no reply, ever

            reply = exchange(conn, {"id": "1", "op": "stats", "epoch": epoch})
            assert reply["id"] == "1"  # the notification did not consume a reply slot
            result = reply["result"]
            assert result["notifications_total"] == 1
            assert result["notifications_applied"] == 1
            assert result["notifications_ignored"] == 0
            assert result["notified_leases_live"] == 1
            assert result["notifications_by_op"] == {"lease": 1}


def test_a_line_with_an_id_still_gets_a_reply(tmp_path: Path, disk_dir: Path) -> None:
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        (disk_dir / "a.sess").write_bytes(b"x")
        with connect(running.socket_path) as conn:
            reply = exchange(conn, {"id": "t", "op": "touch", "epoch": epoch, "name": "a.sess"})
            assert reply["id"] == "t"
            assert reply["ok"] is True
            assert reply["result"] == {"name": "a.sess", "touched": True}

            # The same op without an id is a notification: nothing comes back.
            notify(conn, "touch", "a.sess")
            expect_silence(conn)


def test_the_engine_line_format_is_accepted(tmp_path: Path, disk_dir: Path) -> None:
    """These are the exact bytes conversation_store_plugin.cpp writes: compact, no id, no epoch."""
    (disk_dir / "strata-conv-3.sess").write_bytes(b"payload")
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            conn.sendall(
                b'{"op":"lease","name":"strata-conv-3.sess"}\n'
                b'{"op":"touch","name":"strata-conv-3.sess"}\n'
                b'{"op":"set_pin","name":"strata-conv-3.sess","on":false}\n'
                b'{"op":"unlease","name":"strata-conv-3.sess"}\n'
                b'{"op":"lease","name":"quoted\\"name.sess"}\n'
            )
            expect_silence(conn)  # five notifications, not one reply
            result = stats(conn, epoch)
            assert result["notifications_total"] == 5
            assert result["notifications_applied"] == 5
            assert result["notifications_by_op"] == {
                "lease": 2,
                "touch": 1,
                "set_pin": 1,
                "unlease": 1,
            }
            assert result["notified_leases_live"] == 1


def test_mixed_requests_and_notifications_share_one_connection(
    tmp_path: Path, disk_dir: Path
) -> None:
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        (disk_dir / "kept.sess").write_bytes(b"1234")
        with connect(running.socket_path) as conn:
            notify(conn, "lease", "kept.sess")
            notify(conn, "touch", "kept.sess")

            listed = exchange(conn, {"id": "l", "op": "list", "epoch": epoch})
            assert listed["ok"] is True
            assert listed["result"]["files"] == [{"name": "kept.sess", "bytes": 4}]

            notify(conn, "unlease", "kept.sess")
            reply = exchange(conn, {"id": "s", "op": "stats", "epoch": epoch})
            assert reply["result"]["notifications_total"] == 3
            assert reply["result"]["notifications_applied"] == 3
            assert reply["result"]["notified_leases_live"] == 0


def test_a_partial_trailing_line_at_eof_is_ignored(tmp_path: Path, disk_dir: Path) -> None:
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        partial = connect(running.socket_path)
        # One whole line, then half of one, then the client goes away.
        partial.sendall(b'{"op":"lease","name":"whole.sess"}\n{"op":"lease","name":"hal')
        partial.close()

        with connect(running.socket_path) as conn:
            result = stats(conn, epoch)
        assert result["notifications_total"] == 1
        assert result["notified_leases_live"] == 1


def test_the_notification_lease_count_is_per_name(tmp_path: Path, disk_dir: Path) -> None:
    (disk_dir / "held.sess").write_bytes(b"held")
    with Server(
        free_socket_dir(tmp_path),
        "--disk-dir",
        str(disk_dir),
        "--grace-s",
        "0.2",
        "--lease-ttl-s",
        "60",
    ) as running:
        epoch = running.wait_ready()
        age(disk_dir / "held.sess")
        with connect(running.socket_path) as conn:
            notify(conn, "lease", "held.sess")
            notify(conn, "lease", "held.sess")
            assert stats(conn, epoch)["notified_leases_live"] == 2

            evicted = exchange(conn, {"id": "e", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert evicted["ok"] is True, evicted
            assert evicted["result"]["removed"] == []
            assert evicted["result"]["skipped"]["leased"] == 1
            assert (disk_dir / "held.sess").exists()

            notify(conn, "unlease", "held.sess")
            assert stats(conn, epoch)["notified_leases_live"] == 1
            still = exchange(conn, {"id": "e2", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert still["result"]["removed"] == []

            notify(conn, "unlease", "held.sess")
            assert stats(conn, epoch)["notified_leases_live"] == 0
            # The unlease is activity itself, so the grace period restarts from it.
            inside = exchange(conn, {"id": "e3", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert inside["result"]["removed"] == []
            assert inside["result"]["skipped"]["in_grace"] == 1

            time.sleep(0.35)
            gone = exchange(conn, {"id": "e4", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert gone["result"]["removed"] == ["held.sess"]
            assert (disk_dir / "held.sess").exists() is False


def test_a_dropped_lease_leaves_the_entry_inside_the_grace_period(
    tmp_path: Path, disk_dir: Path
) -> None:
    """The notification is never sent: the plugin must still not delete the entry."""
    (disk_dir / "dropped.sess").write_bytes(b"payload")
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            hello = exchange(conn, {"id": "h", "op": "hello", "protocol": 1})
            assert hello["result"]["grace_s"] == 300.0  # the default, in seconds

            evicted = exchange(conn, {"id": "e", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert evicted["ok"] is True, evicted
            assert evicted["result"]["removed"] == []
            assert evicted["result"]["skipped"]["in_grace"] == 1
            assert evicted["result"]["bytes"] == len(b"payload")
        assert (disk_dir / "dropped.sess").read_bytes() == b"payload"


def test_an_old_entry_outside_the_grace_period_is_evicted(tmp_path: Path, disk_dir: Path) -> None:
    (disk_dir / "stale.sess").write_bytes(b"stale")
    (disk_dir / "fresh.sess").write_bytes(b"fresh")
    age(disk_dir / "stale.sess")
    with Server(
        free_socket_dir(tmp_path), "--disk-dir", str(disk_dir), "--grace-s", "300"
    ) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            evicted = exchange(conn, {"id": "e", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert evicted["result"]["removed"] == ["stale.sess"]
            assert evicted["result"]["freed_bytes"] == len(b"stale")
            assert evicted["result"]["skipped"]["in_grace"] == 1
        assert (disk_dir / "stale.sess").exists() is False
        assert (disk_dir / "fresh.sess").read_bytes() == b"fresh"


def test_touch_updates_last_used_and_eviction_respects_it(tmp_path: Path, disk_dir: Path) -> None:
    (disk_dir / "used.sess").write_bytes(b"used")
    age(disk_dir / "used.sess")
    with Server(
        free_socket_dir(tmp_path), "--disk-dir", str(disk_dir), "--grace-s", "0.2"
    ) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "touch", "used.sess")
            kept = exchange(conn, {"id": "e", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert kept["result"]["removed"] == []
            assert kept["result"]["skipped"]["in_grace"] == 1

            time.sleep(0.35)  # the grace period runs out from the touch
            gone = exchange(conn, {"id": "e2", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert gone["result"]["removed"] == ["used.sess"]


def test_set_pin_keeps_an_entry_through_eviction(tmp_path: Path, disk_dir: Path) -> None:
    (disk_dir / "pin.sess").write_bytes(b"pin")
    age(disk_dir / "pin.sess")
    with Server(
        free_socket_dir(tmp_path), "--disk-dir", str(disk_dir), "--grace-s", "0.2"
    ) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "set_pin", "pin.sess", on=True)
            held = exchange(conn, {"id": "e", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert held["result"]["removed"] == []
            assert held["result"]["skipped"]["pinned"] == 1
            assert stats(conn, epoch)["pins"] == 1

            # A pin is not activity: unpinning puts the entry back under plain eviction.
            notify(conn, "set_pin", "pin.sess", on=False)
            released = exchange(conn, {"id": "e2", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert released["result"]["removed"] == ["pin.sess"]
        assert (disk_dir / "pin.sess").exists() is False


def test_a_notification_count_lapses_after_its_ttl(tmp_path: Path, disk_dir: Path) -> None:
    """A dropped unlease must not hold a name for the life of the process."""
    (disk_dir / "lapsed.sess").write_bytes(b"lapsed")
    age(disk_dir / "lapsed.sess")
    with Server(
        free_socket_dir(tmp_path),
        "--disk-dir",
        str(disk_dir),
        "--grace-s",
        "0.2",
        "--lease-ttl-s",
        "0.2",
    ) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "lease", "lapsed.sess")  # the unlease is never sent
            held = exchange(conn, {"id": "e", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert held["result"]["removed"] == []

            time.sleep(0.45)  # past the TTL: the count is forgotten
            gone = exchange(conn, {"id": "e2", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert gone["result"]["removed"] == ["lapsed.sess"]


def test_ignored_notifications_are_counted_and_harmless(
    tmp_path: Path, disk_dir: Path
) -> None:
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        (disk_dir / "a.sess").write_bytes(b"a")
        with connect(running.socket_path) as conn:
            notify(conn, "lease")  # no name
            notify(conn, "lease", ".hidden.sess")  # outside the name rule
            notify(conn, "flush", "a.sess")  # not a notification
            notify(conn, "set_pin", "a.sess", on="yes")  # on must be a boolean
            notify(conn, "set_pin", "absent.sess", on=True)  # a pin may precede the file

            result = stats(conn, epoch)
            assert result["notifications_total"] == 5
            assert result["notifications_ignored"] == 4
            assert result["notifications_applied"] == 1
            assert result["pins"] == 1
            assert stats(conn, epoch)["requests_total"] >= 1


def test_notifications_are_ignored_without_a_store(tmp_path: Path) -> None:
    with Server(free_socket_dir(tmp_path)) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "lease", "a.sess")
            result = stats(conn, epoch)
            assert result["notifications_total"] == 1
            assert result["notifications_ignored"] == 1
            assert result["disk_dir"] is None


def test_evict_needs_a_budget_and_an_epoch(tmp_path: Path, disk_dir: Path) -> None:
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            missing = exchange(conn, {"id": "e", "op": "evict", "epoch": epoch})
            assert missing["error"]["code"] == "invalid"
            wrong_type = exchange(
                conn, {"id": "e2", "op": "evict", "epoch": epoch, "budget_bytes": "0"}
            )
            assert wrong_type["error"]["code"] == "invalid"
            no_epoch = exchange(conn, {"id": "e3", "op": "evict", "budget_bytes": 0})
            assert no_epoch["error"]["code"] == "epoch_mismatch"


def test_the_slot_directory_is_listed_and_evictable_but_never_written_to(
    tmp_path: Path, disk_dir: Path, slot_dir: Path
) -> None:
    (slot_dir / "session.bin").write_bytes(b"session")
    (disk_dir / "spill.sess").write_bytes(b"spill")
    (disk_dir / "both.sess").write_bytes(b"disk")
    (slot_dir / "both.sess").write_bytes(b"slot!")
    for path in (
        slot_dir / "session.bin",
        slot_dir / "both.sess",
        disk_dir / "spill.sess",
        disk_dir / "both.sess",
    ):
        age(path)
    with Server(
        free_socket_dir(tmp_path),
        "--disk-dir",
        str(disk_dir),
        "--slot-dir",
        str(slot_dir),
        "--grace-s",
        "0.2",
    ) as running:
        running.wait_ready()
        with FdClient(running.socket_path) as client:
            hello = client.hello()
            assert hello["slot_dir"] == str(slot_dir)

            listed, _ = client.call("list")
            assert listed["result"]["files"] == [
                {"name": "both.sess", "bytes": 4},  # the disk copy: disk wins
                {"name": "session.bin", "bytes": 7},
                {"name": "spill.sess", "bytes": 5},
            ]

            found, _ = client.call("stat", name="session.bin")
            assert found["result"] == {"name": "session.bin", "exists": True, "bytes": 7}

            # An append resolves in the disk directory only: a slot-only name is a miss,
            # so the plugin cannot leave a disk twin beside a session file.
            appended, fd = client.call("stage_append", name="session.bin", committed=0)
            assert fd is None
            assert appended["error"]["code"] == "miss"
            assert (slot_dir / "session.bin").read_bytes() == b"session"
            assert (disk_dir / "session.bin").exists() is False

            removed, _ = client.call("remove", name="both.sess")
            assert removed["result"]["removed"] is True
            assert (disk_dir / "both.sess").exists() is False
            assert (slot_dir / "both.sess").read_bytes() == b"slot!"  # still there, now the only copy
            after, _ = client.call("stat", name="both.sess")
            assert after["result"]["bytes"] == 5

            evicted, _ = client.call("evict", budget_bytes=0)
            assert sorted(evicted["result"]["removed"]) == ["both.sess", "session.bin", "spill.sess"]
            assert (slot_dir / "session.bin").exists() is False


def test_a_remove_notification_drops_the_entry_and_its_records(
    tmp_path: Path, disk_dir: Path
) -> None:
    """The engine deleted the name: its count, its pin and its last_used go with it."""
    (disk_dir / "gone.sess").write_bytes(b"gone")
    age(disk_dir / "gone.sess")  # an old file, so only the plugin's own records could spare it
    with Server(
        free_socket_dir(tmp_path),
        "--disk-dir",
        str(disk_dir),
        "--grace-s",
        "0.3",
        "--lease-ttl-s",
        "60",
    ) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "lease", "gone.sess")
            notify(conn, "lease", "gone.sess")
            notify(conn, "set_pin", "gone.sess", on=True)
            notify(conn, "touch", "gone.sess")  # last_used is now, inside the grace period
            held = stats(conn, epoch)
            assert held["notified_leases_live"] == 2
            assert held["pins"] == 1
            kept = exchange(conn, {"id": "e", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert kept["result"]["removed"] == []
            assert kept["result"]["skipped"]["pinned"] == 1

            notify(conn, "remove", "gone.sess")
            dropped = stats(conn, epoch)
            assert dropped["notified_leases_live"] == 0
            assert dropped["pins"] == 0
            assert dropped["notifications_applied"] == 5
            # nothing of the plugin's holds the name any more, and its last_used is the old mtime
            gone = exchange(conn, {"id": "e2", "op": "evict", "epoch": epoch, "budget_bytes": 0})
            assert gone["result"]["removed"] == ["gone.sess"]
        assert (disk_dir / "gone.sess").exists() is False


def test_a_remove_notification_for_an_unknown_name_is_applied(
    tmp_path: Path, disk_dir: Path
) -> None:
    """A name the plugin never heard of: there is nothing to drop, and no failure."""
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "remove", "never-heard-of.sess")
            result = stats(conn, epoch)
            assert result["notifications_total"] == 1
            assert result["notifications_applied"] == 1
            assert result["notifications_ignored"] == 0
            assert result["notified_leases_live"] == 0


def test_remove_is_refused_while_a_notification_count_holds_the_name(
    tmp_path: Path, disk_dir: Path
) -> None:
    """A request keeps its answer: a live count is a hold, exactly like a wire lease."""
    (disk_dir / "held.sess").write_bytes(b"held")
    with Server(
        free_socket_dir(tmp_path), "--disk-dir", str(disk_dir), "--lease-ttl-s", "60"
    ) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "lease", "held.sess")
            refused = exchange(conn, {"id": "r", "op": "remove", "epoch": epoch, "name": "held.sess"})
            assert refused["error"]["code"] == "busy"
            assert "notification count" in refused["error"]["message"]
            assert (disk_dir / "held.sess").read_bytes() == b"held"
            assert stats(conn, epoch)["removes_refused"] == 1

            notify(conn, "unlease", "held.sess")
            allowed = exchange(conn, {"id": "r2", "op": "remove", "epoch": epoch, "name": "held.sess"})
            assert allowed["result"]["removed"] is True
        assert (disk_dir / "held.sess").exists() is False


def test_remove_sweeps_a_count_that_has_already_lapsed(tmp_path: Path, disk_dir: Path) -> None:
    """A hold nobody refreshed is not a hold: the request is answered, not refused."""
    (disk_dir / "lapsed.sess").write_bytes(b"lapsed")
    with Server(
        free_socket_dir(tmp_path), "--disk-dir", str(disk_dir), "--lease-ttl-s", "0.2"
    ) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "lease", "lapsed.sess")
            time.sleep(0.45)  # past the TTL
            removed = exchange(
                conn, {"id": "r", "op": "remove", "epoch": epoch, "name": "lapsed.sess"}
            )
            assert removed["ok"] is True, removed
            assert removed["result"]["removed"] is True
            assert stats(conn, epoch)["removes_refused"] == 0


def test_a_lapsed_count_neither_grows_nor_comes_back(tmp_path: Path, disk_dir: Path) -> None:
    """A lease may not add to a record that lapsed, and an unlease may not revive one."""
    (disk_dir / "counted.sess").write_bytes(b"counted")
    with Server(
        free_socket_dir(tmp_path), "--disk-dir", str(disk_dir), "--lease-ttl-s", "0.2"
    ) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "lease", "counted.sess")
            notify(conn, "lease", "counted.sess")
            assert stats(conn, epoch)["notified_leases_live"] == 2

            time.sleep(0.45)  # both lapsed
            notify(conn, "lease", "counted.sess")  # a fresh hold, not a third one
            assert stats(conn, epoch)["notified_leases_live"] == 1
            notify(conn, "unlease", "counted.sess")  # releases that one, and nothing else
            assert stats(conn, epoch)["notified_leases_live"] == 0


def test_the_first_ignored_notification_is_logged_once(tmp_path: Path, disk_dir: Path) -> None:
    """Its sender can never be told, so the plugin says it once in the log."""
    with Server(free_socket_dir(tmp_path), "--disk-dir", str(disk_dir)) as running:
        epoch = running.wait_ready()
        with connect(running.socket_path) as conn:
            notify(conn, "flush", "a.sess")  # not a notification op
            notify(conn, "lease", ".hidden.sess")  # outside the name rule
            result = stats(conn, epoch)
            assert result["notifications_ignored"] == 2
            assert result["notifications_ignored_by_op"] == {"flush": 1, "lease": 1}
        log = running.stderr_text()
        assert log.count("ignored a notification") == 1
        assert "flush" in log
