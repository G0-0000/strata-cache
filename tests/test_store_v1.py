"""v1 store tests: a real server, a real socket, real descriptor passing.

Every test names an artifact, receives a descriptor with recvmsg and moves the
bytes itself - exactly what the engine does. No artifact byte crosses the
socket, and each assertion on the wire is made on the JSON alone.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Iterator

import pytest

from support import FdClient, Server, free_socket_dir, private_dir
from strata_cache import store_v1


def artifacts(root: Path) -> list[str]:
    """Every entry in the store directory, in name order."""
    return sorted(entry.name for entry in root.iterdir())


def temporaries(root: Path) -> list[str]:
    return sorted(name for name in artifacts(root) if store_v1.STAGE_INFIX in name)


@pytest.fixture()
def store_root(tmp_path: Path) -> Path:
    return private_dir(tmp_path, "store")


@pytest.fixture()
def server(tmp_path: Path, store_root: Path) -> Iterator[Server]:
    with Server(
        free_socket_dir(tmp_path), "--disk-dir", str(store_root), "--lease-ttl-s", "120"
    ) as running:
        yield running


def test_hello_reports_protocol_one_and_the_directories(
    server: Server, store_root: Path
) -> None:
    with FdClient(server.socket_path) as client:
        result = client.hello()
        assert result["protocol"] == 1
        assert result["disk_dir"] == str(store_root)
        assert result["slot_dir"] is None
        assert result["notifications"] == ["lease", "unlease", "touch", "set_pin", "remove"]
        for feature in (
            "lease_read",
            "stage_write",
            "commit_write",
            "abort_write",
            "remove",
            "evict",
            "notify",
        ):
            assert feature in result["features"]


def test_a_foreign_protocol_is_refused_and_the_connection_closed(server: Server) -> None:
    with FdClient(server.socket_path) as client:
        reply, fd = client.call("hello", protocol=0)
        assert fd is None
        assert reply["ok"] is False
        assert reply["error"]["code"] == "not_implemented"
        assert client.conn.recv(65536) == b""  # the server hung up
        client.epoch = None


def test_hello_without_a_protocol_is_refused_and_the_connection_closed(server: Server) -> None:
    """A v0 client says nothing about its protocol: refuse it, do not answer bad_request."""
    with FdClient(server.socket_path) as client:
        reply, fd = client.call("hello")
        assert fd is None
        assert reply["ok"] is False
        assert reply["error"]["code"] == "not_implemented"
        assert "None" in reply["error"]["message"]
        assert client.conn.recv(65536) == b""  # the server hung up
        client.epoch = None
    # the server itself is fine: a client that speaks protocol 1 is served
    with FdClient(server.socket_path) as client:
        assert client.hello()["protocol"] == 1


def test_lease_read_hands_over_a_readable_descriptor(server: Server, store_root: Path) -> None:
    payload = os.urandom(128 * 1024)
    artifact = store_root / "conv-3.sess"
    artifact.write_bytes(payload)

    with FdClient(server.socket_path) as client:
        client.hello()
        reply, fd = client.call("lease_read", name="conv-3.sess")
        assert reply["ok"] is True, reply
        result = reply["result"]
        assert result == {
            "name": "conv-3.sess",
            "bytes": len(payload),
            "lease": result["lease"],
            "fd": True,
        }
        # The protocol carries numbers, never bytes.
        assert len(json.dumps(reply)) < 512
        assert fd is not None

        with os.fdopen(fd, "rb") as handle:
            assert handle.read() == payload


def test_staged_write_commit_publishes_the_name(server: Server, store_root: Path) -> None:
    payload = b"beta" * 4096
    with FdClient(server.socket_path) as client:
        client.hello()

        staged, fd = client.call("stage_write", name="new.sess")
        assert staged["ok"] is True, staged
        token = staged["result"]["token"]
        assert staged["result"]["aborted"] == []
        assert fd is not None
        assert (store_root / "new.sess").exists() is False
        assert len(temporaries(store_root)) == 1

        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

        stat_before = client.call("stat", name="new.sess")[0]
        assert stat_before["result"] == {"name": "new.sess", "exists": False, "bytes": 0}

        committed, commit_fd = client.call(
            "commit_write", name="new.sess", token=token, bytes=len(payload)
        )
        assert commit_fd is None
        assert committed["ok"] is True, committed
        assert committed["result"]["published"] is True
        assert committed["result"]["token"] == token

        assert (store_root / "new.sess").read_bytes() == payload
        assert temporaries(store_root) == []

        stat_after = client.call("stat", name="new.sess")[0]
        assert stat_after["result"] == {"name": "new.sess", "exists": True, "bytes": len(payload)}

        listed, _ = client.call("list")
        assert listed["result"]["files"] == [{"name": "new.sess", "bytes": len(payload)}]


def test_append_truncates_to_committed_and_extends(server: Server, store_root: Path) -> None:
    artifact = store_root / "chat.sess"
    artifact.write_bytes(b"0123456789")

    with FdClient(server.socket_path) as client:
        client.hello()
        staged, fd = client.call("stage_append", name="chat.sess", committed=4)
        assert staged["ok"] is True, staged
        assert staged["result"]["committed"] == 4
        assert fd is not None

        with os.fdopen(fd, "r+b") as handle:
            assert handle.read() == b"0123"  # truncated to committed
            handle.seek(0, os.SEEK_END)
            handle.write(b"XY")
            handle.flush()
            os.fsync(handle.fileno())

        committed, _ = client.call(
            "commit_write", name="chat.sess", token=staged["result"]["token"]
        )
        assert committed["result"]["published"] is True
        assert artifact.read_bytes() == b"0123XY"


def test_append_past_the_artifact_size_is_refused(server: Server, store_root: Path) -> None:
    (store_root / "short.sess").write_bytes(b"abc")
    with FdClient(server.socket_path) as client:
        client.hello()
        reply, fd = client.call("stage_append", name="short.sess", committed=99)
        assert fd is None
        assert reply["error"]["code"] == "invalid"
        assert (store_root / "short.sess").read_bytes() == b"abc"


def test_abort_leaves_neither_temporary_nor_name(server: Server, store_root: Path) -> None:
    with FdClient(server.socket_path) as client:
        client.hello()
        staged, fd = client.call("stage_write", name="gone.sess")
        token = staged["result"]["token"]
        assert len(temporaries(store_root)) == 1
        os.close(fd)

        aborted, _ = client.call("abort_write", name="gone.sess", token=token)
        assert aborted["result"]["aborted"] is True
        assert artifacts(store_root) == []

        # Aborting twice is a no-op, not an error.
        again, _ = client.call("abort_write", name="gone.sess", token=token)
        assert again["result"]["aborted"] is False


def test_a_second_stage_aborts_the_previous_temporary(server: Server, store_root: Path) -> None:
    with FdClient(server.socket_path) as client:
        client.hello()
        first, first_fd = client.call("stage_write", name="dup.sess")
        first_token = first["result"]["token"]
        first_temp = f".dup.sess{store_v1.STAGE_INFIX}{first_token}"
        assert (store_root / first_temp).exists()
        os.close(first_fd)

        second, second_fd = client.call("stage_write", name="dup.sess")
        assert second["result"]["aborted"] == [first_token]
        assert (store_root / first_temp).exists() is False
        assert len(temporaries(store_root)) == 1

        with os.fdopen(second_fd, "wb") as handle:
            handle.write(b"second")
            handle.flush()
            os.fsync(handle.fileno())
        client.call("commit_write", name="dup.sess", token=second["result"]["token"])
        assert (store_root / "dup.sess").read_bytes() == b"second"


def test_remove_is_refused_while_a_lease_is_live(server: Server, store_root: Path) -> None:
    (store_root / "held.sess").write_bytes(b"held")
    with FdClient(server.socket_path) as client:
        client.hello()
        leased, fd = client.call("lease_read", name="held.sess")
        assert fd is not None
        lease = leased["result"]["lease"]

        refused, _ = client.call("remove", name="held.sess")
        assert refused["ok"] is False
        assert refused["error"]["code"] == "busy"
        assert (store_root / "held.sess").exists()

        released, _ = client.call("release", lease=lease)
        assert released["result"]["released"] is True

        removed, _ = client.call("remove", name="held.sess")
        assert removed["result"]["removed"] is True
        assert artifacts(store_root) == []

        again, _ = client.call("release", lease=lease)
        assert again["result"]["released"] is False  # idempotent

        stats, _ = client.call("stats")
        assert stats["result"]["removes_refused"] == 1
        assert stats["result"]["leases_live"] == 0
        os.close(fd)


def test_an_expired_lease_stops_blocking_remove(tmp_path: Path, store_root: Path) -> None:
    (store_root / "stale.sess").write_bytes(b"stale")
    with Server(
        free_socket_dir(tmp_path), "--disk-dir", str(store_root), "--lease-ttl-s", "0.2"
    ) as running:
        with FdClient(running.socket_path) as client:
            client.hello()
            _lease, fd = client.call("lease_read", name="stale.sess")
            assert client.call("remove", name="stale.sess")[0]["error"]["code"] == "busy"
            os.close(fd)

            time.sleep(0.45)  # past the lease TTL, with no release
            allowed, _ = client.call("remove", name="stale.sess")
            assert allowed["result"]["removed"] is True


def test_touch_and_set_pin_update_records_only(server: Server, store_root: Path) -> None:
    (store_root / "pin.sess").write_bytes(b"p")
    with FdClient(server.socket_path) as client:
        client.hello()
        assert client.call("touch", name="pin.sess")[0]["result"]["touched"] is True
        assert client.call("touch", name="absent.sess")[0]["result"]["touched"] is False

        pinned, _ = client.call("set_pin", name="pin.sess", on=True)
        assert pinned["result"] == {"name": "pin.sess", "pinned": True}
        unpinned, _ = client.call("set_pin", name="pin.sess", on=False)
        assert unpinned["result"] == {"name": "pin.sess", "pinned": False}

        missing, _ = client.call("set_pin", name="absent.sess", on=True)
        assert missing["error"]["code"] == "miss"
        assert (store_root / "pin.sess").read_bytes() == b"p"  # nothing opened for writing


def test_the_name_rule_is_enforced(server: Server, store_root: Path) -> None:
    (store_root / "ok.sess").write_bytes(b"x")
    bad_names = ["", "../escape.sess", "a/b.sess", ".hidden.sess", "x" * 201, "nul\x00.sess"]
    with FdClient(server.socket_path) as client:
        client.hello()
        for name in bad_names:
            for op in ("lease_read", "stat", "stage_write"):
                reply, fd = client.call(op, name=name)
                assert fd is None, (op, name)
                assert reply["ok"] is False, (op, name)
                assert reply["error"]["code"] == "invalid", (op, name, reply)

        # 200 bytes is the limit and is accepted.
        longest = "y" * 200
        (store_root / longest).write_bytes(b"z")
        assert client.call("stat", name=longest)[0]["result"]["exists"] is True
        assert artifacts(store_root) == sorted(["ok.sess", longest])


def test_a_symlink_is_never_followed(server: Server, store_root: Path) -> None:
    secret = store_root.parent / "outside.txt"
    secret.write_bytes(b"outside")
    (store_root / "link.sess").symlink_to(secret)
    with FdClient(server.socket_path) as client:
        client.hello()
        reply, fd = client.call("lease_read", name="link.sess")
        assert fd is None
        assert reply["error"]["code"] == "invalid"
        assert "symlink" in reply["error"]["message"]
        listed, _ = client.call("list")
        assert listed["result"]["files"] == []  # a symlink is not an artifact


def test_list_and_stat_never_see_temporaries(server: Server, store_root: Path) -> None:
    (store_root / "kept.sess").write_bytes(b"123")
    with FdClient(server.socket_path) as client:
        client.hello()
        staged, fd = client.call("stage_write", name="pending.sess")
        os.close(fd)
        listed, _ = client.call("list")
        assert listed["result"]["files"] == [{"name": "kept.sess", "bytes": 3}]
        # The temporary's own name is outside the name rule, so it is unreachable.
        temp = f".pending.sess{store_v1.STAGE_INFIX}{staged['result']['token']}"
        assert client.call("stat", name=temp)[0]["error"]["code"] == "invalid"
        assert client.call("remove", name=temp)[0]["error"]["code"] == "invalid"
        assert (store_root / temp).exists()


def test_commit_with_an_unknown_token_is_invalid(server: Server) -> None:
    with FdClient(server.socket_path) as client:
        client.hello()
        reply, _ = client.call("commit_write", name="nothing.sess", token="deadbeef")
        assert reply["error"]["code"] == "invalid"


def test_malformed_and_deep_payloads_do_not_kill_the_server(server: Server) -> None:
    with FdClient(server.socket_path) as client:
        client.hello()
        client.send_raw(b"not json\n")
        broken, _ = client.read()
        assert broken["ok"] is False
        assert broken["error"]["code"] == "bad_json"

        client.send_raw(b"[" * 20000 + b"]" * 20000 + b"\n")
        deep, _ = client.read()
        assert deep["ok"] is False
        assert deep["error"]["code"] == "bad_json"

        assert client.call("list")[0]["ok"] is True


def test_store_operations_need_a_store(tmp_path: Path) -> None:
    with Server(free_socket_dir(tmp_path)) as running:
        with FdClient(running.socket_path) as client:
            client.hello()
            for op in ("lease_read", "stat", "list", "remove"):
                reply, fd = client.call(op, name="a.sess")
                assert fd is None
                assert reply["error"]["code"] == "not_implemented", op


def test_a_state_root_inside_a_protected_root_is_refused(tmp_path: Path) -> None:
    with pytest.raises(store_v1.StoreError, match="protected root"):
        store_v1.Store("/models/strata-cache").prepare()
    with pytest.raises(store_v1.StoreError, match="protected root"):
        store_v1.Store("/home/g0/strata-v100/cache").prepare()


def test_the_store_directory_is_created_private(tmp_path: Path) -> None:
    directory = tmp_path / "fresh"
    store_v1.Store(directory).prepare()
    assert directory.is_dir()
    assert (directory.stat().st_mode & 0o777) == store_v1.STORE_DIR_MODE
