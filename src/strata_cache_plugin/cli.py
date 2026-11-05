"""Command line: ``strata-cache-plugin {scan,dry-run,serve}``.

``scan`` and ``dry-run`` are read-only and never touch a file: ``scan`` refuses
the protected cache root unless the caller opts in, and even then it only
stats. ``serve`` writes, but only inside the directories it was given with
``--disk-dir`` and ``--slot-dir``, and only when a request names a file.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Sequence

from . import PROTOCOL_VERSION, __version__
from . import policy, scan, socket_server, store_v1

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_REFUSED = 2


class CliError(RuntimeError):
    """A problem the user can fix: bad flag, unreadable fixture."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="strata-cache-plugin",
        description=(
            "External cache plugin for Strata."
            " It reports what a cache directory holds, predicts what an eviction"
            " policy would do, and serves descriptors so the engine moves its own"
            " bytes. Artifact contents are never read or parsed here."
        ),
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser(
        "scan", help="list a cache directory and print aggregate sizes"
    )
    scan_parser.add_argument("--dir", required=True, help="directory to scan (stat only)")
    scan_parser.add_argument(
        "--max-depth", type=int, default=1, help="directory depth to walk (default 1)"
    )
    scan_parser.add_argument("--json", action="store_true", help="print JSON instead of a table")
    scan_parser.add_argument(
        "--allow-production-readonly",
        action="store_true",
        help="allow a protected root such as /models; stat only, still read-only",
    )
    scan_parser.set_defaults(handler=cmd_scan)

    dry_parser = subparsers.add_parser(
        "dry-run", help="predict evictions from a fixture descriptor list"
    )
    dry_parser.add_argument("--fixture", required=True, help="JSON array of entry descriptors")
    mode_group = dry_parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--delta",
        dest="mode",
        action="store_const",
        const=policy.MODE_LRU,
        help="delta mode on: LRU by last_used (default)",
    )
    mode_group.add_argument(
        "--fifo",
        dest="mode",
        action="store_const",
        const=policy.MODE_FIFO,
        help="delta mode off: first-in first-out by insertion order",
    )
    dry_parser.set_defaults(mode=policy.MODE_LRU)
    dry_parser.add_argument(
        "--budget-bytes", type=int, required=True, help="byte budget for the whole cache"
    )
    dry_parser.add_argument(
        "--slots", type=int, default=policy.DEFAULT_SLOT_CAP, help="entry-count cap"
    )
    dry_parser.add_argument(
        "--bytes-per-token",
        type=int,
        default=policy.DEFAULT_BYTES_PER_TOKEN,
        help="cost model: bytes of cache per token (default %(default)s)",
    )
    dry_parser.add_argument("--json", action="store_true", help="print JSON instead of a report")
    dry_parser.set_defaults(handler=cmd_dry_run)

    serve_parser = subparsers.add_parser(
        "serve", help="serve the wire protocol on a Unix socket"
    )
    serve_parser.add_argument("--socket", required=True, help="socket path; its dir must be 0700")
    serve_parser.add_argument(
        "--allow-uid",
        type=int,
        action="append",
        default=None,
        help="peer uid allowed to connect; repeatable, defaults to the server's own uid",
    )
    serve_parser.add_argument(
        "--max-request-bytes",
        type=int,
        default=socket_server.DEFAULT_MAX_REQUEST_BYTES,
        help="largest accepted request line (default %(default)s)",
    )
    serve_parser.add_argument(
        "--scan-max-depth", type=int, default=1, help="depth a scan request may walk"
    )
    serve_parser.add_argument(
        "--allow-production-readonly",
        action="store_true",
        help="allow scan requests on protected roots such as /models; stat only",
    )
    serve_parser.add_argument(
        "--disk-dir",
        default=None,
        help="directory holding the spill artifacts the engine leases and writes;"
        " without it the v1 store operations answer not_implemented",
    )
    serve_parser.add_argument(
        "--slot-dir",
        default=None,
        help="optional second directory (session files) the store also lists,"
        " stats and deletes from under the same pin/lease/grace rules; it is"
        " never written to",
    )
    serve_parser.add_argument(
        "--lease-ttl-s",
        type=float,
        default=store_v1.LEASE_TTL_S,
        help="lease lifetime in seconds, and the lifetime of a notification count"
        " (default %(default)s)",
    )
    serve_parser.add_argument(
        "--grace-s",
        type=float,
        default=store_v1.GRACE_S,
        help="how long an entry with no known lease is spared by eviction"
        " (default %(default)s)",
    )
    serve_parser.set_defaults(handler=cmd_serve)
    return parser


def cmd_scan(args: argparse.Namespace) -> int:
    if scan.is_protected_path(args.dir) and not args.allow_production_readonly:
        raise CliError(
            f"{scan.lexical_path(args.dir)} is a protected root;"
            " pass --allow-production-readonly to stat it (still read-only)"
        )
    if args.max_depth < 1:
        raise CliError("--max-depth must be >= 1")
    report = scan.scan_directory(args.dir, max_depth=args.max_depth)
    if args.json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    else:
        print(scan.format_report(report))
    return EXIT_OK


def cmd_dry_run(args: argparse.Namespace) -> int:
    if scan.is_protected_path(args.fixture):
        raise CliError(f"{scan.lexical_path(args.fixture)} is a protected root; refusing to read it")
    raw = load_fixture(args.fixture)
    decision = policy.evaluate_fixture(
        raw,
        mode=args.mode,
        budget_bytes=args.budget_bytes,
        slots=args.slots,
        bytes_per_token=args.bytes_per_token,
    )
    if args.json:
        print(json.dumps(decision.to_dict(), indent=2, ensure_ascii=False))
    else:
        print(policy.format_decision(decision))
    return EXIT_OK


def cmd_serve(args: argparse.Namespace) -> int:
    server = socket_server.SocketServer(
        args.socket,
        allow_production_readonly=args.allow_production_readonly,
        allowed_uids=set(args.allow_uid) if args.allow_uid else None,
        max_request_bytes=args.max_request_bytes,
        scan_max_depth=args.scan_max_depth,
        disk_dir=args.disk_dir,
        slot_dir=args.slot_dir,
        lease_ttl_s=args.lease_ttl_s,
        grace_s=args.grace_s,
    )
    store_note = (
        f" disk_dir={server.store.disk_dir} slot_dir={server.store.slot_dir}"
        f" grace_s={server.store.grace_s}"
        if server.store is not None
        else ""
    )
    print(
        f"starting strata-cache-plugin {__version__} protocol {PROTOCOL_VERSION}"
        f" on {server.socket_path} epoch={server.epoch}{store_note}",
        file=sys.stderr,
        flush=True,
    )
    server.run()
    return EXIT_OK


def load_fixture(path: str) -> Any:
    """Read a fixture descriptor file. The fixture is JSON, not a cache artifact."""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except OSError as exc:
        raise CliError(f"cannot read fixture {path}: {exc.strerror or exc}") from exc
    except json.JSONDecodeError as exc:
        raise CliError(f"fixture {path} is not valid JSON: {exc.msg}") from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except CliError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_REFUSED
    except scan.ScanError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except policy.PolicyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except socket_server.ServerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover - exercised via subprocess in tests
    sys.exit(main())
