# strata-cache-plugin

An external cache plugin for Strata, grown outwards from the engine. This
repository holds **P1: a read-only shadow**, deliberately narrow.

The plugin's eventual job is to manage three Strata cache layers - the disk
spill, the in-RAM history and the session files - **without snapshot bytes ever
passing through the plugin process**. P1 only proves the shape: a process
skeleton, a scanner, a dry-run policy engine and a wire protocol.

## What P1 does

- `scan` walks a cache directory and reports it: bytes per artifact suffix,
  entry counts, the three temporary-name families, unknown files. Depth 1 by
  default, recursion available with `--max-depth`.
- `dry-run` reads a fixture of entry descriptors and reports which entries an
  eviction policy would drop, in order, with a reason for each.
- `serve` exposes the same work over a Unix socket with newline-delimited JSON
  (see [PROTOCOL.md](PROTOCOL.md)).

Every filesystem call in `scan` is `os.scandir`, `lstat`, or one `os.stat` of
the directory the caller named. No file is ever opened.

Precisely what is followed:

- the **root argument** is opened with `os.stat`, so a symlink given as `--dir`
  is followed to the directory it points at;
- every **entry inside** it is `lstat`-ed with `follow_symlinks=False`, so
  symlinks are reported as symlinks and never followed;
- nothing is resolved on the way to a protected-root decision: that check is
  lexical, see below.

## What P1 explicitly does not do

- It does not parse `.sess`, `.meta`, `.dlog`, `.dchain` or `.tokens.json`.
  They are opaque names and sizes.
- It does not write, rename, delete, truncate or chmod anything inside a cache
  directory. Temporary files are reported and left alone.
- It does not read snapshot bytes, over the socket or anywhere else. Byte
  transfer is not part of the protocol, not even as a future option in v0.
- It does not touch `/models` or any Strata checkout. Protected paths are
  `/models` (that root and everything below it) and `/home/g0/strata*` (every
  `strata` worktree, patch tree and the plugin's own symlink). Refused for
  `scan --dir` (unless `--allow-production-readonly`, and even then stat only),
  for `dry-run --fixture`, and for `--socket`, where a 0700 directory inside a
  protected root would otherwise accept a socket write. The check is lexical: it
  never resolves symlinks, because resolving a protected root would itself be a
  filesystem call on it.
- It does not talk to the running engine, take a lease, or hold a lock.
- It does not install or enable a systemd unit. `systemd/*.service` is a
  documented example with placeholder paths.

## Layout

```
src/strata_cache_plugin/scan.py           directory scanner (stat only)
src/strata_cache_plugin/policy.py         dry-run eviction policy (pure)
src/strata_cache_plugin/socket_server.py  Unix socket server
src/strata_cache_plugin/cli.py            scan / dry-run / serve
tests/                                     scanner, policy, socket and kill tests
systemd/strata-cache-plugin.service        user unit, placeholder ExecStart
PROTOCOL.md                                wire protocol v0 draft
```

## Install and run

Python 3.12, standard library only. No third-party runtime dependency; `pytest`
is used for the tests.

```sh
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/strata-cache-plugin --help
```

### scan

```sh
strata-cache-plugin scan --dir /path/to/cache          # aggregate table
strata-cache-plugin scan --dir /path/to/cache --json   # aggregates plus every entry
strata-cache-plugin scan --dir /path/to/cache --max-depth 2
```

The protected roots (`/models`, `/home/g0/strata*`) are refused without
`--allow-production-readonly`; even then the scan is stat only.

### dry-run

The fixture is a JSON array of descriptors; `name` is the only required field:

```json
[
  {"name": "conv-a", "tokens": 12000, "last_used": 4},
  {"name": "conv-b", "tokens": 30000, "last_used": 1, "prefix": true},
  {"name": "conv-c", "tokens": 900,   "last_used": 9, "pinned": true}
]
```

```sh
strata-cache-plugin dry-run --fixture entries.json --budget-bytes 400000000
strata-cache-plugin dry-run --fixture entries.json --budget-bytes 400000000 --fifo
strata-cache-plugin dry-run --fixture entries.json --budget-bytes 400000000 --slots 12 --json
```

Rules, mirroring the engine:

- delta mode on (`--delta`, the default): LRU, oldest `last_used` first.
- delta mode off (`--fifo`): first-in first-out, by insertion order.
- ordinary entries before prefix entries, in both modes;
- `pinned` is never evicted;
- one entry bigger than the whole budget is reported as **written then deleted**
  and never evicts anybody else;
- entry size is `tokens x --bytes-per-token` (default 15260, a placeholder cost
  model; P1 does not measure).

Where this does not match the engine: the engine's spill pass skips entries that
are complete and already have a chain (`conversation_spill.cpp:1317-1318`), so
its FIFO order starts from a filtered list while P1's starts from the fixture as
given. A `--fifo` victim list can therefore differ from the engine's. Delta mode
(LRU) has no such filter.

`--budget-bytes` is required: the tool will not invent a budget for you.

### serve

```sh
strata-cache-plugin serve --socket /run/user/$(id -u)/strata-cache-plugin/cache.sock
```

The socket directory must be private (`0700`). The server creates it if it is
missing; it refuses to chmod a directory it did not create, so pointing
`--socket` at `/tmp` fails loudly instead of quietly locking down `/tmp`. A
socket path inside a protected root is refused before anything is created.

Requests must carry the owner epoch returned by `hello`. `lease_read` answers
`not_implemented` in P1. `scan` requests need an explicit `dir`, refused for a
protected root unless the server was started with
`--allow-production-readonly`; a requested `max_depth` above the server's
`--scan-max-depth` is an error, not a silent deep scan.

## Tests

```sh
python3 -m pytest            # from the repository root
```

The suite covers: classification and aggregates, a full `(name, size, mtime,
mode)` manifest compared before and after a scan (nothing may change, including
in a directory holding a live socket), a scan run with `builtins.open` patched
to raise, LRU against FIFO ordering, prefix last, pinned kept, single oversize
entry, the slot cap, a zero byte budget and zero slots, the epoch guard,
`SO_PEERCRED` rejection, malformed JSON that would otherwise raise
`RecursionError` or `ValueError`, an oversized line crossing a read boundary,
serial handling of a second connection, a client that disconnects mid-line,
`SIGKILL` followed by a restart on the same socket, and a hung client followed
by a clean `SIGTERM`.

## P1 limits, on purpose

- Serial by design: one connection is served at a time, so a client that
  connects and sends nothing blocks the queue until it is killed.
- The protected-root check is lexical, so a symlink pointing into `/models` is
  not caught. P1 only scans directories the operator names.
- The cost model is a constant, not a measurement.
- `--fifo` order does not model the engine's skip of complete entries that
  already have a chain, so its victims can differ; see the dry-run section.
