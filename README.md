# strata-cache-plugin

An external cache plugin for Strata, grown outwards from the engine. This
repository holds **P1, a read-only shadow**, and **v1, the store service**: the
operations the engine calls to lease an artifact, stage a write and publish it,
plus the notifications it sends to say what it is doing with an entry.

The plugin's eventual job is to manage three Strata cache layers - the disk
spill, the in-RAM history and the session files - **without snapshot bytes ever
passing through the plugin process**. P1 proves the shape (a process skeleton, a
scanner, a dry-run policy engine, a wire protocol); v1 adds the store that hands
the engine descriptors instead of bytes, and the notification mode that keeps
the store's idea of who is using what.

The plugin never parses an artifact. A `.sess`, `.meta`, `.dlog`, `.dchain` or
`.tokens.json` file is a name and a size here; the bytes belong to the engine
and go straight between the engine and the filesystem.

## Vocabulary

The cache terms used by this project are defined in [GLOSSARY.md](GLOSSARY.md).
The canonical copy lives in the Strata worktree at personal/docs/GLOSSARY.md; keep the two in step.

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

## What v1 adds

Given `--disk-dir DIR`, `serve` also answers the v1 store operations. That
directory is the plugin's own: it holds the artifacts the engine leases and
writes, and it is refused when it lies inside a protected root. `--slot-dir`
optionally names a second directory of session files the store lists, stats and
removes from but never writes to.

| op | what it does | descriptor |
|----|--------------|------------|
| `hello` | protocol, version, epoch, features | no |
| `lease_read` | open a regular artifact read-only, lease it for 120 s | yes |
| `release` | drop a lease; idempotent | no |
| `stage_write` | create `.<name>.stage-<token>`, mode 0600 | yes |
| `stage_append` | open the artifact read-write, truncated to `committed` | yes |
| `commit_write` | rename the temporary over `name`, flush the directory | no |
| `abort_write` | unlink the temporary; never touches `name` | no |
| `touch` | refresh the leases on a name | no |
| `set_pin` | record or clear a pin | no |
| `remove` | delete a name, refused with `busy` while a live lease or count holds it | no |
| `stat` | does the name exist, and how big is it | no |
| `list` | every regular artifact, with sizes; temporaries never listed | no |
| `evict` | delete least-recently-used artifacts down to `budget_bytes`, inside the grace period | no |

The descriptor travels with the reply in one `sendmsg`. The plugin closes its
own copy immediately after the send; the engine reads or writes the file itself.

## Notification mode

A line without an `id` is a notification. The engine sends five, one
non-blocking `send()` each, and reads nothing back:

    {"op":"lease","name":"strata-conv-3.sess"}
    {"op":"unlease","name":"strata-conv-3.sess"}
    {"op":"touch","name":"strata-conv-3.sess"}
    {"op":"set_pin","name":"strata-conv-3.sess","on":true}
    {"op":"remove","name":"strata-conv-3.sess"}

The plugin applies each one - `lease`/`unlease` keep a reference count per
name with the lease TTL (120 s), `touch` records that the name was just used,
`set_pin` records or clears the pin - and never writes a reply, not even an
error. Lease, unlease and touch are activity and restart the entry's grace
period; a pin is only a retention instruction and does not. `remove` is the
engine saying it deleted the name, so the plugin drops that name's count, pin
and last_used record; a name it never heard of is applied all the same.

Requests with an `id` keep the request/reply behaviour, on the same connection
and in the same serial loop. A notification that is malformed, that names
something outside the name rule, or that arrives at a server started without
`--disk-dir` is counted (`notifications_ignored`, by op in
`notifications_ignored_by_op`) and has no reply; the first one of the run is
written to stderr, since its sender cannot be told. A line left half-written
when a connection ends is discarded rather than applied.

**Notifications may be dropped.** The engine does not retry: a line the socket
will not take whole is gone, so a missing `unlease` or a missing `lease` is a
fact of the deployment, not an error. That is why the store never treats "no
notification" as proof that an entry is unused, and why eviction has a grace
period.

## Retention and the grace period

State lives in memory and dies with the process: the lease table, the
notification counts, the write tokens, the pins and the last_used records are
rebuilt empty after a restart. The plugin deletes a staged temporary on its own
initiative, and it deletes an artifact on two paths only: an explicit `remove`
request, and `evict`.

`evict` deletes least-recently-used artifacts until the store fits
`budget_bytes`, and it never removes:

- a name a live lease holds, or a name a live notification count holds;
- a pinned name;
- **a name whose lease is unknown but whose last_used is younger than the grace
  period** - default 300 s, `--grace-s`. last_used is the plugin's own record,
  or the file's mtime when the plugin has no record of that name, so a restart
  does not make every entry look ancient.

The grace period is the answer to a dropped notification: an entry the plugin
has heard nothing about may still be in the middle of a read, so it has to age
before eviction may touch it. Eviction gives up on the budget rather than break
one of those rules, and the reply says how much it left behind (`bytes`,
`skipped`).

## What the plugin explicitly does not do

- It does not parse `.sess`, `.meta`, `.dlog`, `.dchain` or `.tokens.json`.
  They are opaque names and sizes, in P1 and in v1 alike.
- P1 never writes, renames, deletes, truncates or chmods anything inside a cache
  directory; temporary files are reported and left alone. v1 writes, but only
  inside its own `--disk-dir` and only when a request names a file:
  `stage_write`, `stage_append`, `commit_write`, `abort_write`, `remove` and
  `evict`. The slot directory is never written to - no write, no temporary, no
  rename lands there - but its entries are listed and can be deleted by
  `remove` or `evict` under the same pin, lease and grace rules as the disk
  directory.
- It does not read snapshot bytes, over the socket or anywhere else. Byte
  transfer is not part of the protocol: a descriptor is a handle, and the bytes
  move between the engine and the filesystem without entering this process.
- It does not touch `/models` or any Strata checkout. Protected paths are
  `/models` (that root and everything below it) and `/home/g0/strata*` (every
  `strata` worktree, patch tree and the plugin's own symlink). Refused for
  `scan --dir` (unless `--allow-production-readonly`, and even then stat only),
  for `dry-run --fixture`, and for `--socket`, where a 0700 directory inside a
  protected root would otherwise accept a socket write. The check is lexical: it
  never resolves symlinks, because resolving a protected root would itself be a
  filesystem call on it.
- It holds no lock and shares no state with the running engine. A lease is the
  plugin's own record, in memory, that keeps `remove` and `evict` away from a
  name; the engine's notifications are the only thing it knows about a running
  request, and they are best effort.
- It does not install or enable a systemd unit. `systemd/*.service` is a
  documented example with placeholder paths.

## Layout

```
src/strata_cache_plugin/scan.py           directory scanner (stat only)
src/strata_cache_plugin/policy.py         dry-run eviction policy (pure)
src/strata_cache_plugin/store_v1.py       leases, notifications, staged writes, eviction
src/strata_cache_plugin/socket_server.py  Unix socket server
src/strata_cache_plugin/cli.py            scan / dry-run / serve
tests/                                     scanner, policy, socket, store, notification and kill tests
systemd/strata-cache-plugin.service        user unit, placeholder ExecStart
PROTOCOL.md                                wire protocol v1
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
`dry-run` is the predictor, over a fixture, with a placeholder cost model;
`evict` over the socket is the real deletion path and uses the sizes on disk
(see [Retention and the grace period](#retention-and-the-grace-period)).

### serve

```sh
strata-cache-plugin serve --socket /run/user/$(id -u)/strata-cache-plugin/cache.sock
strata-cache-plugin serve --socket ... --disk-dir /var/lib/strata-cache \
    --slot-dir /var/lib/strata-slots --lease-ttl-s 120 --grace-s 300
```

The socket directory must be private (`0700`). The server creates it if it is
missing; it refuses to chmod a directory it did not create, so pointing
`--socket` at `/tmp` fails loudly instead of quietly locking down `/tmp`. A
socket path inside a protected root is refused before anything is created.

`--disk-dir` names the artifact directory. It is created (mode `0700`) when
missing, refused when it is a symlink, and refused when it lies inside a
protected root. Without it the server still scans, dry-runs and reports stats,
but the v1 store operations answer `not_implemented` and every notification is
counted and dropped.

`--slot-dir` names a second directory the store also lists, stats and deletes
from - session files, which the plugin never writes and never parses. Its
entries are evicted under the same pin, lease and grace rules as the disk
directory; what the plugin never does is write there. It is prepared the same
way as `--disk-dir`. A name that exists in both resolves to `--disk-dir`.

`--lease-ttl-s` sets the lease lifetime (default 120 s) and the lifetime of a
notification count. `--grace-s` sets how long an entry with no known lease is
still spared by `evict` (default 300 s).

Requests must carry the owner epoch returned by `hello`; a notification carries
none and is never refused for one. `scan` requests need an
explicit `dir`, refused for a protected root unless the server was started with
`--allow-production-readonly`; a requested `max_depth` above the server's
`--scan-max-depth` is an error, not a silent deep scan.

Store requests carry `"name"`: a flat basename, at most 200 bytes, no slash, no
leading dot, no NUL. Anything else is `invalid`, which is also how the staged
temporaries stay unreachable - their names begin with a dot.

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

The v1 tests run a real server on a temporary socket and pass descriptors for
real: a lease descriptor is read back byte for byte, a staged write is written
through and published, an append is truncated to `committed` and extended, an
abort leaves neither temporary nor name, `remove` is refused with `busy` while
a lease is live and succeeds once it expires (short `--lease-ttl-s`), a second
`stage_write` aborts the previous temporary, the name rule and the symlink
refusal hold, `list` never shows a temporary, and a malformed or 20000-deep
payload leaves the server answering.

The notification tests assert on what a connection received and on what the
store then believes: a notification is applied and the connection stays silent
while an `id` line still gets exactly one reply; requests and notifications
interleave on one connection; a half-written line at end of file is discarded
(only the whole line is applied); a `lease` count survives one `unlease` and
not two; a count nobody refreshed lapses after the TTL; `touch` restarts the
grace period; a pin holds an entry through `evict`; a dropped notification -
one that is simply never sent - still leaves the entry inside the grace period;
an old entry outside it is evicted while a fresh one is kept; ignored
notifications are counted, the first one logged and all harmless; notifications
to a server without `--disk-dir` are ignored; a `remove` notification drops a
name's counts, pin and last_used; and `--slot-dir` is listed and evictable
while an append to a slot-only name is a miss.

## Limits, on purpose

- Serial by design: one connection is served at a time, so a client that
  connects and sends nothing blocks the queue until it is killed - and blocks
  the notifications of every other engine connection with it. The engine holds
  its connection for the whole run, so with the engine attached the plugin
  serves that connection: an operator probe waits until the engine exits.
- Notifications are best effort, in both directions: a busy plugin makes the
  engine drop lines, and a plugin that restarts loses the counts it had. The
  grace period bounds the damage; it does not remove it.
- `evict` can stop short of its budget when pins, live leases or the grace
  period cover the rest of the store. It reports that rather than breaking a
  rule.
- The protected-root check is lexical, so a symlink pointing into `/models` is
  not caught. P1 only scans directories the operator names.
- The cost model is a constant, not a measurement.
- `--fifo` order does not model the engine's skip of complete entries that
  already have a chain, so its victims can differ; see the dry-run section.
