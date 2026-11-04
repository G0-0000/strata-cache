# Wire protocol v0 (draft)

Status: draft for P1. The envelope, the owner epoch and the error model are
real; `lease_read` is reserved and **not used in P1**.

## Ground rules

- **Byte transfer is never part of this protocol.** Requests and replies name
  files and carry numbers. No cache artifact, snapshot, diff or lease payload
  ever crosses the socket, in v0 or in the operations reserved for later. A
  future operation that needs bytes describes a path and an offset; the bytes go
  directly between the engine and the filesystem.
- **One request per line.** Newline-delimited JSON, UTF-8, one compact JSON
  object per line in each direction. No length prefix, no batching, no
  framing beyond `\n`.
- **Transport**: `AF_UNIX`, `SOCK_STREAM`, at a path the operator chooses. The
  socket directory must be `0700`; the socket file is created `0600`. A socket
  path inside a protected root is refused before a directory or a socket is
  created there.
- **Peer check**: `SO_PEERCRED` on connect. A peer whose uid is not the server's
  own uid (`--allow-uid` can widen this) receives one `peer_rejected` reply and
  is disconnected.
- **Sequencing**: P1 serves one connection at a time, strictly serial. A client
  that connects and sends no newline blocks every other client; that is a known
  P1 limit, not a bug. Requests on one connection are answered in order.

## Envelopes

Request:

```json
{"id": "req-1", "op": "scan", "epoch": "9f3c1a7b2d4e6f80", "dir": "/tmp/x"}
```

Reply, success:

```json
{"id": "req-1", "ok": true, "result": {"...": "..."}}
```

Reply, failure:

```json
{"id": "req-1", "ok": false, "error": {"code": "scan_failed", "message": "cannot stat /tmp/x: No such file or directory"}}
```

Fields:

| Field   | Direction | Meaning |
|---------|-----------|---------|
| `id`    | both      | Opaque non-empty client string, echoed verbatim. Correlates reply to request. Required on every request, including `hello`. |
| `op`    | request   | Operation name, one of the table below. |
| `epoch` | request   | Owner epoch, required on every operation except `hello`. |
| `lease` | request   | Lease id, optional. Reserved; ignored in P1. |
| `ok`    | reply     | `true` on success, `false` on failure. Never both `result` and `error`. |
| `result`| reply     | Operation payload, see below. |
| `error` | reply     | `{"code": ..., "message": ...}`, see error codes. |

### Request id

Opaque, client-chosen, echoed unchanged. P1 does not require uniqueness, does not
persist them, and does not deduplicate retries: a resent request is executed
again. A reply whose `id` is `null` belongs to a request that could not be
parsed far enough to have an id.

### Owner epoch

The server generates an epoch (16 hex characters) at every start and returns it
from `hello`. A request carrying any other epoch is rejected with
`epoch_mismatch`; the client must `hello` again.

The epoch is the ownership token of the plugin's cache state: after a restart the
new server owns nothing of the old one, so a stale client cannot act on state it
only thought it owned. An epoch is never reused across starts.

### Lease id

An opaque string naming a `(session file, generation)` pair the client believes
it may read. In P1 leases are accepted and ignored: `lease_read` is reserved and
`release` is a no-op. When leases become real, a lease id will be needed to make
a read or a release idempotent; it will still never carry bytes.

## Operations

| op | In P1 | Purpose |
|----|-------|---------|
| `hello` | yes | Protocol version, server version, epoch, capability list. |
| `scan` | yes | Stat a directory and return aggregates plus entries. |
| `stats` | yes | Server counters and uptime. |
| `dry_run` | yes | Run the eviction policy on inline descriptors. |
| `lease_read` | **not used in P1** | Reserved: read a session artifact under a lease. Answers `not_implemented`. |
| `release` | yes, no-op | Release a lease. Answers `{"released": false, "note": "no-op in P1"}`. |

### hello

```json
{"id": "h", "op": "hello"}
```

```json
{"id": "h", "ok": true, "result": {
  "protocol": "v0", "version": "0.0.1", "epoch": "9f3c1a7b2d4e6f80",
  "pid": 4242, "uid": 1000, "socket": "/run/user/1000/strata-cache-plugin/cache.sock",
  "allow_production_readonly": false,
  "capabilities": ["scan", "stats", "dry_run", "release"],
  "not_used_in_p1": ["lease_read"]
}}
```

### scan

```json
{"id": "s", "op": "scan", "epoch": "...", "dir": "/tmp/cache", "max_depth": 1}
```

`dir` is required - the server has no default directory and will not guess. A
protected path (`/models`, and any `/home/g0/strata*` tree) is refused with
`refused_by_policy` unless the server was started with
`--allow-production-readonly`; even then the scan is `stat` only and opens
nothing. `max_depth` defaults to the server's `--scan-max-depth`, which is a
**cap**: a request asking for more than the cap is refused with
`refused_by_policy` rather than quietly walked as deeply as asked.

The result is the scanner report: `root`, `max_depth`, `entry_count`,
`file_count`, `directory_count`, `symlink_count`, `temporary_count`,
`unknown_count`, `total_bytes`, `unknown_bytes`, `bytes_by_suffix`,
`files_by_suffix`, `temporary_bytes_by_family`, `temporaries_by_family`, and
`entries` (each `name`, `rel_path`, `kind`, `label`, `size_bytes`, `mtime_ns`).

### stats

```json
{"id": "t", "op": "stats", "epoch": "..."}
```

Returns `protocol`, `version`, `epoch`, `pid`, `socket`, `started_at`,
`uptime_s`, `connections_total`, `requests_total`, `errors_total`,
`peer_rejections_total`, `requests_by_op`, `allow_production_readonly`.

### dry_run

```json
{"id": "d", "op": "dry_run", "epoch": "...", "mode": "lru",
 "budget_bytes": 400000000, "slots": 256,
 "entries": [{"name": "a", "tokens": 12000, "last_used": 4},
             {"name": "b", "tokens": 900, "last_used": 9, "pinned": true}]}
```

`entries`, `budget_bytes` required; `mode` is `lru` (delta mode on, default) or
`fifo` (delta mode off); `slots` defaults to 256; `bytes_per_token` defaults to
15260.

Descriptors travel **inline**. The daemon never reads a client-named file for a
dry run, so a request cannot turn into an arbitrary file read.

Result is the policy decision: `mode`, `budget_bytes`, `slots`,
`bytes_per_token`, `victims` (names, in eviction order), `reasons`
(name -> `over_budget`, `over_slots` or `over_budget+slots`), `kept`,
`written_then_deleted`, `kept_bytes`, `kept_count`, `warnings`.

### lease_read (not used in P1)

Reserved for the first version that may need to tell the engine which session
generation it is allowed to read. Every call in P1 answers:

```json
{"id": "l", "ok": false, "error": {"code": "not_implemented", "message": "lease_read is not used in P1: no snapshot bytes cross this socket"}}
```

### release

Accepts `lease_id` and answers success without touching anything:

```json
{"id": "r", "ok": true, "result": {"released": false, "lease_id": "L-1", "note": "no-op in P1"}}
```

## Error codes

| code | Meaning |
|------|---------|
| `bad_json` | The line is not UTF-8, is not parseable JSON, nests deeper than the interpreter will parse (`RecursionError`), or holds an integer literal past the 4300-digit conversion limit (`ValueError`). `id` is `null`. |
| `bad_request` | Decoded, but not an object, or `id`/`op` missing or of the wrong type. |
| `unknown_op` | `op` is not one of the operations above. |
| `epoch_mismatch` | `epoch` missing or not the current server's epoch. |
| `peer_rejected` | `SO_PEERCRED` uid is not allowed. One reply, then disconnect. |
| `refused_by_policy` | The request would touch a protected path, or asks for a scan depth above the server cap. |
| `scan_failed` | The scan could not complete: missing path, not a directory, unreadable entry, bad `max_depth`. |
| `policy_error` | The dry-run input is invalid: unknown field, duplicate name, bad number. |
| `not_implemented` | Reserved operation called too early (`lease_read`). |
| `request_too_large` | A single line exceeded `--max-request-bytes` (default 1 MiB), including a line whose newline arrives in the same read as the overflow, then disconnect. |
| `internal_error` | Anything unexpected. The connection stays open; the server stays up. |

An error never closes the connection, except `peer_rejected` and
`request_too_large`. A client that disconnects mid-line is simply dropped: the
partial line is discarded and the server keeps serving.

## Versioning

`hello` reports `protocol: "v0"` and a `capabilities` list. A consumer must
check the capability it needs rather than the version string alone. A breaking
change bumps `protocol`; additive operations can appear in v0 while listed under
`not_used_in_p1` and answering `not_implemented`.
