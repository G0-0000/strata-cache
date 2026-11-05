# Wire protocol v1

Status: v1 is the P2a protocol: requests, which carry an id and are answered, and
notifications, which carry none and are not. v0 (P1) is superseded for the
operations below; the envelope, epoch and error model carry over unchanged.

## Ground rules

- **Byte transfer is never part of this protocol.** Requests and replies name
  files and carry numbers. No cache artifact, snapshot, diff or lease payload
  ever crosses the socket. A descriptor handed over with SCM_RIGHTS is a
  handle to a file the plugin opened; the bytes go between the engine and the
  filesystem.
- **One object per line, with or without an id.** Newline-delimited JSON,
  UTF-8. A line with an "id" is a request and gets exactly one reply; a line
  without one is a notification, applied and never answered. Both share one
  connection and one loop.
- **A partial last line is discarded.** A connection that ends mid-line leaves
  half a line behind; the plugin drops it rather than applying half a
  notification.
- **Transport**: AF_UNIX, SOCK_STREAM, socket directory 0700, socket file
  0600. A socket path inside a protected root is refused before creation.
- **Peer check**: SO_PEERCRED; a foreign uid gets one peer_rejected reply
  and is disconnected.
- **Sequencing**: one connection at a time, strictly serial. Replies are in
  request order. A hung client blocks others; the engine must timeout and
  reconnect, which is why every call has a deadline.
- **Descriptor rule**: at most one fd per reply, attached to the same
  sendmsg as the JSON line. A reply that carries one sets "fd": true in the
  result. A reply without it must never carry an fd.
- **Name rule**: artifact names are flat basenames, non-empty, at most 200
  bytes, no slash, no leading dot, no NUL. Names outside this rule are
  invalid.

## Envelope

Request:

    {"id":"7","op":"lease_read","epoch":"9f3c1a7b2d4e6f80","name":"strata-conv-3.sess"}

Success:

    {"id":"7","ok":true,"result":{"name":"strata-conv-3.sess","bytes":289669120,"lease":"L-4f2a","fd":true}}

Failure:

    {"id":"7","ok":false,"error":{"code":"miss","message":"no such artifact"}}

Error codes: miss, invalid, io, memory, busy, epoch_mismatch,
not_implemented, bad_request.

Notification (no id, no epoch, no reply - see below):

    {"op":"lease","name":"strata-conv-3.sess"}

## Operations

| op | Request fields | Reply result | fd |
|----|----------------|--------------|----|
| hello | protocol, engine, instance | protocol, version, epoch, disk_dir, slot_dir, grace_s, features, notifications | no |
| lease_read | name | name, bytes, lease | yes, read-only |
| release | lease | released (bool) | no |
| stage_write | name | name, token | yes, write |
| stage_append | name, committed | name, token | yes, read-write |
| commit_write | name, token, bytes | published (bool) | no |
| abort_write | name, token | aborted (bool) | no |
| touch | name | touched (bool) | no |
| set_pin | name, on | pinned (bool) | no |
| remove | name | removed (bool) | no |
| stat | name | exists (bool), bytes | no |
| list | none | files: [{name, bytes}] | no |
| evict | budget_bytes | removed, freed_bytes, bytes, budget_bytes, grace_s, skipped | no |
| stats | none | counters | no |

## Notifications

A notification says what the engine is doing with one entry. It is
fire-and-forget: no id, no epoch, no reply, and the engine never reads the
socket back. One non-blocking `send()` puts a whole line on the wire, or the
line is dropped.

    {"op":"lease","name":"strata-conv-3.sess"}
    {"op":"unlease","name":"strata-conv-3.sess"}
    {"op":"touch","name":"strata-conv-3.sess"}
    {"op":"set_pin","name":"strata-conv-3.sess","on":true}

| op | what the plugin records |
|----|-------------------------|
| lease | one more holder of the name |
| unlease | one holder fewer; the name is free once the count reaches zero |
| touch | the name was just used |
| set_pin | pinned (`on:true`) or no longer pinned (`on:false`) |

- **Never answered.** Nothing follows a notification, not even an error. A
  request sent without an id is therefore indistinguishable from a
  notification: it is applied as one, or ignored if its op is not in the table
  above. A client that wants an answer sends an id.
- **Droppable.** A line the engine's socket will not take whole is dropped, not
  retried, and never carried to a new connection. A line taken only in part
  ends that stream for the run, which leaves half a line as the last thing the
  plugin has to discard.
- **No epoch.** A notification is never refused for a stale owner. It adds or
  refreshes a record, and a record a stale engine created lapses on its TTL.
- **Never read as absence.** The plugin only ever adds state here and lets it
  expire. A missing unlease does not prove the engine is done with an entry;
  that is what the grace period is for. The lease count carries the wire lease
  TTL (120 s), and lease, unlease and touch each restart it, so a dropped
  unlease costs retention, never a live read.
- **Lease, unlease and touch are activity; a pin is not.** Each of the three
  restarts the entry's grace period (it is evidence the engine is working with
  the name now). set_pin only records what to keep, so it does not make an old
  entry look fresh.
- **Names follow the name rule.** A notification naming anything else - a
  slash, a leading dot, more than 200 bytes, a NUL - is ignored and counted.
- **A pin may precede the file.** `set_pin` records the pin whether or not the
  entry exists, because a notification cannot be asked to try again. The
  request form of `set_pin` on a missing name still answers `miss`.
- **Ignoring is silent by construction.** A notification with no name, an
  unknown op, or a non-boolean `on` is counted and dropped. `stats` reports
  `notifications_total`, `notifications_ignored`, `notifications_by_op` and
  `notified_leases_live`.

## Leases

- A lease is created by lease_read and names one artifact for one client.
- TTL 120 s, refreshed by lease_read on the same name and by touch.
- While any lease on a name is live, remove refuses with busy and evict skips
  the name. A live notification count protects a name the same way.
- A wire lease and a notification count are separate records over the same
  name: the request form of lease_read opens the file and answers with a lease
  id, the notification form only counts. Nothing reconciles the two, because
  the wire form is what a client asks for and the notification is what the
  engine's own store reports.
- release is idempotent: releasing an unknown or expired lease answers
  released:false, never an error.
- lease_read opens with O_RDONLY|O_NOFOLLOW|O_CLOEXEC, checks a regular
  file, and duplicates nothing: the descriptor sent is the one opened. The
  plugin closes its copy after the reply is sent.

## Writes

- Writes land in --disk-dir only. A name that exists only in --slot-dir is a
  miss for stage_append, so the plugin never leaves a disk twin beside a
  session file.
- stage_write creates .<name>.stage-<token> in the store directory with
  O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC, mode 0600, and returns its
  descriptor. The file does not exist under name yet.
- stage_append opens the existing name read-write with O_NOFOLLOW,
  truncates to committed, and returns the descriptor. The caller is
  responsible for the data fsync.
- commit_write for a staged write renames the temporary over name and
  flushes the directory; published:false means the rename happened but the
  directory flush failed, and the caller must treat the name as published
  but not durable. For an append it only flushes the directory.
- abort_write removes a staged temporary. It never removes name.
- A second stage_write for the same name while one is pending discards the
  previous temporary (aborted:true for the old token).

## Index queries

- stat answers whether a name exists and its size, without opening it.
- list returns every regular artifact the plugin holds in either owned
  directory (names it will serve), with sizes. Temporaries are never listed,
  and a name present in both directories is listed once.

## Eviction

    {"id":"9","op":"evict","epoch":"9f3c1a7b2d4e6f80","budget_bytes":42949672960}

evict deletes least-recently-used artifacts until the store fits budget_bytes.
It is the one place the plugin deletes without being told which name to delete,
so the grace period is its safety argument:

- A notification may have been dropped, so "no live lease" means "the plugin
  does not know", not "the engine is done with it".
- An entry with no live lease is still spared while its last_used is younger
  than grace_s (default 300 s, --grace-s). last_used is the plugin's own
  record, or the file's mtime when the plugin has no record of that name: a
  restart cannot hand every entry to the eviction at once.
- A pinned entry, an entry a live wire lease or a live notification count
  holds, and an entry inside the grace period are never removed. Eviction gives
  up on the budget before it breaks one of those rules, which is why the reply
  reports the byte count it did not reach.
- Only regular files obeying the name rule are candidates. The path deleted is
  the path the store would serve, built from a validated name, so a staged
  temporary and a stray dot file are out of reach.

The reply is removed (names, in the order deleted), freed_bytes, bytes (left
after the pass), budget_bytes, grace_s and skipped (counts of pinned, leased and
in_grace).

## State

- The plugin owns two directories: --disk-dir holds the spill artifacts,
  --slot-dir (optional) holds session files. It lists, stats, removes and
  tracks names in both; it writes only in the disk directory and parses
  neither. A name present in both resolves to the disk directory.
- It also owns the lease table, the notification counts, the write-token table,
  the pin set, one last_used record per name, and counters. All of it lives in
  memory and dies with the process; last_used falls back to the file's mtime.
- It does not parse artifact contents and does not decide identity or matching.
- Two operations delete an artifact: remove, which names it, and evict, which
  respects the grace period. Nothing else does, and no other file is deleted
  except a staged temporary the plugin created itself.

## Handshake and versioning

hello must be the first request. protocol is an integer; this document is
protocol 1. A mismatch is refused with not_implemented and the connection is
closed. epoch is generated at start and never reused; every request except
hello must carry the current epoch. A notification may arrive before hello and
between requests: it carries no epoch, so there is nothing for it to be refused
for.
