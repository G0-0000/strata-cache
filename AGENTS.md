# AGENTS.md — strata-cache

External cache plugin for Strata. It owns where cached conversations live and how long they stay,
while the engine keeps every byte of the snapshot format, identity and matching. The plugin never
reads or parses .sess/.meta/.dlog/.dchain contents.

## Hard boundaries

- **No byte transfer through this process.** The plugin names files and sends notifications;
  the engine reads and writes the files directly.
- **Never parse a snapshot.** Names, sizes and mtimes only.
- **Never delete a live or in-grace entry.** A lease (explicit or inferred) and a grace period
  (default 300 s) protect entries; eviction gives up rather than break the rule.
- **The store directory must not be inside a protected root** (/models, /home/g0/strata*).
  Tests use /var/tmp or another scratch path.
- **Slot directory is never written** by the plugin; it is listed, stat-ed and may be evicted
  under the same pin/lease/grace rules.

## Layout

- `src/strata_cache/` — `cli.py`, `scan.py`, `policy.py`, `socket_server.py`, `store_v1.py`
- `tests/` — pytest; scanner, policy, socket, notifications, store
- `systemd/` — example user unit, not enabled
- `PROTOCOL.md` — wire protocol v1 (requests with id get replies; notifications have no id and no reply)
- `GLOSSARY.md` — canonical cache terms; the copy in the Strata worktree must stay identical

## Develop and test

    python3 -m venv .venv && .venv/bin/pip install -e . pytest
    .venv/bin/python -m pytest -q

Never point a test at /models or the production cache. Use a temporary directory.

## Engine interface (the other side)

The Strata engine (worktree /home/g0/strata-v100) adds four notification methods to its cache store:
`lease`, `unlease`, `touch`, `set_pin`. It sends one non-blocking NDJSON line per event, expects no
reply, and may drop or tear a line. See PROTOCOL.md, "notification" section.

## Conventions

- Python standard library only at runtime; pytest for tests.
- Flat artifact names, at most 200 bytes, no slash, no leading dot.
- All opens use O_NOFOLLOW; the socket dir is 0700.
- Keep the engine side free of format knowledge: if a change needs snapshot fields, it belongs upstream.

## Known limits

- Same host, shared filesystem only; no remote backend.
- Notifications can be lost; the grace period is the safety net, not the lease alone.
- RAM historical cache and slot execution are not wired yet (P2b/P3 on the engine side).
