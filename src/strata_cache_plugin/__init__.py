"""Strata external cache plugin - P1 read-only shadow skeleton.

P1 scope, in one sentence: look at a Strata cache directory with ``stat`` only,
report what is there, and predict what an eviction policy would do, without
touching, parsing, moving or deleting a single byte.

See README.md for what P1 deliberately does not do and PROTOCOL.md for the
wire protocol draft.
"""

from __future__ import annotations

__version__ = "0.0.1"

#: Wire protocol version, bumped when a change breaks a v0 client.
PROTOCOL_VERSION = "v0"
