"""Strata external cache plugin.

The plugin holds a cache directory for the engine and hands it descriptors: it
never carries artifact bytes and never parses artifact contents. P1 was a
read-only shadow (scan, dry-run policy); v1 adds the store operations that let
the engine lease, stage and publish artifacts over the socket.

See README.md for the scope and its limits, PROTOCOL.md for the wire protocol.
"""

from __future__ import annotations

__version__ = "0.1.0"

#: Wire protocol version, bumped when a change breaks a v1 client.
PROTOCOL_VERSION = 1
