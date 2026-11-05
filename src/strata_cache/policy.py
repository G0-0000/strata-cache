"""Dry-run eviction policy for the Strata conversation cache.

The engine has two eviction orders and this module mirrors both:

* delta mode on  -> LRU, oldest ``last_used`` first (0 is oldest);
* delta mode off -> FIFO, insertion order (the fixture array order).

Both are filtered by the same rules: ordinary entries are evicted before prefix
entries, ``pinned`` entries are never evicted, and the result must fit a byte
budget and an entry-count cap.

Pure function: no filesystem, no clock, no randomness. The descriptor list is
the only input, and ``Decision`` is the only output.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

#: Entry-count cap, in slots. Matches the engine's conversation-cache slot count
#: at the time of writing; it is an input, not a measurement.
DEFAULT_SLOT_CAP = 256

#: Placeholder cost model, in bytes of cache per conversation token. The engine
#: measured roughly 15.26 KB per token for one conversation; P1 never measures,
#: it only accepts a number. Override with ``--bytes-per-token``.
DEFAULT_BYTES_PER_TOKEN = 15_260

MODE_LRU = "lru"
MODE_FIFO = "fifo"
MODES: tuple[str, ...] = (MODE_LRU, MODE_FIFO)

#: Reason recorded for a victim: which constraint was violated when it went.
REASON_BUDGET = "budget"
REASON_SLOTS = "slots"

#: Reason for an entry bigger than the whole budget: written, then dropped. It
#: never evicts anybody else.
REASON_WRITTEN_THEN_DELETED = "entry_larger_than_budget"

ALLOWED_FIELDS = frozenset({"name", "tokens", "last_used", "prefix", "pinned"})


class PolicyError(ValueError):
    """The fixture or the policy parameters are unusable."""


@dataclass(frozen=True)
class FixtureEntry:
    """One cache entry descriptor, straight from the fixture JSON."""

    name: str
    tokens: int
    last_used: int
    prefix: bool
    pinned: bool
    index: int  # position in the fixture, i.e. insertion order


@dataclass(frozen=True)
class Decision:
    """What the policy would do. Names only: P1 never touches the entries."""

    mode: str
    budget_bytes: int
    slots: int
    bytes_per_token: int
    victims: tuple[str, ...]
    reasons: Mapping[str, str]
    kept: tuple[str, ...]
    written_then_deleted: tuple[str, ...]
    kept_bytes: int
    kept_count: int
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "budget_bytes": self.budget_bytes,
            "slots": self.slots,
            "bytes_per_token": self.bytes_per_token,
            "victims": list(self.victims),
            "reasons": dict(self.reasons),
            "kept": list(self.kept),
            "written_then_deleted": list(self.written_then_deleted),
            "kept_bytes": self.kept_bytes,
            "kept_count": self.kept_count,
            "warnings": list(self.warnings),
        }


def entry_size(entry: FixtureEntry, bytes_per_token: int = DEFAULT_BYTES_PER_TOKEN) -> int:
    """Bytes this entry would occupy, from its token count."""
    return entry.tokens * bytes_per_token


def parse_fixture(raw: Iterable[Mapping[str, Any]] | Any) -> tuple[FixtureEntry, ...]:
    """Validate and normalise a fixture descriptor list.

    Every field is optional except ``name``; defaults are ``tokens=0``,
    ``last_used=0``, ``prefix=False``, ``pinned=False``. Unknown fields are an
    error, so a typo cannot silently change the outcome.
    """
    if isinstance(raw, (str, bytes, bytearray, Mapping)):
        raise PolicyError("fixture must be a JSON array of objects")
    try:
        items = list(raw)
    except TypeError as exc:
        raise PolicyError("fixture must be a JSON array of objects") from exc

    seen: set[str] = set()
    parsed: list[FixtureEntry] = []
    for index, item in enumerate(items):
        if not isinstance(item, Mapping):
            raise PolicyError(f"entry {index}: expected an object")
        unknown = set(item) - ALLOWED_FIELDS
        if unknown:
            raise PolicyError(f"entry {index}: unknown field(s) {sorted(unknown)}")
        name = item.get("name")
        if not isinstance(name, str) or not name:
            raise PolicyError(f"entry {index}: name must be a non-empty string")
        if name in seen:
            raise PolicyError(f"duplicate entry name: {name!r}")
        seen.add(name)
        parsed.append(
            FixtureEntry(
                name=name,
                tokens=_non_negative_int(item.get("tokens", 0), f"entry {name!r}: tokens"),
                last_used=_non_negative_int(
                    item.get("last_used", 0), f"entry {name!r}: last_used"
                ),
                prefix=_flag(item.get("prefix", False), f"entry {name!r}: prefix"),
                pinned=_flag(item.get("pinned", False), f"entry {name!r}: pinned"),
                index=index,
            )
        )
    return tuple(parsed)


def evaluate(
    entries: Sequence[FixtureEntry],
    *,
    mode: str = MODE_LRU,
    budget_bytes: int,
    slots: int = DEFAULT_SLOT_CAP,
    bytes_per_token: int = DEFAULT_BYTES_PER_TOKEN,
) -> Decision:
    """Decide which entries would be evicted, in eviction order."""
    if mode not in MODES:
        raise PolicyError(f"unknown mode {mode!r}; expected one of {list(MODES)}")
    _non_negative_int(budget_bytes, "budget_bytes")
    _non_negative_int(slots, "slots")
    _non_negative_int(bytes_per_token, "bytes_per_token")

    sizes = {entry.name: entry_size(entry, bytes_per_token) for entry in entries}
    pinned = [entry for entry in entries if entry.pinned]
    candidates = [entry for entry in entries if not entry.pinned]

    # An entry larger than the whole budget can never stay resident: it would be
    # written and immediately deleted. It must not push anybody else out.
    written_then_deleted = [
        entry for entry in candidates if sizes[entry.name] > budget_bytes
    ]
    pool = [entry for entry in candidates if sizes[entry.name] <= budget_bytes]

    queue = _eviction_order(pool, mode)
    kept = list(pool) + list(pinned)
    kept_bytes = sum(sizes[entry.name] for entry in kept)
    kept_count = len(kept)

    victims: list[str] = []
    reasons: dict[str, str] = {}
    while (kept_bytes > budget_bytes or kept_count > slots) and queue:
        violated = []
        if kept_bytes > budget_bytes:
            violated.append(REASON_BUDGET)
        if kept_count > slots:
            violated.append(REASON_SLOTS)
        victim = queue.pop(0)
        reasons[victim.name] = "over_" + "+".join(violated)
        victims.append(victim.name)
        kept.remove(victim)
        kept_bytes -= sizes[victim.name]
        kept_count -= 1

    warnings: list[str] = []
    if kept_bytes > budget_bytes:
        warnings.append(
            f"pinned entries alone occupy {kept_bytes} bytes, over the "
            f"{budget_bytes}-byte budget"
        )
    if kept_count > slots:
        warnings.append(
            f"pinned entries alone are {kept_count} entries, over the {slots}-slot cap"
        )

    kept_names = {entry.name for entry in kept}
    return Decision(
        mode=mode,
        budget_bytes=budget_bytes,
        slots=slots,
        bytes_per_token=bytes_per_token,
        victims=tuple(victims),
        reasons=reasons,
        kept=tuple(entry.name for entry in entries if entry.name in kept_names),
        written_then_deleted=tuple(entry.name for entry in written_then_deleted),
        kept_bytes=kept_bytes,
        kept_count=kept_count,
        warnings=tuple(warnings),
    )


def evaluate_fixture(
    raw: Iterable[Mapping[str, Any]] | Any,
    *,
    mode: str = MODE_LRU,
    budget_bytes: int,
    slots: int = DEFAULT_SLOT_CAP,
    bytes_per_token: int = DEFAULT_BYTES_PER_TOKEN,
) -> Decision:
    """``parse_fixture`` then ``evaluate``: the whole dry run in one call."""
    return evaluate(
        parse_fixture(raw),
        mode=mode,
        budget_bytes=budget_bytes,
        slots=slots,
        bytes_per_token=bytes_per_token,
    )


def _eviction_order(pool: Sequence[FixtureEntry], mode: str) -> list[FixtureEntry]:
    """Candidates in eviction order: ordinary first, then prefix, then by mode."""
    if mode == MODE_LRU:
        key = lambda entry: (entry.last_used, entry.index)  # noqa: E731
    elif mode == MODE_FIFO:
        key = lambda entry: (entry.index,)  # noqa: E731
    else:  # pragma: no cover - evaluate() rejects unknown modes first
        raise PolicyError(f"unknown mode {mode!r}")
    ordinary = sorted((entry for entry in pool if not entry.prefix), key=key)
    prefix = sorted((entry for entry in pool if entry.prefix), key=key)
    return ordinary + prefix


def format_decision(decision: Decision) -> str:
    """Human-readable dry-run report for the CLI."""
    lines: list[str] = [
        f"mode:          {decision.mode}",
        f"budget bytes:  {decision.budget_bytes}",
        f"slots:         {decision.slots}",
        f"bytes/token:   {decision.bytes_per_token}",
        "",
        f"kept ({decision.kept_count}): {decision.kept_bytes} bytes",
    ]
    lines.extend(f"  keep    {name}" for name in decision.kept)
    lines.append("")
    lines.append(f"evicted ({len(decision.victims)})")
    lines.extend(
        f"  drop    {name:<24}{decision.reasons.get(name, '')}"
        for name in decision.victims
    )
    if decision.written_then_deleted:
        lines.append("")
        lines.append(f"written then deleted ({len(decision.written_then_deleted)})")
        lines.extend(
            f"  cycle   {name:<24}{REASON_WRITTEN_THEN_DELETED}"
            for name in decision.written_then_deleted
        )
    for warning in decision.warnings:
        lines.append("")
        lines.append(f"warning: {warning}")
    return "\n".join(lines)


def _non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PolicyError(f"{field_name} must be an integer")
    if value < 0:
        raise PolicyError(f"{field_name} must be >= 0")
    return value


def _flag(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise PolicyError(f"{field_name} must be true or false")
    return value
