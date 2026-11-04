"""Policy tests: the two eviction orders, the common rules, and purity."""

from __future__ import annotations

import pytest

from strata_cache_plugin import policy

BPT = 100  # bytes per token in every test below: tokens == hundreds of bytes


def entry(name: str, tokens: int, last_used: int = 0, *, prefix: bool = False,
          pinned: bool = False) -> dict[str, object]:
    return {
        "name": name,
        "tokens": tokens,
        "last_used": last_used,
        "prefix": prefix,
        "pinned": pinned,
    }


def run(entries: list[dict[str, object]], **kwargs: object) -> policy.Decision:
    options: dict[str, object] = {
        "mode": policy.MODE_LRU,
        "budget_bytes": 1_000_000,
        "slots": policy.DEFAULT_SLOT_CAP,
        "bytes_per_token": BPT,
    }
    options.update(kwargs)
    return policy.evaluate_fixture(entries, **options)


def test_lru_evicts_the_oldest_last_used_first() -> None:
    entries = [entry("a", 10, 3), entry("b", 10, 1), entry("c", 10, 2)]
    decision = run(entries, budget_bytes=2000)
    assert decision.victims == ("b",)
    assert decision.reasons == {"b": "over_budget"}
    assert decision.kept == ("a", "c")
    assert decision.kept_bytes == 2000
    assert decision.kept_count == 2


def test_fifo_evicts_by_insertion_order_whatever_last_used_says() -> None:
    entries = [entry("a", 10, 3), entry("b", 10, 1), entry("c", 10, 2)]
    decision = run(entries, mode=policy.MODE_FIFO, budget_bytes=2000)
    assert decision.victims == ("a",)
    assert decision.kept == ("b", "c")


def test_fifo_ignores_last_used_and_lru_does_not() -> None:
    entries = [entry("first", 10, 0), entry("second", 10, 9)]
    assert run(entries, budget_bytes=1000).victims == ("first",)
    assert run(entries, mode=policy.MODE_FIFO, budget_bytes=1000).victims == ("first",)
    swapped = [entry("first", 10, 9), entry("second", 10, 0)]
    assert run(swapped, budget_bytes=1000).victims == ("second",)
    assert run(swapped, mode=policy.MODE_FIFO, budget_bytes=1000).victims == ("first",)


def test_ordinary_entries_are_evicted_before_prefix_entries() -> None:
    entries = [
        entry("prefix-old", 10, 0, prefix=True),
        entry("ordinary-new", 10, 9),
        entry("ordinary-old", 10, 5),
    ]
    decision = run(entries, budget_bytes=2000)
    assert decision.victims == ("ordinary-old",)
    assert set(decision.kept) == {"prefix-old", "ordinary-new"}


def test_prefix_entries_go_last_when_everything_must_go() -> None:
    entries = [
        entry("plain-a", 10, 0),
        entry("prefix-a", 10, 0, prefix=True),
        entry("plain-b", 10, 1),
        entry("prefix-b", 10, 1, prefix=True),
    ]
    decision = run(entries, budget_bytes=1000)
    assert decision.victims == ("plain-a", "plain-b", "prefix-a")
    assert decision.kept == ("prefix-b",)


def test_pinned_is_never_evicted() -> None:
    entries = [entry("a", 10, 0), entry("pin", 10, 0, pinned=True), entry("c", 10, 9)]
    decision = run(entries, budget_bytes=1000)
    assert decision.victims == ("a", "c")
    assert decision.kept == ("pin",)
    assert decision.kept_bytes == 1000


def test_pinned_only_over_budget_produces_a_warning() -> None:
    entries = [entry("pin-1", 10, pinned=True), entry("pin-2", 10, pinned=True)]
    decision = run(entries, budget_bytes=1500)
    assert decision.victims == ()
    assert decision.kept == ("pin-1", "pin-2")
    assert decision.kept_bytes == 2000
    assert any("pinned entries alone" in warning for warning in decision.warnings)


def test_one_entry_larger_than_the_budget_is_written_then_deleted() -> None:
    entries = [entry("small", 5, 0), entry("huge", 50, 9)]
    decision = run(entries, budget_bytes=1000)
    assert decision.written_then_deleted == ("huge",)
    assert decision.victims == ()
    assert decision.kept == ("small",)


def test_an_oversize_entry_does_not_push_others_out() -> None:
    """Two small entries would still fit; the huge one must not evict either."""
    entries = [entry("small-1", 5, 0), entry("huge", 50, 9), entry("small-2", 5, 1)]
    decision = run(entries, budget_bytes=1000)
    assert decision.written_then_deleted == ("huge",)
    assert decision.victims == ()
    assert decision.kept == ("small-1", "small-2")


def test_entry_count_cap() -> None:
    entries = [entry(f"c{index}", 1, index) for index in range(5)]
    decision = run(entries, budget_bytes=1_000_000, slots=3)
    assert decision.victims == ("c0", "c1")
    assert decision.reasons == {"c0": "over_slots", "c1": "over_slots"}
    assert decision.kept_count == 3


def test_budget_and_slot_cap_can_bite_together() -> None:
    entries = [entry("a", 10, 0), entry("b", 10, 1), entry("c", 10, 2)]
    decision = run(entries, budget_bytes=1000, slots=5)
    # Evicting a fixes both the byte budget and leaves room under the cap.
    assert decision.victims == ("a", "b")
    assert decision.reasons == {"a": "over_budget", "b": "over_budget"}


def test_decision_is_stable_across_runs() -> None:
    entries = [entry(f"c{i}", 10, i % 3) for i in range(6)]
    first = run(entries, budget_bytes=3000)
    second = run(entries, budget_bytes=3000)
    assert first.to_dict() == second.to_dict()


def test_empty_fixture() -> None:
    decision = run([])
    assert decision.victims == ()
    assert decision.kept == ()
    assert decision.kept_bytes == 0


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("{}", "must be a JSON array"),
        ({"entries": []}, "must be a JSON array"),
        ([{"name": "a"}, {"name": "a"}], "duplicate"),
        ([{"name": ""}], "name must be a non-empty string"),
        ([{"name": "a", "tokens": -1}], "must be >= 0"),
        ([{"name": "a", "tokens": "many"}], "must be an integer"),
        ([{"name": "a", "pinned": 1}], "must be true or false"),
        ([{"name": "a", "pinnedd": True}], "unknown field"),
        ([42], "expected an object"),
    ],
)
def test_fixture_validation(raw: object, message: str) -> None:
    with pytest.raises(policy.PolicyError, match=message):
        policy.parse_fixture(raw)


def test_mode_and_budget_validation() -> None:
    entries = policy.parse_fixture([entry("a", 1)])
    with pytest.raises(policy.PolicyError):
        policy.evaluate(entries, mode="nru", budget_bytes=10)
    with pytest.raises(policy.PolicyError):
        policy.evaluate(entries, budget_bytes=-1)


def test_defaults_are_filled_in() -> None:
    (parsed,) = policy.parse_fixture([{"name": "bare"}])
    assert parsed.tokens == 0
    assert parsed.last_used == 0
    assert parsed.prefix is False
    assert parsed.pinned is False
    assert parsed.index == 0


def test_policy_module_never_touches_the_filesystem() -> None:
    """Purity guard: no filesystem module may be reachable from this module."""
    for forbidden in ("os", "pathlib", "shutil", "io", "glob", "tempfile"):
        assert forbidden not in vars(policy)
    assert not hasattr(policy, "open")
