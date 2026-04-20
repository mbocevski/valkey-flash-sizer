"""Unit tests for flash_sizer.sample.

We exercise the sampler with a hand-rolled `FakeClient` that mimics only
the valkey-py surface the sampler touches: `scan_iter`, `dbsize`, and a
minimal `pipeline()` recorder. This is deliberate — using fakeredis would
drag a 40 MB dependency in for tests that care about aggregation logic,
not wire semantics.

Each test builds a dict of (key → type/size/idle/ttl) and lets the fake
pipeline answer commands from that dict. TYPE and MEMORY USAGE disappearance
are modelled with sentinel values (``None`` / ``"none"``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from valkey.exceptions import ResponseError

from flash_sizer.sample import (
    TIERABLE_TYPES,
    KeyProbe,
    SampleStats,
    _TopN,
    sample_keyspace,
)

# ── Fake client ───────────────────────────────────────────────────────────────


@dataclass
class FakeKey:
    """Server-side shape of a key as the fake pipeline sees it."""

    type: str  # "string", "hash", "none", …
    size: int | None  # MEMORY USAGE; None → key vanished
    idle: int | None  # OBJECT IDLETIME; None → vanished; sentinel "lfu" handled below
    ttl: int  # -1 no ttl, -2 expired, ≥0 seconds
    idle_error: bool = False  # simulate LFU-mode error for OBJECT IDLETIME


class FakePipeline:
    def __init__(self, store: dict[bytes, FakeKey]) -> None:
        self._store = store
        self._queued: list[tuple[str, tuple[Any, ...]]] = []

    def type(self, key: bytes) -> None:
        self._queued.append(("type", (key,)))

    def memory_usage(self, key: bytes) -> None:
        self._queued.append(("memory_usage", (key,)))

    def object(self, subcommand: str, key: bytes) -> None:
        self._queued.append(("object", (subcommand, key)))

    def ttl(self, key: bytes) -> None:
        self._queued.append(("ttl", (key,)))

    def execute(self, raise_on_error: bool = True) -> list[Any]:
        results: list[Any] = []
        for cmd, args in self._queued:
            entry = self._store.get(args[-1])
            if cmd == "type":
                results.append("none" if entry is None else entry.type)
            elif cmd == "memory_usage":
                results.append(None if entry is None else entry.size)
            elif cmd == "object":
                if entry is None:
                    results.append(None)
                elif entry.idle_error:
                    results.append(ResponseError("An LFU maxmemory policy is not selected"))
                else:
                    results.append(entry.idle)
            elif cmd == "ttl":
                results.append(-2 if entry is None else entry.ttl)
            else:
                raise RuntimeError(f"fake pipeline: unexpected cmd {cmd}")
        self._queued.clear()
        return results


class FakeClient:
    def __init__(self, store: dict[bytes, FakeKey]) -> None:
        self._store = store

    def scan_iter(self, count: int = 10) -> Any:
        yield from iter(self._store.keys())

    def dbsize(self) -> int:
        return len(self._store)

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        return FakePipeline(self._store)


def _mk_store(spec: list[tuple[bytes, str, int, int | None, int]]) -> dict[bytes, FakeKey]:
    """`(key, type, size, idle, ttl)` tuples → FakeKey dict."""
    return {k: FakeKey(type=t, size=s, idle=i, ttl=ttl) for (k, t, s, i, ttl) in spec}


# ── sample_keyspace behavioural tests ─────────────────────────────────────────


def test_sampler_counts_every_probed_key_and_bytes() -> None:
    store = _mk_store(
        [
            (b"s1", "string", 100, 10, -1),
            (b"s2", "string", 200, 20, -1),
            (b"h1", "hash", 1000, 30, -1),
        ]
    )
    stats = sample_keyspace(FakeClient(store), pipeline_size=10)
    assert stats.total_scanned == 3
    assert stats.total_probed == 3
    assert stats.total_bytes == 1300
    assert stats.dbsize == 3


def test_sampler_splits_tierable_from_nontierable() -> None:
    store = _mk_store(
        [
            (b"str", "string", 100, 10, -1),
            (b"hash", "hash", 200, 20, -1),
            (b"list", "list", 300, 30, -1),
            (b"zset", "zset", 400, 40, -1),
            (b"set", "set", 500, 50, -1),
            (b"stream", "stream", 600, 60, -1),
        ]
    )
    stats = sample_keyspace(FakeClient(store))
    assert stats.tierable_count == 4
    assert stats.tierable_bytes == 100 + 200 + 300 + 400
    assert stats.nontierable_count == 2
    assert stats.nontierable_bytes == 500 + 600
    assert set(stats.type_sizes) == {"string", "hash", "list", "zset"}


def test_sampler_skips_vanished_keys() -> None:
    # Key present in scan_iter but server reports `none` / None on probe.
    # Simulate by putting a TYPE=none entry in the store.
    store = _mk_store([(b"present", "string", 42, 0, -1)])
    store[b"gone"] = FakeKey(type="none", size=None, idle=None, ttl=-2)
    stats = sample_keyspace(FakeClient(store))
    assert stats.total_scanned == 2  # we attempted both
    assert stats.total_probed == 1  # only one passed through
    assert stats.total_bytes == 42


def test_sampler_skips_keys_with_ttl_negative_two() -> None:
    # Race: key existed at SCAN, expired before MEMORY USAGE. Valkey returns
    # a valid size but TTL=-2 ("already expired"). Drop.
    store = {
        b"racy": FakeKey(type="string", size=42, idle=10, ttl=-2),
    }
    stats = sample_keyspace(FakeClient(store))
    assert stats.total_scanned == 1
    assert stats.total_probed == 0


def test_sampler_handles_lfu_idle_error_gracefully() -> None:
    store = _mk_store([(b"a", "string", 100, 0, -1), (b"b", "hash", 200, 0, -1)])
    # Force OBJECT IDLETIME to error for every key (LFU policy simulation).
    for v in store.values():
        v.idle_error = True
    stats = sample_keyspace(FakeClient(store))
    assert stats.total_probed == 2
    assert stats.total_bytes == 300
    assert stats.idle_unsupported_count == 2
    # No probe contributed to any idle bucket.
    assert sum(stats.idle_counts.values()) == 0
    # Cold counts are zero because idle was unknowable.
    assert stats.tierable_cold_count == 0


def test_sampler_cold_threshold_only_counts_tierable() -> None:
    store = _mk_store(
        [
            (b"cold-str", "string", 100, 3600, -1),  # 1h idle → cold
            (b"hot-str", "string", 100, 10, -1),
            (b"cold-set", "set", 100, 3600, -1),  # non-tierable; excluded from cold
        ]
    )
    stats = sample_keyspace(FakeClient(store), cold_threshold_seconds=1800)
    assert stats.tierable_cold_count == 1
    assert stats.tierable_cold_bytes == 100


def test_sampler_idle_histogram_fills_all_buckets() -> None:
    store = _mk_store(
        [
            (b"a", "string", 10, 30, -1),  # <1m
            (b"b", "string", 20, 300, -1),  # <10m
            (b"c", "string", 40, 1800, -1),  # <1h
            (b"d", "string", 80, 7200, -1),  # <1d
            (b"e", "string", 160, 90000, -1),  # ≥1d
        ]
    )
    stats = sample_keyspace(FakeClient(store))
    assert stats.idle_counts == {"<1m": 1, "<10m": 1, "<1h": 1, "<1d": 1, "≥1d": 1}
    assert stats.idle_bytes == {"<1m": 10, "<10m": 20, "<1h": 40, "<1d": 80, "≥1d": 160}


def test_sampler_idle_histogram_zero_fills_empty_buckets() -> None:
    store = _mk_store([(b"a", "string", 10, 30, -1)])  # only <1m bucket sees data
    stats = sample_keyspace(FakeClient(store))
    # Every labelled bucket present with 0/0 entries, so the report renders
    # the full distribution instead of a ragged one.
    assert set(stats.idle_counts) == {"<1m", "<10m", "<1h", "<1d", "≥1d"}
    assert stats.idle_counts["<1m"] == 1
    for label in ("<10m", "<1h", "<1d", "≥1d"):
        assert stats.idle_counts[label] == 0
        assert stats.idle_bytes[label] == 0


def test_sampler_top_n_returns_largest_in_desc_order() -> None:
    # 12 keys of increasing size; top_n=3 should pick the top three.
    store = _mk_store([(f"k{n}".encode(), "string", n * 10, 0, -1) for n in range(1, 13)])
    stats = sample_keyspace(FakeClient(store), top_n=3)
    assert len(stats.top_n_large) == 3
    sizes = [size for (_k, _t, size, _idle) in stats.top_n_large]
    assert sizes == [120, 110, 100]


def test_sampler_top_n_handles_size_ties_stably() -> None:
    store = _mk_store(
        [
            (b"a", "string", 100, 0, -1),
            (b"b", "string", 100, 0, -1),
            (b"c", "string", 100, 0, -1),
            (b"d", "string", 50, 0, -1),
        ]
    )
    stats = sample_keyspace(FakeClient(store), top_n=3)
    # All three top entries have size 100; order among ties is insertion
    # order (stable) because the internal counter is monotonic.
    assert len(stats.top_n_large) == 3
    assert {k for (k, _t, _s, _i) in stats.top_n_large} == {b"a", b"b", b"c"}


def test_sampler_respects_target_samples_cap() -> None:
    store = _mk_store([(f"k{n}".encode(), "string", 10, 0, -1) for n in range(1000)])
    stats = sample_keyspace(FakeClient(store), target_samples=200, pipeline_size=50)
    # Cap is approximate-up-to-one-batch; cannot exceed target + pipeline - 1
    # in the worst case, but with pipeline_size=50 and target=200 we expect
    # exactly 200 scanned (batch boundary aligns).
    assert stats.total_scanned == 200
    assert stats.total_probed == 200


def test_sampler_preserves_raw_bytes_keys() -> None:
    # Binary key with non-UTF8 bytes must survive the sampler round-trip —
    # report rendering is the only place decoding happens.
    non_utf8 = b"\xff\xfe\x00\x01binary"
    store = {non_utf8: FakeKey(type="string", size=42, idle=5, ttl=-1)}
    stats = sample_keyspace(FakeClient(store), top_n=1)
    assert stats.top_n_large[0][0] == non_utf8


def test_sampler_handles_empty_keyspace() -> None:
    stats = sample_keyspace(FakeClient({}))
    assert stats.total_scanned == 0
    assert stats.total_probed == 0
    assert stats.total_bytes == 0
    assert stats.top_n_large == []
    # Histogram still zero-filled so the report renders cleanly.
    assert set(stats.idle_counts) == {"<1m", "<10m", "<1h", "<1d", "≥1d"}


def test_sampler_rejects_pipeline_result_length_drift() -> None:
    # Simulate a client whose pipeline returns the wrong number of results
    # per key. The sampler raises immediately — silent misalignment would
    # mis-attribute sizes to idle buckets.
    class BrokenPipeline(FakePipeline):
        def execute(self, raise_on_error: bool = True) -> list[Any]:
            return super().execute(raise_on_error)[:-1]  # drop last entry

    class BrokenClient(FakeClient):
        def pipeline(self, transaction: bool = False) -> FakePipeline:
            return BrokenPipeline(self._store)

    store = _mk_store([(b"a", "string", 10, 0, -1)])
    with pytest.raises(RuntimeError, match="pipeline returned"):
        sample_keyspace(BrokenClient(store))


# ── _TopN unit tests ──────────────────────────────────────────────────────────


def test_topn_smaller_than_capacity_returns_all_sorted() -> None:

    tn = _TopN(n=5)
    for n in [30, 10, 20]:
        tn.push(
            KeyProbe(
                key=str(n).encode(), type="string", size_bytes=n, idle_seconds=0, ttl_seconds=None
            )
        )
    out = tn.sorted_desc()
    assert [size for (_k, _t, size, _i) in out] == [30, 20, 10]


def test_topn_evicts_smaller_to_keep_largest_n() -> None:

    tn = _TopN(n=2)
    for n in [1, 2, 3, 4, 5]:
        tn.push(
            KeyProbe(
                key=str(n).encode(), type="string", size_bytes=n, idle_seconds=0, ttl_seconds=None
            )
        )
    out = tn.sorted_desc()
    assert [size for (_k, _t, size, _i) in out] == [5, 4]


def test_topn_bytes_keys_do_not_break_heap_comparison() -> None:

    # Two keys with the same size but different bytes — the heap tiebreaker
    # must avoid comparing `bytes` objects directly (which works but is
    # noisy in tracebacks; we prefer a deterministic insertion counter).
    tn = _TopN(n=2)
    tn.push(KeyProbe(key=b"aaa", type="string", size_bytes=10, idle_seconds=0, ttl_seconds=None))
    tn.push(KeyProbe(key=b"bbb", type="string", size_bytes=10, idle_seconds=0, ttl_seconds=None))
    # Another push with the same size triggers heappushpop which must not
    # fall back to comparing the bytes keys. Would raise TypeError in
    # pre-3.x Python; in 3.x it'd just sort by byte value nondeterministically.
    tn.push(KeyProbe(key=b"ccc", type="string", size_bytes=10, idle_seconds=0, ttl_seconds=None))
    out = tn.sorted_desc()
    assert len(out) == 2


# ── SampleStats invariants ────────────────────────────────────────────────────


def test_sample_stats_defaults_are_all_zero() -> None:
    s = SampleStats()
    assert s.total_scanned == 0
    assert s.total_bytes == 0
    assert s.type_sizes == {}
    assert s.top_n_large == []


def test_tierable_types_is_frozen() -> None:
    # Belt + braces: the module-level constant must be immutable so tests
    # in other files can't mutate it and create spooky action at a distance.
    assert isinstance(TIERABLE_TYPES, frozenset)
    assert frozenset({"string", "hash", "list", "zset"}) == TIERABLE_TYPES
