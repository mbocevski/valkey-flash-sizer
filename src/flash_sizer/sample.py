"""Keyspace sampler: SCAN, probe each sampled key, aggregate.

Design notes:

- Keys are kept as `bytes` throughout. Valkey keys may be non-UTF-8 (binary
  counters, hashed identifiers) and a decode error during sampling would be
  a terrible failure mode. Decoding happens only at report render time.

- Probes are pipelined. A round-trip per key over a remote connection would
  take hours for 100 000 samples; pipelining in batches brings it to seconds.

- `OBJECT IDLETIME` is unsupported under an LFU `maxmemory-policy` — Valkey
  returns an error instead of a number. The sampler records this per-probe
  as `idle_seconds=None` and leaves policy detection + user warnings to the
  caller (the CLI has the whole-session context to warn about once).

- Top-N tracking uses a bounded min-heap: keep the N largest keys without
  materialising the full size list.

- `SampleStats` aggregates everything needed for projection and report;
  per-type size lists are retained so `percentile()` can run on them later
  without a second pass over probes.
"""

from __future__ import annotations

import heapq
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol

from flash_sizer.stats import IDLE_BUCKETS, idle_bucket

_log = logging.getLogger(__name__)

# Valkey types reachable by our tiering data-types. Other types (set, stream,
# pubsub, HLL, etc.) are counted in the sample but excluded from the
# "tierable" projection because valkey-flash has no tiered equivalent yet.
TIERABLE_TYPES: frozenset[str] = frozenset({"string", "hash", "list", "zset"})

# Valkey TTL sentinels (documented in server.c). -1 = persistent key,
# -2 = the key has already expired at the time of the call.
_TTL_PERSISTENT = -1
_TTL_EXPIRED = -2

# Pipeline result layout: 4 commands per key, in this fixed order.
_COMMANDS_PER_KEY = 4


@dataclass(frozen=True)
class KeyProbe:
    """Per-key data from a single sampler probe.

    `size_bytes` is what `MEMORY USAGE` returns — includes value encoding
    overhead, embedded TTL, and per-object bookkeeping. It's a good proxy
    for "what this key costs the server."

    `idle_seconds` is `None` if `OBJECT IDLETIME` wasn't supported (LFU
    policy) or if the key had vanished between SCAN and probe.

    `ttl_seconds` follows Valkey's convention: positive integer = seconds
    remaining, `None` = no TTL set.
    """

    key: bytes
    type: str
    size_bytes: int
    idle_seconds: int | None
    ttl_seconds: int | None


@dataclass
class SampleStats:
    """Aggregate of all probed keys, in the shape the projection/report need.

    `total_scanned` counts every key the sampler attempted to probe;
    `total_probed` is the subset where `MEMORY USAGE` returned a number
    (keys that vanished between SCAN and probe get dropped, not counted).

    Byte totals are always for probed keys only. That's the honest
    denominator for any "% cold" calculation.
    """

    total_scanned: int = 0
    total_probed: int = 0
    total_bytes: int = 0

    # Tierable vs non-tierable split. Non-tierable bytes are subtracted
    # from the projection denominator because valkey-flash can't reduce them.
    tierable_count: int = 0
    tierable_bytes: int = 0
    nontierable_count: int = 0
    nontierable_bytes: int = 0

    # Idle-time histogram over *probed keys that reported an idle value*.
    # If the whole run is under LFU, these stay at zero and the report
    # surfaces the policy mismatch rather than showing a bogus distribution.
    idle_counts: dict[str, int] = field(default_factory=dict)
    idle_bytes: dict[str, int] = field(default_factory=dict)

    # Cold = idle >= threshold *and* type is tierable. That's the real
    # candidate set for valkey-flash migration.
    tierable_cold_count: int = 0
    tierable_cold_bytes: int = 0

    # Retained for percentile computation at report time. Keyed by the
    # tierable type so we can surface "string p99 = 4.2 KB, hash p99 = …".
    type_sizes: dict[str, list[int]] = field(default_factory=dict)

    # Largest probed keys (key, type, bytes, idle-or-None). Size-bounded
    # at construction time; see `_TopN`.
    top_n_large: list[tuple[bytes, str, int, int | None]] = field(default_factory=list)

    # How many probes came back with `idle_seconds=None` because the server
    # rejected `OBJECT IDLETIME` (LFU policy). When this equals total_probed
    # the whole histogram is silent and the report should say so.
    idle_unsupported_count: int = 0

    # Server-reported total keyspace size; sampled_fraction = total_scanned/dbsize.
    dbsize: int = 0


# ── Client protocol ───────────────────────────────────────────────────────────


class SamplerClient(Protocol):
    """Minimal slice of valkey-py we actually use.

    Declared as a Protocol so tests can supply a hand-rolled fake without
    importing real valkey-py client classes. The real `valkey.Valkey` and
    `valkey.cluster.ValkeyCluster` both satisfy this structurally.
    """

    def scan_iter(self, count: int = ...) -> Iterator[bytes]: ...
    def dbsize(self) -> int: ...
    def pipeline(self, transaction: bool = ...) -> Any: ...


# ── Top-N helper ──────────────────────────────────────────────────────────────


class _TopN:
    """Min-heap that keeps only the N largest-size probes.

    Python's `heapq` is a min-heap; storing `(size, …)` means the smallest
    size sits at index 0. On overflow we pop that one, which is exactly
    what "evict the smallest so we keep the largest N" wants.
    """

    def __init__(self, n: int) -> None:
        self._n = n
        # Heap entries: (size, tiebreaker, key, type, idle). The tiebreaker
        # is the running counter so heapq never compares bytes keys (which
        # would work but is nondeterministic-looking in tracebacks).
        self._heap: list[tuple[int, int, bytes, str, int | None]] = []
        self._counter = 0

    def push(self, probe: KeyProbe) -> None:
        self._counter += 1
        entry = (probe.size_bytes, self._counter, probe.key, probe.type, probe.idle_seconds)
        if len(self._heap) < self._n:
            heapq.heappush(self._heap, entry)
        else:
            heapq.heappushpop(self._heap, entry)

    def sorted_desc(self) -> list[tuple[bytes, str, int, int | None]]:
        """Return `(key, type, size, idle)` tuples, largest first."""
        # nlargest is cheap here — heap already sized at N.
        return [(k, t, s, i) for (s, _counter, k, t, i) in sorted(self._heap, key=lambda e: -e[0])]


# ── Main sampler ──────────────────────────────────────────────────────────────


def sample_keyspace(
    client: SamplerClient,
    *,
    target_samples: int = 100_000,
    cold_threshold_seconds: float = 1800.0,
    pipeline_size: int = 200,
    top_n: int = 10,
    scan_count: int = 1000,
) -> SampleStats:
    """Walk SCAN, probe in pipelined batches, aggregate into a `SampleStats`.

    `target_samples` is a cap on probed keys, not scanned. Small keyspaces
    finish before the cap; large keyspaces stop at the cap and the report
    flags the sampling fraction.

    `cold_threshold_seconds` defines "cold" for the `tierable_cold_*`
    counters. Defaults to 30 minutes.

    `pipeline_size` controls the per-batch MEMORY/IDLETIME/TTL/TYPE
    round-trip. Larger = fewer RTTs but more server-side CPU per batch.
    200 is a reasonable midpoint; the CLI exposes a flag for users on
    high-latency links to bump it.
    """
    stats = SampleStats()
    try:
        stats.dbsize = int(client.dbsize())
    except Exception as e:
        # DBSIZE is trivial; if it fails the rest will fail too, but we'd
        # rather carry on and let the real command raise a clearer error.
        _log.warning("DBSIZE failed: %s", e)
        stats.dbsize = 0

    top_tracker = _TopN(top_n)

    # Buffer keys from SCAN, flush through a pipeline when the buffer fills
    # or when target_samples is hit.
    batch: list[bytes] = []
    for key in client.scan_iter(count=scan_count):
        batch.append(key)
        if len(batch) >= pipeline_size:
            _probe_batch(client, batch, cold_threshold_seconds, stats, top_tracker)
            batch.clear()
            if stats.total_scanned >= target_samples:
                break

    # Final partial batch.
    if batch and stats.total_scanned < target_samples:
        remaining = target_samples - stats.total_scanned
        _probe_batch(client, batch[:remaining], cold_threshold_seconds, stats, top_tracker)

    stats.top_n_large = top_tracker.sorted_desc()

    # Zero-fill idle histogram buckets that saw no samples so the report
    # renders every bucket (a missing "<1h" bucket would look like a bug).
    for label, _upper in IDLE_BUCKETS:
        stats.idle_counts.setdefault(label, 0)
        stats.idle_bytes.setdefault(label, 0)

    return stats


def _probe_batch(
    client: SamplerClient,
    keys: Iterable[bytes],
    cold_threshold_seconds: float,
    stats: SampleStats,
    top_tracker: _TopN,
) -> None:
    """Pipeline `TYPE`, `MEMORY USAGE`, `OBJECT IDLETIME`, `TTL` for each key.

    The order of commands per key is fixed — `_COMMANDS_PER_KEY` entries per
    key — so the result-list offsets line up with `_decode_probe` below.
    """
    key_list = list(keys)
    if not key_list:
        return

    pipe = client.pipeline(transaction=False)
    for k in key_list:
        pipe.type(k)
        pipe.memory_usage(k)
        pipe.object("idletime", k)
        pipe.ttl(k)

    # valkey-py returns a flat list: 4 entries per key in the order pushed.
    # A ResponseError for any single command appears as the exception
    # instance at that position rather than aborting the whole pipeline
    # (we don't call it in transaction mode).
    results = pipe.execute(raise_on_error=False)
    expected = _COMMANDS_PER_KEY * len(key_list)
    if len(results) != expected:
        raise RuntimeError(
            f"pipeline returned {len(results)} results, expected {expected} "
            f"({_COMMANDS_PER_KEY} × {len(key_list)} keys) — upstream client API drift?"
        )

    for i, key in enumerate(key_list):
        stats.total_scanned += 1
        off = i * _COMMANDS_PER_KEY
        probe, idle_unsupported = _decode_probe(
            key,
            type_raw=results[off],
            size_raw=results[off + 1],
            idle_raw=results[off + 2],
            ttl_raw=results[off + 3],
        )
        if idle_unsupported:
            stats.idle_unsupported_count += 1
        if probe is not None:
            _record(probe, cold_threshold_seconds, stats, top_tracker)


def _decode_probe(
    key: bytes,
    *,
    type_raw: Any,
    size_raw: Any,
    idle_raw: Any,
    ttl_raw: Any,
) -> tuple[KeyProbe | None, bool]:
    """Turn a 4-result pipeline slice into a `KeyProbe` or None.

    Returns `(probe, idle_unsupported)`. `idle_unsupported` is True when
    the server rejected `OBJECT IDLETIME` (LFU policy) — the caller uses
    it to feed the per-run warning counter independently of whether the
    rest of the probe succeeded.
    """
    # TYPE: str or "none" for a vanished key. Any error → drop.
    if isinstance(type_raw, Exception):
        return None, False
    type_str = type_raw.decode() if isinstance(type_raw, bytes) else str(type_raw)
    if type_str == "none":
        return None, False

    # MEMORY USAGE: int, or None if the key vanished mid-probe.
    if isinstance(size_raw, Exception) or size_raw is None:
        return None, False
    try:
        size_bytes = int(size_raw)
    except (TypeError, ValueError):
        return None, False

    # OBJECT IDLETIME: int under LRU; error under LFU; None if vanished.
    idle_unsupported = False
    idle_seconds: int | None
    if isinstance(idle_raw, Exception) or idle_raw is None:
        idle_seconds = None
        idle_unsupported = True
    else:
        try:
            idle_seconds = int(idle_raw)
        except (TypeError, ValueError):
            idle_seconds = None
            idle_unsupported = True

    # TTL: -1 persistent, -2 already expired (drop), >=0 seconds remaining.
    if isinstance(ttl_raw, Exception) or ttl_raw is None:
        ttl_seconds: int | None = None
    else:
        try:
            ttl_val = int(ttl_raw)
        except (TypeError, ValueError):
            ttl_val = _TTL_PERSISTENT
        if ttl_val == _TTL_EXPIRED:
            return None, idle_unsupported
        ttl_seconds = None if ttl_val < 0 else ttl_val

    probe = KeyProbe(
        key=key,
        type=type_str,
        size_bytes=size_bytes,
        idle_seconds=idle_seconds,
        ttl_seconds=ttl_seconds,
    )
    return probe, idle_unsupported


def _record(
    probe: KeyProbe,
    cold_threshold_seconds: float,
    stats: SampleStats,
    top_tracker: _TopN,
) -> None:
    stats.total_probed += 1
    stats.total_bytes += probe.size_bytes
    top_tracker.push(probe)

    is_tierable = probe.type in TIERABLE_TYPES
    if is_tierable:
        stats.tierable_count += 1
        stats.tierable_bytes += probe.size_bytes
        stats.type_sizes.setdefault(probe.type, []).append(probe.size_bytes)
    else:
        stats.nontierable_count += 1
        stats.nontierable_bytes += probe.size_bytes

    if probe.idle_seconds is not None:
        bucket = idle_bucket(float(probe.idle_seconds))
        stats.idle_counts[bucket] = stats.idle_counts.get(bucket, 0) + 1
        stats.idle_bytes[bucket] = stats.idle_bytes.get(bucket, 0) + probe.size_bytes
        if is_tierable and probe.idle_seconds >= cold_threshold_seconds:
            stats.tierable_cold_count += 1
            stats.tierable_cold_bytes += probe.size_bytes
