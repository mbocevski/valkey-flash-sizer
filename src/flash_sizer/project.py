"""Projection: turn a `SampleStats` into "what would valkey-flash save me?"

The honest claim is a Wilson-score confidence interval on the *fraction*
of tierable keys that are cold — that's the proportion we actually
sampled, and Wilson is the right instrument for it. Everything else is
a derivation, and every derivation carries an assumption worth naming:

1. **Scale factor `dbsize / total_scanned`** applies the sample's
   byte totals to the full keyspace. Assumes the sample is representative
   of the whole keyspace, which is what stratified SCAN aims for but
   can't guarantee under heavy concurrent writes. Reported as an
   explicit multiplier so the reader sees it.

2. **Cold-bytes via fraction × tierable bytes** assumes cold keys and
   hot keys have the same average size. Often they don't — cold keys
   skew larger in typical caching workloads. Reported as a caveat in
   the "Known biases" section.

3. **RAM saving = cold bytes × (1 − hot_cache_ratio)** assumes the
   user will configure `valkey-flash` with a hot-cache fraction equal
   to `hot_cache_ratio`. Default 5 % matches the module's recommended
   starting point; users can override.

Under LFU policy `OBJECT IDLETIME` errors out and none of this is
computable. The projection flags `idle_data_available=False` and the
report surfaces the policy mismatch instead of rendering zeros.
"""

from __future__ import annotations

from dataclasses import dataclass

from flash_sizer.sample import SampleStats
from flash_sizer.stats import ProportionCI, percentile, wilson_ci


@dataclass(frozen=True)
class RangeEstimate:
    """A projected numeric value with confidence bounds.

    `point` is the best point estimate; `lower`/`upper` are the CI
    endpoints propagated from the underlying Wilson proportion. All
    three share the same units (bytes, in practice).
    """

    point: int
    lower: int
    upper: int


@dataclass(frozen=True)
class TypePercentiles:
    """Per-type size percentiles for the tierable types."""

    type: str
    count: int
    p50: int
    p99: int
    max: int


@dataclass(frozen=True)
class Projection:
    """Full RAM-saving projection for the sampled keyspace."""

    # Input context — kept by reference so the report has everything in one object.
    stats: SampleStats
    cold_threshold_seconds: float
    hot_cache_ratio: float
    confidence_level: float

    # True when at least one probe had a known idle time. False under pure LFU.
    idle_data_available: bool

    # dbsize / total_scanned, floored at 1.0. When dbsize is 0 or missing
    # (stats.dbsize == 0) we default to 1.0 and the projection describes
    # only the sample, not an extrapolation.
    scale_factor: float

    # Wilson-score CI on the fraction of tierable-with-idle keys that
    # exceed the cold threshold. None under pure LFU.
    cold_fraction_ci: ProportionCI | None

    # Keyspace-scale projections (sample totals × scale_factor).
    projected_total_bytes: int
    projected_tierable_bytes: int
    projected_nontierable_bytes: int

    # Derived from cold_fraction_ci × projected_tierable_bytes. Zero when
    # idle_data_available is False.
    projected_cold_bytes: RangeEstimate
    projected_ram_saving: RangeEstimate

    # Per-type size summaries for the "value-size distribution" table.
    per_type_percentiles: tuple[TypePercentiles, ...]


_ZERO_RANGE = RangeEstimate(point=0, lower=0, upper=0)


def project(
    stats: SampleStats,
    *,
    cold_threshold_seconds: float,
    hot_cache_ratio: float = 0.05,
    confidence_level: float = 0.95,
) -> Projection:
    """Compute a `Projection` from a `SampleStats`.

    Pure function. No I/O, no side effects — the report layer renders
    the returned object; the CLI orchestrates sample → project → render.
    """
    if not 0.0 <= hot_cache_ratio <= 1.0:
        raise ValueError(f"hot_cache_ratio must be in [0, 1], got {hot_cache_ratio}")

    scale_factor = _compute_scale_factor(stats)
    projected_total = int(stats.total_bytes * scale_factor)
    projected_tierable = int(stats.tierable_bytes * scale_factor)
    projected_nontierable = int(stats.nontierable_bytes * scale_factor)

    idle_data_available = stats.tierable_with_idle_count > 0
    cold_fraction_ci: ProportionCI | None
    cold_bytes: RangeEstimate
    ram_saving: RangeEstimate

    if idle_data_available:
        cold_fraction_ci = wilson_ci(
            k=stats.tierable_cold_count,
            n=stats.tierable_with_idle_count,
            level=confidence_level,
        )
        # Cold bytes: Wilson CI bounds on fraction multiplied by the
        # projected tierable-bytes total. See module docstring for why
        # the size-neutrality assumption is stated as a caveat, not
        # absorbed into the interval width.
        cold_bytes = RangeEstimate(
            point=int(cold_fraction_ci.point * projected_tierable),
            lower=int(cold_fraction_ci.lower * projected_tierable),
            upper=int(cold_fraction_ci.upper * projected_tierable),
        )
        # RAM saving = cold bytes × (1 - hot_cache_ratio). Linear
        # propagation of the interval endpoints.
        keep_ratio = 1.0 - hot_cache_ratio
        ram_saving = RangeEstimate(
            point=int(cold_bytes.point * keep_ratio),
            lower=int(cold_bytes.lower * keep_ratio),
            upper=int(cold_bytes.upper * keep_ratio),
        )
    else:
        cold_fraction_ci = None
        cold_bytes = _ZERO_RANGE
        ram_saving = _ZERO_RANGE

    per_type = _compute_per_type_percentiles(stats)

    return Projection(
        stats=stats,
        cold_threshold_seconds=cold_threshold_seconds,
        hot_cache_ratio=hot_cache_ratio,
        confidence_level=confidence_level,
        idle_data_available=idle_data_available,
        scale_factor=scale_factor,
        cold_fraction_ci=cold_fraction_ci,
        projected_total_bytes=projected_total,
        projected_tierable_bytes=projected_tierable,
        projected_nontierable_bytes=projected_nontierable,
        projected_cold_bytes=cold_bytes,
        projected_ram_saving=ram_saving,
        per_type_percentiles=per_type,
    )


def _compute_scale_factor(stats: SampleStats) -> float:
    """`dbsize / total_scanned`, floored at 1.0 and guarded against zeros.

    When `total_scanned == 0` we have nothing to scale and return 1.0 —
    the projection collapses to "zero on zero." When `dbsize == 0` we
    couldn't read DBSIZE so we decline to extrapolate beyond the sample.
    When `dbsize < total_scanned` (possible under concurrent deletes
    between DBSIZE and the sample's end), we also floor at 1.0; the
    sample is the best estimate we have.
    """
    if stats.total_scanned == 0 or stats.dbsize == 0:
        return 1.0
    factor = stats.dbsize / stats.total_scanned
    return max(1.0, factor)


def _compute_per_type_percentiles(stats: SampleStats) -> tuple[TypePercentiles, ...]:
    """Sort types alphabetically for deterministic report ordering."""
    rows: list[TypePercentiles] = []
    for type_name in sorted(stats.type_sizes):
        sizes = stats.type_sizes[type_name]
        if not sizes:
            continue
        rows.append(
            TypePercentiles(
                type=type_name,
                count=len(sizes),
                p50=int(percentile(sizes, 0.5)),
                p99=int(percentile(sizes, 0.99)),
                max=int(max(sizes)),
            )
        )
    return tuple(rows)
