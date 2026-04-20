"""Unit tests for flash_sizer.stats.

Reference Wilson-score intervals are taken from published worked examples so
we catch regressions in the arithmetic rather than the rounding:

  - Agresti & Coull (1998) "Approximate is better than 'exact' for interval
    estimation of binomial proportions", table 1 (95 % CI columns).
  - Brown, Cai, DasGupta (2001) "Interval Estimation for a Binomial
    Proportion", table 2 (Wilson column).
"""

from __future__ import annotations

import math

import pytest

from flash_sizer.stats import (
    IDLE_BUCKETS,
    Z_CRIT,
    idle_bucket,
    percentile,
    wilson_ci,
)

# ── wilson_ci ─────────────────────────────────────────────────────────────────


def test_wilson_95_matches_agresti_coull_worked_example() -> None:
    # Worked example: k=2, n=10, 95 % CI → (0.0368, 0.5178) in Agresti–Coull 1998.
    # Wilson rounds very close to that (same family of intervals; A&C adds a
    # small continuity adjustment). Compare directly to the Wilson formula
    # values, not A&C.
    ci = wilson_ci(k=2, n=10, level=0.95)
    assert ci.n == 10
    assert ci.k == 2
    assert ci.point == pytest.approx(0.2, abs=1e-12)
    # Wilson (p̂=0.2, n=10, z=1.96) → (0.05669, 0.50987), published to 4 dp.
    assert ci.lower == pytest.approx(0.0567, abs=5e-4)
    assert ci.upper == pytest.approx(0.5099, abs=5e-4)


def test_wilson_95_at_zero_success_is_well_defined() -> None:
    # Naive normal (p̂ ± z·√(p̂q̂/n)) collapses to [0, 0] here, which is wrong.
    # Wilson gives a real upper bound — this is why we use it.
    ci = wilson_ci(k=0, n=100, level=0.95)
    assert ci.lower == 0.0
    assert ci.upper > 0.0
    assert ci.upper == pytest.approx(0.0370, abs=5e-4)


def test_wilson_95_at_full_success_is_well_defined() -> None:
    ci = wilson_ci(k=100, n=100, level=0.95)
    assert ci.upper == 1.0
    assert ci.lower < 1.0
    assert ci.lower == pytest.approx(0.9630, abs=5e-4)


def test_wilson_empty_sample_returns_full_range() -> None:
    ci = wilson_ci(k=0, n=0, level=0.95)
    assert ci.point == 0.0
    assert ci.lower == 0.0
    assert ci.upper == 1.0


def test_wilson_large_n_tightens_interval() -> None:
    narrow = wilson_ci(k=500, n=1000, level=0.95)
    wide = wilson_ci(k=5, n=10, level=0.95)
    narrow_width = narrow.upper - narrow.lower
    wide_width = wide.upper - wide.lower
    assert narrow_width < 0.1
    assert wide_width > 0.5


def test_wilson_higher_level_widens_interval() -> None:
    ci_90 = wilson_ci(k=100, n=1000, level=0.90)
    ci_99 = wilson_ci(k=100, n=1000, level=0.99)
    assert ci_99.upper - ci_99.lower > ci_90.upper - ci_90.lower


@pytest.mark.parametrize("level", sorted(Z_CRIT))
def test_wilson_center_lies_between_bounds(level: float) -> None:
    ci = wilson_ci(k=37, n=103, level=level)
    assert ci.lower <= ci.point <= ci.upper


def test_wilson_rejects_k_greater_than_n() -> None:
    with pytest.raises(ValueError):
        wilson_ci(k=11, n=10)


def test_wilson_rejects_negative() -> None:
    with pytest.raises(ValueError):
        wilson_ci(k=-1, n=10)
    with pytest.raises(ValueError):
        wilson_ci(k=1, n=-1)


def test_wilson_rejects_unknown_level() -> None:
    with pytest.raises(ValueError):
        wilson_ci(k=1, n=10, level=0.77)


# ── percentile ────────────────────────────────────────────────────────────────


def test_percentile_of_single_value_is_that_value() -> None:
    assert percentile([42.0], 0.5) == 42.0
    assert percentile([42.0], 0.99) == 42.0


def test_percentile_empty_returns_zero() -> None:
    assert percentile([], 0.5) == 0.0


def test_percentile_linear_interpolation_between_points() -> None:
    # 10 ordered values 1..10 → p50 is the interpolation halfway between 5 and 6.
    # Linear: idx = 0.5 * (10-1) = 4.5 → (5 + (6-5)*0.5) = 5.5.
    values = list(range(1, 11))
    assert percentile(values, 0.5) == pytest.approx(5.5)


def test_percentile_bounds() -> None:
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert percentile(values, 0.0) == 1.0
    assert percentile(values, 1.0) == 5.0


def test_percentile_handles_unsorted_input() -> None:
    assert percentile([5, 3, 1, 4, 2], 0.5) == pytest.approx(3.0)


def test_percentile_p99_matches_numpy_linear() -> None:
    # 100 values 0..99 → p99 via numpy default "linear" = 98.01.
    # (idx = 0.99 * 99 = 98.01 → 98 + 0.01*(99-98) = 98.01)
    values = list(range(100))
    assert percentile(values, 0.99) == pytest.approx(98.01)


def test_percentile_rejects_out_of_range_p() -> None:
    with pytest.raises(ValueError):
        percentile([1.0, 2.0], 1.5)
    with pytest.raises(ValueError):
        percentile([1.0, 2.0], -0.1)


# ── idle_bucket ───────────────────────────────────────────────────────────────


def test_idle_buckets_are_in_ascending_order() -> None:
    uppers = [u for _, u in IDLE_BUCKETS]
    assert uppers == sorted(uppers)


@pytest.mark.parametrize(
    "idle_s, expected",
    [
        (0.0, "<1m"),
        (30.0, "<1m"),
        (60.0, "<10m"),
        (599.0, "<10m"),
        (600.0, "<1h"),
        (3599.0, "<1h"),
        (3600.0, "<1d"),
        (86399.0, "<1d"),
        (86400.0, "≥1d"),
        (math.inf, "≥1d"),
    ],
)
def test_idle_bucket_boundaries(idle_s: float, expected: str) -> None:
    assert idle_bucket(idle_s) == expected
