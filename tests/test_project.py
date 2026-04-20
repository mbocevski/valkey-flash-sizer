"""Unit tests for flash_sizer.project.

The projection layer is a pure function over `SampleStats`, so tests
just hand-build a `SampleStats` with the exact shape we want and assert
on the computed `Projection` fields.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from flash_sizer.project import (
    Projection,
    RangeEstimate,
    TypePercentiles,
    project,
)
from flash_sizer.sample import SampleStats


def _stats(**overrides: object) -> SampleStats:
    """Build a SampleStats with sensible defaults for the tests that don't
    care about every field; overrides set what the test does care about."""
    s = SampleStats()
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


# ── Basic projection ─────────────────────────────────────────────────────────


def test_projection_scales_bytes_by_dbsize_over_scanned() -> None:
    # Sampled 100 of 1000 keys → scale_factor = 10.0.
    stats = _stats(
        total_scanned=100,
        total_probed=100,
        total_bytes=1000,
        tierable_count=100,
        tierable_bytes=800,
        nontierable_count=0,
        nontierable_bytes=200,
        tierable_with_idle_count=100,
        tierable_with_idle_bytes=800,
        tierable_cold_count=50,
        tierable_cold_bytes=400,
        dbsize=1000,
    )
    proj = project(stats, cold_threshold_seconds=1800)
    assert proj.scale_factor == pytest.approx(10.0)
    assert proj.projected_total_bytes == 10_000
    assert proj.projected_tierable_bytes == 8_000
    assert proj.projected_nontierable_bytes == 2_000


def test_projection_floors_scale_factor_at_one() -> None:
    # dbsize < total_scanned (possible under concurrent deletes): don't
    # shrink the sample's numbers; just report them as-is.
    stats = _stats(
        total_scanned=500,
        dbsize=100,
        total_bytes=1000,
        tierable_bytes=1000,
        tierable_with_idle_count=500,
        tierable_cold_count=100,
    )
    proj = project(stats, cold_threshold_seconds=1800)
    assert proj.scale_factor == 1.0


def test_projection_dbsize_zero_means_no_extrapolation() -> None:
    # DBSIZE failed → scale=1.0, projection describes only the sample.
    stats = _stats(
        total_scanned=100,
        total_bytes=1000,
        tierable_bytes=1000,
        tierable_with_idle_count=100,
        tierable_cold_count=50,
        dbsize=0,
    )
    proj = project(stats, cold_threshold_seconds=1800)
    assert proj.scale_factor == 1.0
    assert proj.projected_total_bytes == 1000


# ── Cold fraction CI ─────────────────────────────────────────────────────────


def test_projection_computes_wilson_ci_on_cold_fraction() -> None:
    # 50 cold out of 100 tierable-with-idle → Wilson 95 % is ≈ (0.402, 0.598).
    stats = _stats(
        total_scanned=100,
        total_probed=100,
        total_bytes=1000,
        tierable_count=100,
        tierable_bytes=1000,
        tierable_with_idle_count=100,
        tierable_with_idle_bytes=1000,
        tierable_cold_count=50,
        tierable_cold_bytes=500,
        dbsize=100,
    )
    proj = project(stats, cold_threshold_seconds=1800)
    assert proj.cold_fraction_ci is not None
    assert proj.cold_fraction_ci.point == pytest.approx(0.5)
    assert proj.cold_fraction_ci.lower == pytest.approx(0.4038, abs=1e-3)
    assert proj.cold_fraction_ci.upper == pytest.approx(0.5962, abs=1e-3)
    assert proj.idle_data_available is True


def test_projection_under_pure_lfu_returns_zero_range() -> None:
    # No tierable key reported an idle value → projection declines.
    stats = _stats(
        total_scanned=100,
        total_probed=100,
        total_bytes=1000,
        tierable_count=100,
        tierable_bytes=1000,
        tierable_with_idle_count=0,
        idle_unsupported_count=100,
    )
    proj = project(stats, cold_threshold_seconds=1800)
    assert proj.idle_data_available is False
    assert proj.cold_fraction_ci is None
    assert proj.projected_cold_bytes == RangeEstimate(0, 0, 0)
    assert proj.projected_ram_saving == RangeEstimate(0, 0, 0)


# ── Cold bytes and RAM saving derivation ─────────────────────────────────────


def test_projection_cold_bytes_endpoints_come_from_wilson_bounds() -> None:
    stats = _stats(
        total_scanned=100,
        tierable_count=100,
        tierable_bytes=10_000,
        tierable_with_idle_count=100,
        tierable_cold_count=50,
        dbsize=100,
    )
    proj = project(stats, cold_threshold_seconds=1800)
    assert proj.cold_fraction_ci is not None
    # projected_cold_bytes = cold_fraction_ci × 10_000
    assert proj.projected_cold_bytes.point == pytest.approx(
        proj.cold_fraction_ci.point * 10_000, abs=1
    )
    assert proj.projected_cold_bytes.lower == pytest.approx(
        proj.cold_fraction_ci.lower * 10_000, abs=1
    )
    assert proj.projected_cold_bytes.upper == pytest.approx(
        proj.cold_fraction_ci.upper * 10_000, abs=1
    )


def test_projection_ram_saving_is_cold_bytes_times_keep_ratio() -> None:
    # 50 % cold of 10_000 tierable, hot_cache_ratio=0.10 → saving ≈ 4500 point
    stats = _stats(
        total_scanned=100,
        tierable_bytes=10_000,
        tierable_with_idle_count=100,
        tierable_cold_count=50,
        dbsize=100,
    )
    proj = project(stats, cold_threshold_seconds=1800, hot_cache_ratio=0.10)
    assert proj.projected_ram_saving.point == pytest.approx(
        proj.projected_cold_bytes.point * 0.90, abs=1
    )
    assert proj.projected_ram_saving.lower == pytest.approx(
        proj.projected_cold_bytes.lower * 0.90, abs=1
    )


def test_projection_rejects_invalid_hot_cache_ratio() -> None:
    stats = _stats(tierable_with_idle_count=1, tierable_cold_count=0)
    with pytest.raises(ValueError):
        project(stats, cold_threshold_seconds=1800, hot_cache_ratio=1.5)
    with pytest.raises(ValueError):
        project(stats, cold_threshold_seconds=1800, hot_cache_ratio=-0.1)


# ── Per-type percentiles ─────────────────────────────────────────────────────


def test_projection_emits_per_type_percentiles_sorted() -> None:
    stats = _stats(
        total_scanned=4,
        total_probed=4,
        tierable_count=4,
        tierable_with_idle_count=4,
        type_sizes={
            "string": [100, 200, 300, 400],
            "hash": [10, 20, 30, 40],
        },
    )
    proj = project(stats, cold_threshold_seconds=1800)
    # Sorted alphabetically: hash, string.
    assert [row.type for row in proj.per_type_percentiles] == ["hash", "string"]
    hash_row = proj.per_type_percentiles[0]
    assert hash_row.count == 4
    assert hash_row.max == 40


def test_projection_empty_per_type_when_no_tierable_samples() -> None:
    stats = _stats(tierable_with_idle_count=1, tierable_cold_count=0)
    proj = project(stats, cold_threshold_seconds=1800)
    assert proj.per_type_percentiles == ()


# ── Shape ────────────────────────────────────────────────────────────────────


def test_projection_is_frozen_dataclass() -> None:
    stats = _stats(tierable_with_idle_count=1, tierable_cold_count=0)
    proj = project(stats, cold_threshold_seconds=1800)
    with pytest.raises(FrozenInstanceError):
        proj.scale_factor = 99.0  # type: ignore[misc]


def test_projection_carries_inputs_back_to_report() -> None:
    # The report layer reads `stats`, `cold_threshold_seconds`,
    # `hot_cache_ratio`, `confidence_level` off the Projection directly —
    # this is a contract check.
    stats = _stats(tierable_with_idle_count=1, tierable_cold_count=0)
    proj = project(
        stats,
        cold_threshold_seconds=1800.0,
        hot_cache_ratio=0.07,
        confidence_level=0.99,
    )
    assert isinstance(proj, Projection)
    assert proj.stats is stats
    assert proj.cold_threshold_seconds == 1800.0
    assert proj.hot_cache_ratio == 0.07
    assert proj.confidence_level == 0.99


def test_type_percentiles_dataclass_fields() -> None:
    # Pure-shape test so the report layer can rely on the attribute names.
    tp = TypePercentiles(type="string", count=1, p50=10, p99=20, max=30)
    assert tp.type == "string"
    assert tp.count == 1
