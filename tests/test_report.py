"""Unit tests for flash_sizer.report.

Most of the work happens in golden-file tests: build a fixed `SampleStats`,
project it, render Markdown + JSON, compare against a checked-in golden.
This catches accidental copy drift (a rephrased caveat, a changed table
heading) which the unit tests would miss.

To intentionally update the goldens after a report change:

    UPDATE_GOLDENS=1 uv run pytest tests/test_report.py

Review the resulting diff before committing — any mechanical difference
(number reshuffled) means the projection or sampler changed underneath,
not the report copy.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from flash_sizer.project import project
from flash_sizer.report import ReportContext, render_json, render_markdown
from flash_sizer.sample import SampleStats

GOLDEN_DIR = Path(__file__).parent / "golden"
UPDATE = os.environ.get("UPDATE_GOLDENS") == "1"


def _fixed_stats() -> SampleStats:
    """A deliberately boring, fully-populated SampleStats fixture.

    Every field that the report reads is exercised: tierable + non-tierable
    keys, full idle histogram, per-type sizes for two types, a top-N list
    including a binary key.
    """
    s = SampleStats()
    s.dbsize = 10_000
    s.total_scanned = 1_000
    s.total_probed = 990
    s.total_bytes = 5_000_000

    s.tierable_count = 800
    s.tierable_bytes = 4_000_000
    s.nontierable_count = 190
    s.nontierable_bytes = 1_000_000

    s.tierable_with_idle_count = 800
    s.tierable_with_idle_bytes = 4_000_000
    s.tierable_cold_count = 320
    s.tierable_cold_bytes = 1_800_000

    # Idle histogram — deterministic shape.
    s.idle_counts = {"<1m": 200, "<10m": 180, "<1h": 100, "<1d": 120, "≥1d": 200}
    s.idle_bytes = {
        "<1m": 800_000,
        "<10m": 700_000,
        "<1h": 400_000,
        "<1d": 600_000,
        "≥1d": 1_200_000,
    }

    s.type_sizes = {
        "string": [100, 200, 500, 800, 1_200, 2_000, 4_000, 10_000],
        "hash": [500, 1_000, 2_000, 5_000, 15_000],
    }

    # Top-N with one non-UTF8 key to exercise the decode-for-display path.
    s.top_n_large = [
        (b"session:user:12345", "string", 524_288, 7200),  # 2h idle
        (b"cache:page:home", "string", 262_144, 3600),
        (b"leaderboard:daily", "zset", 131_072, 120),
        (b"\xff\xfe\x00binkey", "string", 65_536, 60),  # non-UTF8
        (b"todo:inbox:u42", "list", 32_768, None),  # idle unknown (shouldn't happen
        # with our fixed fixture having full idle coverage, but the report must
        # still handle None gracefully — exercised here deliberately).
    ]

    s.idle_unsupported_count = 0
    return s


def _fixed_context() -> ReportContext:
    return ReportContext(
        target_url="valkey://example.com:6379",
        timestamp="2026-04-20T12:00:00Z",
        valkey_version="8.0.1",
        maxmemory_policy="allkeys-lru",
        cluster_mode=False,
        warnings=(
            "Sample taken during off-peak (02:00 UTC); re-run at peak for a "
            "representative cold/hot split.",
        ),
    )


def _assert_golden(name: str, actual: str) -> None:
    path = GOLDEN_DIR / name
    if UPDATE or not path.exists():
        path.write_text(actual, encoding="utf-8")
        if not UPDATE:
            pytest.fail(f"Golden {name} created; review and commit.")
        return
    expected = path.read_text(encoding="utf-8")
    assert actual == expected, (
        f"Report output differs from golden {name}. "
        f"If the change is intentional, regenerate with UPDATE_GOLDENS=1."
    )


# ── Markdown ─────────────────────────────────────────────────────────────────


def test_render_markdown_matches_golden() -> None:
    proj = project(
        _fixed_stats(),
        cold_threshold_seconds=1800.0,
        hot_cache_ratio=0.05,
        confidence_level=0.95,
    )
    md = render_markdown(proj, _fixed_context())
    _assert_golden("report.md", md)


def test_render_markdown_without_context_still_renders() -> None:
    # Context is optional; the report drops the header metadata lines
    # and still renders a usable body.
    proj = project(
        _fixed_stats(),
        cold_threshold_seconds=1800.0,
    )
    md = render_markdown(proj)
    assert "# valkey-flash sizing report" in md
    assert "## Headline" in md
    # No "Target:" line because context is absent.
    assert "**Target:**" not in md


def test_render_markdown_under_lfu_explains_why_no_projection() -> None:
    s = SampleStats()
    s.total_scanned = 100
    s.total_probed = 100
    s.tierable_count = 100
    s.tierable_bytes = 10_000
    s.tierable_with_idle_count = 0  # the LFU marker
    s.idle_unsupported_count = 100
    proj = project(s, cold_threshold_seconds=1800.0)
    md = render_markdown(proj)
    assert "Cannot project RAM saving" in md
    assert "LFU" in md
    # Headline doesn't claim a saving number.
    assert "Projected RAM saving" not in md


# ── JSON ─────────────────────────────────────────────────────────────────────


def test_render_json_matches_golden() -> None:
    proj = project(
        _fixed_stats(),
        cold_threshold_seconds=1800.0,
        hot_cache_ratio=0.05,
        confidence_level=0.95,
    )
    js = render_json(proj, _fixed_context())
    _assert_golden("report.json", js)


def test_render_json_is_valid_json() -> None:
    proj = project(
        _fixed_stats(),
        cold_threshold_seconds=1800.0,
    )
    js = render_json(proj)
    # Parse back: catches invalid control chars, broken escaping, etc.
    parsed = json.loads(js)
    assert parsed["schema_version"] == 1
    assert "sampling" in parsed
    assert "projection" in parsed


def test_render_json_has_stable_top_level_keys() -> None:
    # The JSON contract is what downstream automation depends on.
    # Keep this list exhaustive and in order; any rearrangement should
    # fail the test and be a conscious schema-version bump.
    proj = project(_fixed_stats(), cold_threshold_seconds=1800.0)
    js = render_json(proj)
    parsed = json.loads(js)
    assert list(parsed.keys()) == [
        "schema_version",
        "context",
        "parameters",
        "sampling",
        "idle_histogram",
        "per_type_percentiles",
        "top_n_large",
        "projection",
    ]


def test_render_json_encodes_non_utf8_key_safely() -> None:
    proj = project(_fixed_stats(), cold_threshold_seconds=1800.0)
    js = render_json(proj)
    parsed = json.loads(js)
    keys = [entry["key"] for entry in parsed["top_n_large"]]
    # Binary key rendered via backslashreplace.
    assert any("\\x" in k for k in keys)


def test_render_json_lfu_projection_carries_nulls() -> None:
    s = SampleStats()
    s.total_scanned = 100
    s.total_probed = 100
    s.tierable_count = 100
    s.tierable_with_idle_count = 0
    s.idle_unsupported_count = 100
    proj = project(s, cold_threshold_seconds=1800.0)
    parsed = json.loads(render_json(proj))
    assert parsed["projection"]["idle_data_available"] is False
    assert parsed["projection"]["cold_fraction"] is None
