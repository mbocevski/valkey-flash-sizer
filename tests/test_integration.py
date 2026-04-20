"""End-to-end integration test against a real Valkey.

Runs only when `VALKEY_URL` is set in the environment (CI's service
container provides it). Seeds a deterministic mix of hot and cold keys
using `RESTORE ... IDLETIME <seconds>` — the only way to plant a key
with a specific idle time without waiting real wall-clock.
"""

from __future__ import annotations

import contextlib
import json
import os
import uuid

import pytest
from valkey import Valkey

from flash_sizer.connect import connect
from flash_sizer.project import project
from flash_sizer.report import ReportContext, render_json, render_markdown
from flash_sizer.sample import sample_keyspace
from flash_sizer.version_detect import detect_server_info

VALKEY_URL = os.environ.get("VALKEY_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(VALKEY_URL is None, reason="VALKEY_URL not set"),
]


@pytest.fixture
def run_id() -> str:
    """Unique per-test prefix so concurrent CI jobs on the same instance
    don't collide on key names (one day, maybe)."""
    return uuid.uuid4().hex[:8]


@pytest.fixture
def seeded(run_id: str):
    """Seed a known mix of hot + cold keys. Cleans up after the test."""
    assert VALKEY_URL is not None
    client = Valkey.from_url(VALKEY_URL, decode_responses=False)
    prefix = f"flash-sizer-test:{run_id}:"
    created: list[bytes] = []

    def plant(key_suffix: str, value: bytes, idle_seconds: int) -> bytes:
        """SET → DUMP → DEL → RESTORE with specific idle time."""
        key = f"{prefix}{key_suffix}".encode()
        client.set(key, value)
        dump = client.dump(key)
        client.delete(key)
        # RESTORE replace=True so a previous test leak doesn't block the run.
        client.restore(key, 0, dump, replace=True, idletime=idle_seconds)
        created.append(key)
        return key

    # 20 cold strings (idle=3600, above our 30m threshold).
    for i in range(20):
        plant(f"cold:str:{i}", b"x" * 100, idle_seconds=3600)
    # 15 hot strings (idle=0).
    for i in range(15):
        plant(f"hot:str:{i}", b"y" * 80, idle_seconds=0)
    # 5 cold hashes — bigger payload so tierable_cold_bytes is non-trivial.
    for i in range(5):
        plant(f"cold:hash:{i}", b"h" * 500, idle_seconds=7200)

    yield prefix

    # Cleanup: delete every key we planted. Best-effort so a partial
    # run doesn't leak on assertion failure.
    for k in created:
        with contextlib.suppress(Exception):
            client.delete(k)
    with contextlib.suppress(Exception):
        client.close()


def test_end_to_end_against_real_valkey(seeded: str) -> None:  # noqa: ARG001 - fixture seeds server
    assert VALKEY_URL is not None
    client = connect(VALKEY_URL, timeout_seconds=5.0)

    info = detect_server_info(client)
    # CI service container is plain single-node with default policy.
    assert info.cluster_mode is False
    # Valkey container labels itself `valkey_version`; fall back just in case.
    assert info.version is not None

    stats = sample_keyspace(
        client,
        target_samples=1000,
        cold_threshold_seconds=1800.0,
        pipeline_size=100,
    )
    assert stats.total_probed > 0, "should probe at least the keys we seeded"
    # Our 20+15+5 seeded keys are tierable; others on the instance may
    # or may not be, so just assert we saw at least what we planted.
    assert stats.tierable_count >= 40
    # Cold keys: 20 cold:str + 5 cold:hash = 25. Non-seeded cold keys
    # could raise this above 25, but 25 is the floor.
    assert stats.tierable_cold_count >= 25

    projection = project(
        stats,
        cold_threshold_seconds=1800.0,
        hot_cache_ratio=0.05,
        confidence_level=0.95,
    )
    assert projection.idle_data_available
    assert projection.cold_fraction_ci is not None
    # With ~40-plus observations, 25-ish cold, Wilson point is around 0.6
    # but the CI range here is for a real sample that may include
    # non-test keys — just sanity-check the interval is well-formed.
    assert 0.0 < projection.cold_fraction_ci.point < 1.0
    assert projection.cold_fraction_ci.lower < projection.cold_fraction_ci.point
    assert projection.cold_fraction_ci.upper > projection.cold_fraction_ci.point

    ctx = ReportContext(
        target_url=VALKEY_URL,
        valkey_version=info.version,
        maxmemory_policy=info.maxmemory_policy,
        cluster_mode=info.cluster_mode,
        warnings=info.warnings,
    )
    md = render_markdown(projection, ctx)
    assert "## Headline" in md
    assert "Projected RAM saving" in md
    assert "## Known biases" in md

    js = render_json(projection, ctx)
    parsed = json.loads(js)
    assert parsed["schema_version"] == 1
    assert parsed["projection"]["idle_data_available"] is True
    assert parsed["sampling"]["tierable_cold_count"] >= 25
