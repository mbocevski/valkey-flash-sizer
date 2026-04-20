"""Unit tests for the Click CLI entry point.

We stub out `connect` and `detect_server_info` so the tests run without
a live Valkey. End-to-end wiring against a real server is covered in
`test_integration.py`.
"""

from __future__ import annotations

import json
import re
from typing import Any
from unittest.mock import patch

from click.testing import CliRunner

from flash_sizer.main import main
from flash_sizer.version_detect import ServerInfo
from tests.test_sample import FakeClient, _mk_store


def _make_fake_client() -> FakeClient:
    # A tiny, tierable-heavy keyspace so the CLI produces a real report.
    return FakeClient(
        {
            **_mk_store(
                [
                    (b"hot:1", "string", 100, 10, -1),
                    (b"hot:2", "string", 150, 20, -1),
                    (b"cold:1", "string", 500, 3600, -1),
                    (b"cold:2", "hash", 800, 7200, -1),
                ]
            ),
        }
    )


def _fake_server_info() -> ServerInfo:
    return ServerInfo(
        version="8.0.1",
        maxmemory_policy="allkeys-lru",
        cluster_mode=False,
        warnings=(),
    )


def _run(*argv: str, client: Any = None) -> Any:
    """Invoke the CLI with connect+detect patched to the fake."""
    runner = CliRunner()
    with (
        patch("flash_sizer.main.connect", return_value=client or _make_fake_client()),
        patch("flash_sizer.main.detect_server_info", return_value=_fake_server_info()),
    ):
        return runner.invoke(main, list(argv))


# ── Happy paths ──────────────────────────────────────────────────────────────


def test_cli_default_renders_markdown_to_stdout() -> None:
    result = _run("valkey://example.com:6379")
    assert result.exit_code == 0, result.output
    assert "# valkey-flash sizing report" in result.output
    assert "## Headline" in result.output
    assert "**Target:** `valkey://example.com:6379`" in result.output


def test_cli_json_format_emits_valid_json() -> None:
    result = _run("valkey://example.com:6379", "--format", "json")
    assert result.exit_code == 0, result.output
    parsed = json.loads(result.output)
    assert parsed["schema_version"] == 1
    assert parsed["context"]["target_url"] == "valkey://example.com:6379"


def test_cli_writes_to_output_file(tmp_path: Any) -> None:
    out = tmp_path / "report.md"
    result = _run("valkey://x:6379", "--output", str(out))
    assert result.exit_code == 0, result.output
    assert out.exists()
    body = out.read_text(encoding="utf-8")
    assert "# valkey-flash sizing report" in body
    # stdout stays empty when --output is set.
    assert result.output == ""


def test_cli_accepts_cold_threshold_durations() -> None:
    # `1h` → 3600s. The projection's cold_threshold_seconds should
    # reflect this (report shows "idle for at least 1h").
    result = _run("valkey://x:6379", "--cold-threshold", "1h")
    assert result.exit_code == 0, result.output
    assert re.search(r"idle for at least\s+1h", result.output)


# ── Failure paths ────────────────────────────────────────────────────────────


def test_cli_rejects_unparseable_duration() -> None:
    result = _run("valkey://x:6379", "--cold-threshold", "nope")
    assert result.exit_code == 2  # Click's usage-error exit
    assert "cannot parse" in result.output.lower() or "invalid" in result.output.lower()


def test_cli_rejects_out_of_range_hot_cache_ratio() -> None:
    result = _run("valkey://x:6379", "--hot-cache-ratio", "1.5")
    assert result.exit_code == 2
    assert "1.5" in result.output


def test_cli_surfaces_version_option() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "flash-sizer" in result.output


def test_cli_help_mentions_readonly_scope() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    # The help must reassure users the tool doesn't mutate their data.
    assert "read-only" in result.output.lower()


# ── Warnings surfacing ───────────────────────────────────────────────────────


def test_cli_warnings_from_server_info_appear_in_report() -> None:
    lfu_info = ServerInfo(
        version="8.0.1",
        maxmemory_policy="allkeys-lfu",
        cluster_mode=False,
        warnings=("LFU policy — OBJECT IDLETIME not tracked",),
    )
    runner = CliRunner()
    with (
        patch("flash_sizer.main.connect", return_value=_make_fake_client()),
        patch("flash_sizer.main.detect_server_info", return_value=lfu_info),
    ):
        result = runner.invoke(main, ["valkey://x:6379"])
    assert result.exit_code == 0, result.output
    assert "## Warnings" in result.output
    assert "LFU" in result.output
