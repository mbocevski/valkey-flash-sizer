"""Report rendering: turn a `Projection` into a Markdown or JSON string.

Two renderers. Both are pure functions over `Projection` + optional
`ReportContext` so tests can assert on exact output without spinning
up a Valkey. Goldfile tests (`tests/golden/*`) catch accidental copy
drift — a changed heading or rephrased caveat fails CI until the
golden is regenerated with `UPDATE_GOLDENS=1`.

Design choices:

- Byte keys are decoded with `errors="backslashreplace"` so a binary
  key renders as a visible escape sequence instead of blowing up.
- Every headline number is paired with `(sampled N of M total, ±X % CI)`.
  No naked marketing numbers — that's the whole product.
- The "Known biases" section is appended verbatim from a module-level
  string constant. Editing it is one diff; the golden tests then fail
  so the author has to acknowledge the copy change.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from flash_sizer._format import format_bytes, format_duration, format_percent
from flash_sizer.project import Projection, RangeEstimate

JSON_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ReportContext:
    """Run-level metadata the CLI collects and passes to the renderers.

    Everything is optional; if the CLI doesn't know the Valkey version
    or maxmemory policy, those lines drop out of the Markdown cleanly
    and the JSON gets a `null`.
    """

    target_url: str | None = None
    timestamp: str | None = None  # ISO-8601 at the moment of sampling
    valkey_version: str | None = None
    maxmemory_policy: str | None = None
    cluster_mode: bool | None = None
    # Free-form warning strings collected during the run; each one
    # becomes a bullet under "Warnings" at the top of the report.
    warnings: tuple[str, ...] = ()


# ── Markdown renderer ────────────────────────────────────────────────────────


def render_markdown(projection: Projection, context: ReportContext | None = None) -> str:
    """Render a Projection as a Markdown report.

    The output is designed to paste legibly into a GitHub issue — that's
    where users will share it when asking "is this a good candidate for
    valkey-flash?"
    """
    ctx = context or ReportContext()
    lines: list[str] = []
    lines.append("# valkey-flash sizing report")
    lines.append("")
    _append_context(lines, ctx)
    _append_warnings(lines, ctx)
    _append_headline(lines, projection)
    _append_sampling(lines, projection)
    _append_idle_histogram(lines, projection)
    _append_per_type(lines, projection)
    _append_top_keys(lines, projection)
    _append_biases(lines, projection)
    # Trailing newline keeps `cat report.md` tidy and diffs clean.
    return "\n".join(lines) + "\n"


def _append_context(lines: list[str], ctx: ReportContext) -> None:
    parts: list[str] = []
    if ctx.target_url:
        parts.append(f"**Target:** `{ctx.target_url}`")
    if ctx.timestamp:
        parts.append(f"**Sampled at:** {ctx.timestamp}")
    if ctx.valkey_version:
        parts.append(f"**Valkey version:** {ctx.valkey_version}")
    if ctx.maxmemory_policy:
        parts.append(f"**maxmemory-policy:** `{ctx.maxmemory_policy}`")
    if ctx.cluster_mode is not None:
        parts.append(f"**Cluster:** {'yes' if ctx.cluster_mode else 'no'}")
    if parts:
        lines.append("  \n".join(parts))
        lines.append("")


def _append_warnings(lines: list[str], ctx: ReportContext) -> None:
    if not ctx.warnings:
        return
    lines.append("## Warnings")
    lines.append("")
    for w in ctx.warnings:
        lines.append(f"- {w}")
    lines.append("")


def _append_headline(lines: list[str], p: Projection) -> None:
    lines.append("## Headline")
    lines.append("")
    if not p.idle_data_available:
        lines.append(
            "**Cannot project RAM saving:** no sampled key reported an idle time. "
            "The server's `maxmemory-policy` is likely LFU, which does not track "
            "`OBJECT IDLETIME`. Switch to an LRU-family policy and re-run, or "
            "sample at a time when LRU stats are fresh."
        )
        lines.append("")
        return

    assert p.cold_fraction_ci is not None  # follows from idle_data_available
    ci = p.cold_fraction_ci
    ci_pct = _level_pct(p.confidence_level)

    lines.append(
        f"**Projected RAM saving:** {format_bytes(p.projected_ram_saving.point)} "
        f"({format_bytes(p.projected_ram_saving.lower)}–"
        f"{format_bytes(p.projected_ram_saving.upper)}, {ci_pct} CI)."
    )
    lines.append("")
    lines.append(
        f"About **{format_percent(ci.point)}** of your tierable working set has been "
        f"idle for at least {format_duration(p.cold_threshold_seconds)} — the prime "
        f"migration candidates. "
        f"CI: {format_percent(ci.lower)} to {format_percent(ci.upper)} "
        f"({ci_pct}, Wilson score, n={ci.n})."
    )
    lines.append("")
    lines.append(
        f"Estimate assumes a hot-cache fraction of "
        f"{format_percent(p.hot_cache_ratio)} on the flash tier (module default). "
        "See Known biases below for what this projection does and does not account for."
    )
    lines.append("")


def _append_sampling(lines: list[str], p: Projection) -> None:
    s = p.stats
    lines.append("## Sampling")
    lines.append("")
    if s.dbsize > 0:
        fraction = s.total_scanned / s.dbsize
        scanned_line = (
            f"- Keys scanned: **{s.total_scanned:,}** of {s.dbsize:,} total "
            f"(~{format_percent(min(fraction, 1.0), decimals=2)})"
        )
    else:
        scanned_line = f"- Keys scanned: **{s.total_scanned:,}** (DBSIZE unavailable)"
    lines.append(scanned_line)
    lines.append(f"- Keys probed successfully: {s.total_probed:,}")
    skipped = s.total_scanned - s.total_probed
    lines.append(f"- Keys skipped (vanished between SCAN and probe): {skipped:,}")
    lines.append(
        f"- Tierable (string/hash/list/zset): {s.tierable_count:,} keys, "
        f"{format_bytes(s.tierable_bytes)}"
    )
    lines.append(
        f"- Non-tierable (set/stream/pubsub/etc.): {s.nontierable_count:,} keys, "
        f"{format_bytes(s.nontierable_bytes)}"
    )
    if s.idle_unsupported_count > 0:
        lines.append(
            f"- Idle-time unavailable for {s.idle_unsupported_count:,} probes "
            "(LFU policy rejects `OBJECT IDLETIME`)"
        )
    if p.scale_factor > 1.0:
        lines.append(
            f"- Scale factor applied to byte totals: ×{p.scale_factor:.2f} "
            f"(sample size {s.total_scanned:,} / dbsize {s.dbsize:,})"
        )
    lines.append("")


def _append_idle_histogram(lines: list[str], p: Projection) -> None:
    s = p.stats
    if not p.idle_data_available:
        return
    lines.append("## Idle-time distribution")
    lines.append("")
    lines.append("| Idle | Keys | Bytes |")
    lines.append("|---|---:|---:|")
    # `IDLE_BUCKETS` order is preserved in the dict because sample.py
    # zero-fills via the same tuple.
    for label, count in s.idle_counts.items():
        bytes_val = s.idle_bytes.get(label, 0)
        lines.append(f"| {label} | {count:,} | {format_bytes(bytes_val)} |")
    lines.append("")


def _append_per_type(lines: list[str], p: Projection) -> None:
    if not p.per_type_percentiles:
        return
    lines.append("## Value sizes (tierable types)")
    lines.append("")
    lines.append("| Type | Count | p50 | p99 | Max |")
    lines.append("|---|---:|---:|---:|---:|")
    for row in p.per_type_percentiles:
        lines.append(
            f"| {row.type} | {row.count:,} | {format_bytes(row.p50)} | "
            f"{format_bytes(row.p99)} | {format_bytes(row.max)} |"
        )
    lines.append("")


def _append_top_keys(lines: list[str], p: Projection) -> None:
    if not p.stats.top_n_large:
        return
    lines.append(f"## Largest keys in the sample (top {len(p.stats.top_n_large)})")
    lines.append("")
    lines.append("| Key | Type | Bytes | Idle |")
    lines.append("|---|---|---:|---:|")
    for key, type_str, size, idle in p.stats.top_n_large:
        idle_str = format_duration(idle) if idle is not None else "—"
        key_repr = _decode_key_for_display(key)
        lines.append(f"| `{key_repr}` | {type_str} | {format_bytes(size)} | {idle_str} |")
    lines.append("")


def _append_biases(lines: list[str], p: Projection) -> None:
    lines.append("## Known biases")
    lines.append("")
    lines.append(
        "- **SCAN sampling** visits every shard (in cluster mode) but does not "
        "guarantee strict stratification. On heavily-skewed keyspaces the sample "
        "may over-represent the largest shards."
    )
    lines.append(
        "- **`OBJECT IDLETIME` is approximate.** Under `allkeys-lru` / "
        "`volatile-lru` policies the idle timer is updated lazily during "
        "eviction, not on every access. Under LFU it is not maintained at all."
    )
    lines.append(
        "- **Size-neutrality assumption.** Cold-bytes and RAM-saving bounds "
        "scale the fraction CI by observed tierable bytes. This assumes cold "
        "keys have the same average byte size as hot keys; real workloads "
        "often skew — cold keys tend to be larger."
    )
    lines.append(
        "- **Workload drift.** A sample at 02:00 looks nothing like 14:00. "
        "Run this during peak and off-peak, compare the reports."
    )
    lines.append(
        "- **Non-tierable bytes** (sets, streams, pub-sub buffers, replication "
        "backlog) are counted in `total_bytes` but excluded from the migration "
        "projection — `valkey-flash` has no tiered equivalent for them today."
    )
    if p.idle_data_available:
        assert p.cold_fraction_ci is not None
        lines.append(
            f"- **Confidence level** is {_level_pct(p.confidence_level)} "
            f"(Wilson score, two-sided), n={p.cold_fraction_ci.n}."
        )
    lines.append("")


# ── JSON renderer ────────────────────────────────────────────────────────────


def render_json(
    projection: Projection,
    context: ReportContext | None = None,
    *,
    indent: int | None = 2,
) -> str:
    """Render a Projection as a JSON string.

    The shape is explicitly constructed (not `dataclasses.asdict`) so
    the JSON contract is separate from the Python dataclass layout —
    we can rearrange internals without breaking consumers.
    """
    doc = _to_json_doc(projection, context or ReportContext())
    # `sort_keys=False` because explicit field order reads better in the
    # published schema; we control the order in `_to_json_doc`.
    return json.dumps(doc, indent=indent, sort_keys=False, ensure_ascii=False)


def _to_json_doc(p: Projection, ctx: ReportContext) -> dict[str, Any]:
    s = p.stats
    return {
        "schema_version": JSON_SCHEMA_VERSION,
        "context": {
            "target_url": ctx.target_url,
            "timestamp": ctx.timestamp,
            "valkey_version": ctx.valkey_version,
            "maxmemory_policy": ctx.maxmemory_policy,
            "cluster_mode": ctx.cluster_mode,
            "warnings": list(ctx.warnings),
        },
        "parameters": {
            "cold_threshold_seconds": p.cold_threshold_seconds,
            "hot_cache_ratio": p.hot_cache_ratio,
            "confidence_level": p.confidence_level,
        },
        "sampling": {
            "dbsize": s.dbsize,
            "total_scanned": s.total_scanned,
            "total_probed": s.total_probed,
            "total_bytes": s.total_bytes,
            "scale_factor": p.scale_factor,
            "tierable_count": s.tierable_count,
            "tierable_bytes": s.tierable_bytes,
            "nontierable_count": s.nontierable_count,
            "nontierable_bytes": s.nontierable_bytes,
            "tierable_with_idle_count": s.tierable_with_idle_count,
            "tierable_with_idle_bytes": s.tierable_with_idle_bytes,
            "tierable_cold_count": s.tierable_cold_count,
            "tierable_cold_bytes": s.tierable_cold_bytes,
            "idle_unsupported_count": s.idle_unsupported_count,
        },
        "idle_histogram": {
            label: {"count": count, "bytes": s.idle_bytes.get(label, 0)}
            for label, count in s.idle_counts.items()
        },
        "per_type_percentiles": [
            {
                "type": row.type,
                "count": row.count,
                "p50_bytes": row.p50,
                "p99_bytes": row.p99,
                "max_bytes": row.max,
            }
            for row in p.per_type_percentiles
        ],
        "top_n_large": [
            {
                "key": _decode_key_for_display(k),
                "type": t,
                "bytes": size,
                "idle_seconds": idle,
            }
            for (k, t, size, idle) in s.top_n_large
        ],
        "projection": {
            "idle_data_available": p.idle_data_available,
            "cold_fraction": _ci_to_json(p.cold_fraction_ci),
            "projected_total_bytes": p.projected_total_bytes,
            "projected_tierable_bytes": p.projected_tierable_bytes,
            "projected_nontierable_bytes": p.projected_nontierable_bytes,
            "projected_cold_bytes": _range_to_json(p.projected_cold_bytes),
            "projected_ram_saving_bytes": _range_to_json(p.projected_ram_saving),
        },
    }


def _ci_to_json(ci: Any | None) -> dict[str, Any] | None:
    if ci is None:
        return None
    return {
        "point": ci.point,
        "lower": ci.lower,
        "upper": ci.upper,
        "level": ci.level,
        "n": ci.n,
        "k": ci.k,
    }


def _range_to_json(r: RangeEstimate) -> dict[str, int]:
    return {"point": r.point, "lower": r.lower, "upper": r.upper}


# ── Helpers ──────────────────────────────────────────────────────────────────


def _decode_key_for_display(key: bytes) -> str:
    """Render bytes as a printable, round-trippable string.

    `repr(b)` handles every rough edge: non-UTF-8 bytes (`\\xff`), embedded
    NULs (`\\x00`), backslashes, control chars, quotes. The leading `b'`
    and trailing `'` are stripped so the result reads as a plain string
    in Markdown tables while still being a valid Python bytes-literal
    inside if the reader wants to copy-paste it back into code.
    """
    # repr always uses single quotes unless the key contains a `'` itself,
    # in which case Python switches to double quotes. Strip either form.
    r = repr(key)
    if r.startswith("b'") and r.endswith("'"):
        return r[2:-1]
    if r.startswith('b"') and r.endswith('"'):
        return r[2:-1]
    return r  # Defensive; never expected to hit.


def _level_pct(level: float) -> str:
    """`0.95` → `"95 %"`. Avoids float rounding noise in the string."""
    return f"{int(round(level * 100))} %"
