"""CLI entry point.

Orchestrates `connect → detect → sample → project → render`. Every
non-trivial piece lives in a dedicated module with unit tests; this
file is deliberately thin — it's the glue that a hostile reviewer
can audit in one scroll.
"""

from __future__ import annotations

import datetime as _dt
import logging
import sys
from pathlib import Path

import click

from flash_sizer import __version__
from flash_sizer._duration import parse_duration
from flash_sizer.connect import connect
from flash_sizer.project import project
from flash_sizer.report import ReportContext, render_json, render_markdown
from flash_sizer.sample import sample_keyspace
from flash_sizer.version_detect import detect_server_info

_log = logging.getLogger(__name__)


def _duration_option(ctx: click.Context, param: click.Parameter, value: str) -> float:
    """Click callback that converts a `--cold-threshold` string to seconds."""
    del ctx, param  # unused; Click requires the signature
    try:
        return parse_duration(value)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e


@click.command(
    context_settings={"help_option_names": ["-h", "--help"]},
    help=(
        "Sample a Valkey keyspace and estimate the RAM savings from "
        "tiering cold entries to NVMe via valkey-flash.\n\n"
        "The tool is read-only — only SCAN, MEMORY USAGE, OBJECT IDLETIME, "
        "TYPE, and TTL are issued. Output is a Markdown report (or JSON "
        "with `--format json`) with honest confidence intervals on every "
        "projected number."
    ),
)
@click.version_option(__version__, prog_name="flash-sizer")
@click.argument("url", type=str)
@click.option(
    "--sample",
    "sample_n",
    type=click.IntRange(min=1),
    default=100_000,
    show_default=True,
    help="Number of keys to sample (the report surfaces the sampled fraction).",
)
@click.option(
    "--cold-threshold",
    type=str,
    default="30m",
    show_default=True,
    callback=_duration_option,
    help="Idle-time cutoff above which a key is considered cold. "
    "Accepts `<number><s|m|h|d>` or bare seconds (e.g. `30m`, `1.5h`, `900`).",
)
@click.option(
    "--hot-cache-ratio",
    type=click.FloatRange(min=0.0, max=1.0),
    default=0.05,
    show_default=True,
    help="Fraction of cold-tier bytes expected to stay in the flash hot cache. "
    "Default matches the valkey-flash module's recommended starting point.",
)
@click.option(
    "--confidence",
    type=click.Choice(["0.80", "0.90", "0.95", "0.99"]),
    default="0.95",
    show_default=True,
    help="Two-sided confidence level for the Wilson-score CI on the cold fraction.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    show_default=True,
    help="Report format.",
)
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    default=None,
    help="File to write the report to. Defaults to stdout.",
)
@click.option("--tls", is_flag=True, default=False, help="Use TLS to connect.")
@click.option("--username", type=str, default=None, help="AUTH username.")
@click.option(
    "--password",
    type=str,
    default=None,
    help="AUTH password (avoid shell history — embed in URL instead).",
)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0.1),
    default=10.0,
    show_default=True,
    help="Per-command socket timeout, in seconds.",
)
@click.option(
    "--pipeline-size",
    type=click.IntRange(min=1, max=10_000),
    default=200,
    show_default=True,
    help="Probes pipelined per round-trip. Larger values on high-latency links; "
    "smaller if the server is CPU-bound.",
)
@click.option("-v", "--verbose", is_flag=True, default=False, help="Verbose logging to stderr.")
def main(  # noqa: PLR0913  — CLI entry intentionally wide
    url: str,
    sample_n: int,
    cold_threshold: float,
    hot_cache_ratio: float,
    confidence: str,
    output_format: str,
    output_path: Path | None,
    tls: bool,
    username: str | None,
    password: str | None,
    timeout: float,
    pipeline_size: int,
    verbose: bool,
) -> None:
    """Entry point. `flash-sizer valkey://host:port` from the terminal."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    client = connect(
        url,
        username=username,
        password=password,
        use_tls=tls,
        timeout_seconds=timeout,
    )

    server_info = detect_server_info(client)
    sampled_at = _dt.datetime.now(tz=_dt.timezone.utc).isoformat(timespec="seconds")

    stats = sample_keyspace(
        client,
        target_samples=sample_n,
        cold_threshold_seconds=cold_threshold,
        pipeline_size=pipeline_size,
    )

    projection = project(
        stats,
        cold_threshold_seconds=cold_threshold,
        hot_cache_ratio=hot_cache_ratio,
        confidence_level=float(confidence),
    )

    context = ReportContext(
        target_url=url,
        timestamp=sampled_at,
        valkey_version=server_info.version,
        maxmemory_policy=server_info.maxmemory_policy,
        cluster_mode=server_info.cluster_mode,
        warnings=server_info.warnings,
    )

    if output_format == "json":
        report = render_json(projection, context)
    else:
        report = render_markdown(projection, context)

    if output_path is None:
        click.echo(report, nl=False)
    else:
        output_path.write_text(report, encoding="utf-8")
