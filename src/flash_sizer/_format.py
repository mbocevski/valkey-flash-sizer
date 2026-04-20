"""Human-readable formatters used by the report layer.

Kept in a private module so other tools can import them, but not
re-exported from the package root — the formatting shape is an
implementation detail of the Markdown/JSON reports, not a public API.
"""

from __future__ import annotations

_SI_UNITS = (
    ("B", 1),
    ("KB", 1024),
    ("MB", 1024**2),
    ("GB", 1024**3),
    ("TB", 1024**4),
    ("PB", 1024**5),
)

_TIME_UNITS = (
    ("d", 86_400),
    ("h", 3_600),
    ("m", 60),
    ("s", 1),
)

# Cap on how many units to show in a duration string — "1d 1h" is
# skimmable, "1d 1h 1m 1s" is noise for report headlines.
_MAX_DURATION_UNITS = 2


def format_bytes(n: int | float) -> str:
    """Render a byte count as a short human-readable string.

    Uses binary multiples (1024) with SI-style labels (KB/MB/GB) because
    that's what Valkey's own `INFO memory` output uses — matching it
    avoids a confusing disagreement in the report. Values under 1 KB
    render as integers; larger values get one decimal.

    `format_bytes(0)` → `"0 B"`, not `"0.0 B"`, so the report reads
    naturally when a bucket is empty.
    """
    if n < 0:
        raise ValueError(f"format_bytes: negative value {n}")
    # Walk high-to-low so the largest unit that gives a value >= 1 wins.
    for label, scale in reversed(_SI_UNITS):
        if n >= scale:
            if scale == 1:
                return f"{int(n)} {label}"
            return f"{n / scale:.1f} {label}"
    # n == 0 (and < 1 is unreachable for integer inputs; keep it defensive).
    return "0 B"


def format_duration(seconds: float) -> str:
    """Render a duration in seconds as a short string like `2h 15m` or `30s`.

    Shows at most the two largest non-zero units so the report stays
    skimmable; `90061` seconds becomes `"1d 1h"`, not `"1d 1h 1m 1s"`.
    """
    if seconds < 0:
        raise ValueError(f"format_duration: negative value {seconds}")
    if seconds == 0:
        return "0s"

    remaining = int(seconds)
    parts: list[str] = []
    for label, scale in _TIME_UNITS:
        if remaining >= scale:
            count, remaining = divmod(remaining, scale)
            parts.append(f"{count}{label}")
        if len(parts) == _MAX_DURATION_UNITS:
            break
    return " ".join(parts) if parts else f"{seconds:g}s"


def format_percent(fraction: float, *, decimals: int = 1) -> str:
    """Render a fraction (0..1) as a percentage string."""
    if not 0.0 <= fraction <= 1.0:
        # Don't silently clamp: the projection computes from Wilson bounds
        # which are always in [0,1], so anything else is a bug upstream.
        raise ValueError(f"format_percent: fraction out of [0,1]: {fraction}")
    return f"{fraction * 100:.{decimals}f} %"
