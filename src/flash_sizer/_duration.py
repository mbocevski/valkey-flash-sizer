"""Parse short duration strings like `30m`, `1h`, `90s`, `1.5d`.

Kept separate from `_format.py` so the formatting path and the parsing
path don't drift out of sync — rendering `format_duration(1800)` as
`30m` and then failing to `parse_duration("30m")` would be an ugly
round-trip bug.

Accepts:
  - a bare integer or float string → interpreted as seconds
  - `<number><unit>` where unit ∈ {s, m, h, d}; number may be fractional
  - no whitespace, single-unit only (we don't accept `1h 30m`)

Rejects anything else with a `ValueError` that mentions the input, so
the CLI can surface a clear error to the user.
"""

from __future__ import annotations

import re

_UNIT_SECONDS = {
    "s": 1,
    "m": 60,
    "h": 3_600,
    "d": 86_400,
}

_PATTERN = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([smhd]?)\s*$")


def parse_duration(value: str) -> float:
    """`"30m"` → 1800.0; `"90"` → 90.0; invalid → ValueError."""
    m = _PATTERN.match(value)
    if not m:
        raise ValueError(
            f"parse_duration: cannot parse {value!r}; "
            f"expected a number or `<number><s|m|h|d>`, e.g. `30m`, `1.5h`, `90`"
        )
    num_str, unit = m.group(1), m.group(2) or "s"
    num = float(num_str)
    if num < 0:
        # Regex rejects leading minus; this path is defensive.
        raise ValueError(f"parse_duration: negative duration {value!r}")
    return num * _UNIT_SECONDS[unit]
