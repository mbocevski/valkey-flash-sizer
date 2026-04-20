"""Statistical primitives used by the sizer.

Kept deliberately small and stdlib-only — no numpy/scipy — so the tool stays
installable via a single `uvx` fetch without a C-extension build.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass

# Critical z values for common two-sided confidence levels.
# Enough decimal places that callers pinning a specific CI get byte-stable output.
Z_CRIT = {
    0.80: 1.2816,
    0.90: 1.6449,
    0.95: 1.9600,
    0.99: 2.5758,
}


@dataclass(frozen=True)
class ProportionCI:
    """Two-sided confidence interval on a Bernoulli proportion.

    `point` is the observed k/n. `lower`/`upper` are the Wilson-score bounds
    at the given `level`. All values are in [0.0, 1.0].
    """

    point: float
    lower: float
    upper: float
    level: float
    n: int
    k: int


def wilson_ci(k: int, n: int, level: float = 0.95) -> ProportionCI:
    """Wilson-score confidence interval for a proportion k/n.

    Prefer this over the naive normal approximation (p̂ ± z·√(p̂q̂/n)): Wilson
    stays well-defined at p̂ = 0 and p̂ = 1, is well-behaved at small n, and
    has better actual coverage. See Wilson (1927), Agresti & Coull (1998).

    Degenerate cases:
      - n == 0  → (0, 0, 1) at the requested level; the sample is silent.
      - k > n   → ValueError; a sampling bug upstream.
    """
    if n < 0 or k < 0:
        raise ValueError(f"wilson_ci: k and n must be non-negative (got k={k}, n={n})")
    if k > n:
        raise ValueError(f"wilson_ci: k ({k}) cannot exceed n ({n})")
    if level not in Z_CRIT:
        raise ValueError(f"wilson_ci: unsupported level {level}; pick one of {sorted(Z_CRIT)}")
    if n == 0:
        return ProportionCI(point=0.0, lower=0.0, upper=1.0, level=level, n=0, k=0)

    z = Z_CRIT[level]
    p = k / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p + z2 / (2 * n)) / denom
    margin = (z * math.sqrt(p * (1.0 - p) / n + z2 / (4 * n * n))) / denom
    # Clamp to [0, 1]. At k=0 / k=n the algebra produces exactly 0.0 / 1.0 in
    # real arithmetic, but floating-point rounding can leave us at 0.9999…9 or
    # 1.0000…01. Reporting an "upper bound" of 0.9999999999 is silly, so snap.
    lower = 0.0 if k == 0 else max(0.0, center - margin)
    upper = 1.0 if k == n else min(1.0, center + margin)
    return ProportionCI(point=p, lower=lower, upper=upper, level=level, n=n, k=k)


def percentile(values: Sequence[float], p: float) -> float:
    """Linear-interpolated percentile.

    `p` is a fraction in [0, 1] (so `percentile(xs, 0.99)` gives p99).
    Returns 0.0 for an empty input — the sizer's callers expect a number,
    not an exception, and "no samples ⇒ report zero" is honest.
    """
    if not 0.0 <= p <= 1.0:
        raise ValueError(f"percentile: p must be in [0, 1] (got {p})")
    n = len(values)
    if n == 0:
        return 0.0
    if n == 1:
        return float(values[0])

    ordered = sorted(values)
    # index in [0, n-1], linearly interpolated. This matches numpy's
    # "linear" interpolation (default) and pandas' `quantile(interpolation="linear")`.
    idx = p * (n - 1)
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return float(ordered[int(idx)])
    frac = idx - lo
    return float(ordered[lo] + (ordered[hi] - ordered[lo]) * frac)


# ── Idle-time histogram ──────────────────────────────────────────────────────

# Order matters: the sizer reports these buckets in ascending-idle order.
# Units: seconds.
IDLE_BUCKETS: tuple[tuple[str, float], ...] = (
    ("<1m", 60.0),
    ("<10m", 600.0),
    ("<1h", 3600.0),
    ("<1d", 86400.0),
    ("≥1d", float("inf")),
)


def idle_bucket(idle_seconds: float) -> str:
    """Return the label for which `IDLE_BUCKETS` bucket this idle time falls into.

    Buckets are ordered ascending; the first bucket whose upper bound exceeds
    the value wins.
    """
    for label, upper in IDLE_BUCKETS:
        if idle_seconds < upper:
            return label
    # Unreachable — the last bucket's upper bound is +inf — but be explicit.
    return IDLE_BUCKETS[-1][0]
