"""Unit tests for flash_sizer._duration."""

from __future__ import annotations

import pytest

from flash_sizer._duration import parse_duration


@pytest.mark.parametrize(
    "value, expected",
    [
        ("30s", 30.0),
        ("90s", 90.0),
        ("1m", 60.0),
        ("30m", 1800.0),
        ("1h", 3600.0),
        ("2.5h", 9000.0),
        ("1d", 86400.0),
        ("90", 90.0),  # bare number = seconds
        ("0", 0.0),
        ("0.5s", 0.5),
    ],
)
def test_parse_duration_valid(value: str, expected: float) -> None:
    assert parse_duration(value) == pytest.approx(expected)


@pytest.mark.parametrize(
    "value",
    [
        "",  # empty
        "abc",  # not a number
        "30ms",  # compound unit we don't support
        "-30m",  # negative
        "1h 30m",  # two-unit form
        "1x",  # unknown unit
        "1.2.3h",  # malformed number
    ],
)
def test_parse_duration_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        parse_duration(value)
