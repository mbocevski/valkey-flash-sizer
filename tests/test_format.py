"""Unit tests for the format helpers in flash_sizer._format."""

from __future__ import annotations

import pytest

from flash_sizer._format import format_bytes, format_duration, format_percent


@pytest.mark.parametrize(
    "n, expected",
    [
        (0, "0 B"),
        (1, "1 B"),
        (1023, "1023 B"),
        (1024, "1.0 KB"),
        (1536, "1.5 KB"),
        (1024 * 1024, "1.0 MB"),
        (int(2.5 * 1024**3), "2.5 GB"),
        (1024**4, "1.0 TB"),
        (1024**5, "1.0 PB"),
    ],
)
def test_format_bytes_common_values(n: int, expected: str) -> None:
    assert format_bytes(n) == expected


def test_format_bytes_rejects_negative() -> None:
    with pytest.raises(ValueError):
        format_bytes(-1)


@pytest.mark.parametrize(
    "seconds, expected",
    [
        (0, "0s"),
        (1, "1s"),
        (59, "59s"),
        (60, "1m"),
        (61, "1m 1s"),
        (3600, "1h"),
        (3661, "1h 1m"),  # seconds truncated because only 2 largest shown
        (86400, "1d"),
        (90061, "1d 1h"),  # same truncation
    ],
)
def test_format_duration_common_values(seconds: int, expected: str) -> None:
    assert format_duration(seconds) == expected


def test_format_duration_rejects_negative() -> None:
    with pytest.raises(ValueError):
        format_duration(-1)


@pytest.mark.parametrize(
    "fraction, decimals, expected",
    [
        (0.0, 1, "0.0 %"),
        (0.5, 1, "50.0 %"),
        (0.1234, 2, "12.34 %"),
        (1.0, 1, "100.0 %"),
    ],
)
def test_format_percent_common_values(fraction: float, decimals: int, expected: str) -> None:
    assert format_percent(fraction, decimals=decimals) == expected


def test_format_percent_rejects_out_of_range() -> None:
    with pytest.raises(ValueError):
        format_percent(1.5)
    with pytest.raises(ValueError):
        format_percent(-0.01)
