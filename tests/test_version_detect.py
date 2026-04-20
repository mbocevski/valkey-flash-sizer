"""Unit tests for flash_sizer.version_detect."""

from __future__ import annotations

from typing import Any

from flash_sizer.version_detect import detect_server_info


class FakeInfoClient:
    """Minimal fake. Each attribute is a dict the corresponding command
    returns; setting to `None` makes the command raise, simulating an
    ACL/rename-command setup."""

    def __init__(
        self,
        *,
        server_info: dict[str, Any] | None = None,
        cluster_info: dict[str, Any] | None = None,
        policy: dict[str, Any] | None = None,
    ) -> None:
        self._server_info = server_info
        self._cluster_info = cluster_info
        self._policy = policy

    def info(self, section: str) -> dict[str, Any]:
        if section == "server":
            if self._server_info is None:
                raise RuntimeError("info server refused")
            return self._server_info
        if section == "cluster":
            if self._cluster_info is None:
                raise RuntimeError("info cluster refused")
            return self._cluster_info
        raise RuntimeError(f"unexpected section {section}")

    def config_get(self, name: str) -> dict[str, Any]:
        if self._policy is None:
            raise RuntimeError("CONFIG GET refused")
        return self._policy


def test_detect_returns_version_policy_cluster() -> None:
    c = FakeInfoClient(
        server_info={"valkey_version": "8.0.1"},
        cluster_info={"cluster_enabled": 0},
        policy={"maxmemory-policy": "allkeys-lru"},
    )
    info = detect_server_info(c)
    assert info.version == "8.0.1"
    assert info.maxmemory_policy == "allkeys-lru"
    assert info.cluster_mode is False
    assert info.warnings == ()


def test_detect_lfu_policy_emits_warning() -> None:
    c = FakeInfoClient(
        server_info={"valkey_version": "8.0.1"},
        cluster_info={"cluster_enabled": 0},
        policy={"maxmemory-policy": "allkeys-lfu"},
    )
    info = detect_server_info(c)
    assert info.maxmemory_policy == "allkeys-lfu"
    assert len(info.warnings) == 1
    assert "LFU" in info.warnings[0]


def test_detect_unknown_policy_warns_conservatively() -> None:
    c = FakeInfoClient(
        server_info={"valkey_version": "9.0.0"},
        cluster_info={"cluster_enabled": 0},
        policy={"maxmemory-policy": "volatile-random"},  # not in our LRU allow-list
    )
    info = detect_server_info(c)
    assert len(info.warnings) == 1
    assert "volatile-random" in info.warnings[0]


def test_detect_cluster_enabled_flag() -> None:
    c = FakeInfoClient(
        server_info={"valkey_version": "8.0.1"},
        cluster_info={"cluster_enabled": 1},
        policy={"maxmemory-policy": "noeviction"},
    )
    info = detect_server_info(c)
    assert info.cluster_mode is True


def test_detect_falls_back_to_redis_version_field() -> None:
    # Older servers reporting as redis, not valkey.
    c = FakeInfoClient(
        server_info={"redis_version": "7.2.0"},
        cluster_info={"cluster_enabled": 0},
        policy={"maxmemory-policy": "allkeys-lru"},
    )
    info = detect_server_info(c)
    assert info.version == "7.2.0"


def test_detect_all_probes_failing_yields_all_nones() -> None:
    c = FakeInfoClient()  # every attribute None → every command raises
    info = detect_server_info(c)
    assert info.version is None
    assert info.maxmemory_policy is None
    assert info.cluster_mode is None
    assert info.warnings == ()


def test_detect_cluster_info_string_enabled() -> None:
    # Some client configs return cluster_enabled as a string.
    c = FakeInfoClient(
        server_info={"valkey_version": "8.0.1"},
        cluster_info={"cluster_enabled": "yes"},
        policy={"maxmemory-policy": "allkeys-lru"},
    )
    info = detect_server_info(c)
    assert info.cluster_mode is True
