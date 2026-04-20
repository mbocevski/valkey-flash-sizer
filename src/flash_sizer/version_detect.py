"""Probe the target server for the metadata the report header needs.

One round of INFO + CONFIG GET at the start of a run. Everything is
best-effort — a server that rejects CONFIG GET under `rename-command`
or an ACL still gets a usable report, just without the policy warning.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# maxmemory-policy values for which OBJECT IDLETIME is meaningful. Every
# other value (LFU family, or noeviction with no LRU updates) leaves the
# idle timer stale or missing.
_LRU_POLICIES = frozenset({"allkeys-lru", "volatile-lru", "noeviction", ""})
_LFU_POLICIES = frozenset({"allkeys-lfu", "volatile-lfu"})


@dataclass(frozen=True)
class ServerInfo:
    """Whatever we could glean from the server. Fields may be None if the
    probe failed or the command was unavailable."""

    version: str | None
    maxmemory_policy: str | None
    cluster_mode: bool | None
    # Warnings to pre-pend to the report. These are the one-shot "hey,
    # your idle data will be approximate" hints that would be noise as
    # per-probe log lines.
    warnings: tuple[str, ...]


def detect_server_info(client: Any) -> ServerInfo:
    """Return best-effort server metadata for the report header.

    Accepts any valkey-py-compatible client (single-node `Valkey` or
    `ValkeyCluster`). On cluster clients INFO/CONFIG route to a single
    arbitrary primary; the policy and version are expected to be
    cluster-uniform so that's fine for our purposes.
    """
    version = _probe_version(client)
    policy = _probe_policy(client)
    cluster_mode = _probe_cluster_mode(client)
    warnings = _derive_warnings(policy)
    return ServerInfo(
        version=version,
        maxmemory_policy=policy,
        cluster_mode=cluster_mode,
        warnings=warnings,
    )


def _probe_version(client: Any) -> str | None:
    try:
        info = client.info("server")
    except Exception:
        return None
    if not isinstance(info, dict):
        return None
    v = info.get("valkey_version") or info.get("redis_version")
    return str(v) if v is not None else None


def _probe_policy(client: Any) -> str | None:
    try:
        result = client.config_get("maxmemory-policy")
    except Exception:
        return None
    if not isinstance(result, dict):
        return None
    v = result.get("maxmemory-policy")
    return str(v) if v is not None else None


def _probe_cluster_mode(client: Any) -> bool | None:
    try:
        info = client.info("cluster")
    except Exception:
        return None
    if not isinstance(info, dict):
        return None
    enabled = info.get("cluster_enabled")
    if enabled is None:
        return None
    # valkey-py returns ints or strings depending on decode settings.
    try:
        return bool(int(enabled))
    except (TypeError, ValueError):
        return str(enabled).lower() in ("1", "true", "yes")


def _derive_warnings(policy: str | None) -> tuple[str, ...]:
    if policy is None:
        return ()
    if policy in _LFU_POLICIES:
        return (
            f"`maxmemory-policy` is `{policy}` — Valkey tracks access "
            "frequency (LFU), not idle time. `OBJECT IDLETIME` returns "
            "an error under LFU, so the projection cannot compute a "
            "cold fraction. Switch to an LRU-family policy and re-run.",
        )
    if policy not in _LRU_POLICIES:
        # Newer policies we don't know about (e.g. `volatile-ttl`). Idle
        # data may or may not be meaningful; warn conservatively.
        return (
            f"`maxmemory-policy` is `{policy}`; idle-time readings may "
            "be approximate or unavailable. If the projection reports "
            "no cold keys, the policy is likely the reason.",
        )
    return ()
