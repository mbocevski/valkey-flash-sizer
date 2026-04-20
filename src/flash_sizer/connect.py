"""URL → connected valkey-py client, with cluster auto-detection.

The CLI takes a single URL and we pick the right client class based on
what the server reports. The alternative — requiring the user to know
whether their own deployment is clustered — is an adoption-tool failure
mode.

Flow:
  1. Open a single-node `Valkey` client.
  2. `INFO cluster` — if `cluster_enabled=0`, we're done.
  3. Otherwise close the single-node client and open a `ValkeyCluster`
     using the same URL. valkey-py's cluster client discovers peers
     from the `CLUSTER SLOTS` of the seed node.
"""

from __future__ import annotations

import logging
from typing import Any

from valkey import Valkey
from valkey.cluster import ValkeyCluster

_log = logging.getLogger(__name__)


def connect(
    url: str,
    *,
    username: str | None = None,
    password: str | None = None,
    use_tls: bool = False,
    timeout_seconds: float = 10.0,
) -> Any:
    """Return a connected client. Auto-upgrades to `ValkeyCluster` when
    the seed node reports `cluster_enabled=1`.

    Keys land in the client as raw bytes (`decode_responses=False`) —
    the sampler relies on this. Changing it silently would break binary
    keys in a way the integration test might not catch if the fixture
    happens to use only ASCII.

    TLS is selected via the URL scheme (`valkeys://` / `rediss://`) per
    valkey-py convention; the `--tls` flag rewrites `valkey://` →
    `valkeys://` (and `redis://` → `rediss://`) so users who don't know
    the scheme convention still get TLS when they ask for it.
    """
    url = _apply_tls_scheme(url, use_tls)
    single = Valkey.from_url(
        url,
        username=username,
        password=password,
        socket_timeout=timeout_seconds,
        socket_connect_timeout=timeout_seconds,
        decode_responses=False,
    )
    # Probe cluster mode from the seed node. A refused INFO is fatal here
    # because without it we can't decide which client to return; re-raise.
    cluster_info = single.info("cluster")
    if not isinstance(cluster_info, dict):
        # Non-dict response from INFO is a client-layer bug; fail loud.
        raise RuntimeError(
            f"INFO cluster returned non-dict ({type(cluster_info).__name__}); "
            "client/server protocol mismatch?"
        )
    enabled = cluster_info.get("cluster_enabled", 0)
    try:
        is_cluster = bool(int(enabled))
    except (TypeError, ValueError):
        is_cluster = str(enabled).lower() in ("1", "true", "yes")

    if not is_cluster:
        return single

    # Cluster mode: reconnect with ValkeyCluster. Closing the single-node
    # client prevents a leaked FD — valkey-py doesn't release it on GC
    # until the connection pool idle-reaps.
    try:
        single.close()
    except Exception as e:
        _log.debug("closing pre-cluster single client: %s", e)

    return ValkeyCluster.from_url(
        url,
        username=username,
        password=password,
        socket_timeout=timeout_seconds,
        socket_connect_timeout=timeout_seconds,
        decode_responses=False,
    )


def _apply_tls_scheme(url: str, use_tls: bool) -> str:
    """Rewrite the URL scheme for TLS when `--tls` is set.

    No-op if the URL already uses a TLS scheme (`valkeys://` / `rediss://`)
    or if TLS wasn't requested. Users who passed `valkeys://` directly and
    omitted `--tls` still get TLS — the scheme wins, `--tls` is just a
    shortcut for the common case of pasting a plain `valkey://` URL.
    """
    if not use_tls:
        return url
    for plain, tls in (("valkey://", "valkeys://"), ("redis://", "rediss://")):
        if url.startswith(plain):
            return tls + url[len(plain) :]
    # Unix sockets or already-TLS URLs pass through unchanged.
    return url
