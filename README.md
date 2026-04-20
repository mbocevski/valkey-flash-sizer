# valkey-flash-sizer

Read-only analyzer that samples a Valkey keyspace and estimates the RAM savings you'd get from tiering cold entries to NVMe via [`valkey-flash`](https://github.com/mbocevski/valkey-flash).

```fish
uvx valkey-flash-sizer valkey://my-host:6379
```

The tool never mutates state — it only issues `SCAN`, `MEMORY USAGE`, `OBJECT IDLETIME`, `TYPE`, and `TTL`. Output is a Markdown report (or JSON with `--format json`) with honest confidence intervals on every projected number.

## Install

```fish
# Ephemeral (recommended — no install)
uvx valkey-flash-sizer valkey://host:6379

# Persistent
pipx install valkey-flash-sizer
flash-sizer valkey://host:6379

# Main branch (bleeding edge)
uvx --from git+https://github.com/mbocevski/valkey-flash-sizer flash-sizer valkey://host:6379
```

## Usage

```
flash-sizer [OPTIONS] URL

  URL                          Valkey URL (valkey://, valkeys://, redis://, rediss://, unix://)

  --sample N                   Keys to sample (default 100000)
  --cold-threshold DURATION    Idle cutoff for "cold" (default 30m; accepts 30s/15m/2h/1d)
  --hot-cache-ratio F          Assumed hot-cache fraction on the flash tier (default 0.05)
  --confidence LEVEL           Wilson CI level (0.80 / 0.90 / 0.95 / 0.99, default 0.95)
  --format markdown|json       Report format (default markdown)
  --output FILE                Write report to file instead of stdout
  --tls                        Use TLS (shortcut for valkeys:// scheme)
  --username U --password P    AUTH credentials
  --timeout SECONDS            Per-command socket timeout (default 10)
  --pipeline-size N            Probes per round-trip (default 200)
  -v, --verbose                Debug logging to stderr
```

Cluster mode is auto-detected — point the tool at any primary and the sizer will walk every shard via `SCAN`.

## What it does

1. Samples up to N keys from the target Valkey (default 100 000), walking every shard in cluster mode.
2. Probes each sampled key for byte size, idle time, TTL, and type.
3. Aggregates into an idle-time histogram, per-type size percentiles, and a top-N large-key list.
4. Projects the RAM saving you'd get by tiering everything idler than `--cold-threshold` (default 30 min) to `valkey-flash`, with a 95 % Wilson-score confidence interval derived from the sample size.
5. Surfaces the caveats honestly — sampling variance, `OBJECT IDLETIME` approximation under LRU, non-tierable byte subtraction.

See [`tests/golden/report.md`](tests/golden/report.md) for a full sample report rendered from a fixture.

## What it does NOT do

- **No writes.** No `SET`, `DEL`, `CONFIG SET`, `MONITOR`. No telemetry upload. No phone-home.
- **No auto-install** of the `valkey-flash` module. We recommend, we don't install — the sizer runs *before* the user has made a decision.
- **No synthetic cold-read benchmark.** A `--bench` path would have to write test keys and demote them to measure real cold-read p99 on the user's NVMe; that contradicts the read-only guarantee this tool leads with. If that matters, track it separately — likely a sibling `flash-bencher` tool rather than a flag on this one.
- **No recommendation engine.** We report what's cold; we don't tell you which specific keys to migrate first.

## Honesty about the numbers

Every projected number carries a 95 % Wilson-score confidence interval, and the report's "Known biases" section names the assumptions baked into each derivation:

- **SCAN sampling** visits every shard in cluster mode but does not guarantee strict stratification. On heavily-skewed keyspaces the sample may over-represent the largest shards.
- **`OBJECT IDLETIME`** is approximate under `allkeys-lru` / `volatile-lru` (updated lazily during eviction) and unavailable under LFU (the projection declines cleanly rather than fabricating zeros).
- **Size-neutrality assumption:** cold-bytes and RAM-saving bounds scale the fraction CI by observed tierable bytes, which assumes cold keys have the same average byte size as hot keys. Real workloads often skew — cold keys tend to be larger.
- **Workload drift:** a sample at 02:00 looks nothing like 14:00. Run it during peak and off-peak, compare the reports.
- **Non-tierable bytes** (sets, streams, pub-sub buffers, replication backlog) are counted in totals but excluded from the migration projection — `valkey-flash` has no tiered equivalent for them today.

## Development

```fish
git clone https://github.com/mbocevski/valkey-flash-sizer
cd valkey-flash-sizer
uv sync
uv run pytest                 # unit tests
uv run ruff check             # lint
uv run ruff format --check    # formatting
uv build                      # sdist + wheel in dist/
```

Integration tests (against a real Valkey) run when `VALKEY_URL` is set:

```fish
valkey-server --port 16390 --daemonize no --save "" &
VALKEY_URL=valkey://127.0.0.1:16390 uv run pytest -m integration
```

## License

BSD 3-Clause. See [`LICENSE`](LICENSE).

## Related

- [`valkey-flash`](https://github.com/mbocevski/valkey-flash) — the Valkey module this tool sizes for.
