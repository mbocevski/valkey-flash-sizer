# valkey-flash-sizer

Read-only analyzer that samples a Valkey keyspace and projects the RAM savings you'd get from tiering cold entries to NVMe via [`valkey-flash`](https://github.com/mbocevski/valkey-flash).

```
uvx valkey-flash-sizer valkey://my-host:6379
```

The tool never mutates state — it only issues `SCAN`, `MEMORY USAGE`, `OBJECT IDLETIME`, `TYPE`, and `TTL`. Output is a Markdown report (or JSON with `--format json`) with honest confidence intervals on every projected number.

## Status

Pre-release. Not yet on PyPI. Run from the repo:

```
uvx --from git+https://github.com/mbocevski/valkey-flash-sizer flash-sizer valkey://my-host:6379
```

## Usage

```
flash-sizer [OPTIONS] URL

  URL                Valkey URL (valkey://, valkeys://, redis://, rediss://, unix://)

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
```

Cluster mode is auto-detected — point the tool at any primary.

## Example output

The report (Markdown by default) leads with a headline — projected RAM saving with a 95 % confidence interval — and then walks through the sampling fraction, the idle-time distribution, per-type value-size percentiles, the largest sampled keys, and an explicit "Known biases" section. See `tests/golden/report.md` for a complete example rendered from a fixture.

## What it does

1. Samples up to N keys from the target Valkey (default 100 000), stratified across cluster slots so no shard dominates.
2. Probes each sampled key for byte size, idle time, TTL, and type.
3. Aggregates into an idle-time histogram, per-type size percentiles, and a top-N large-key list.
4. Projects the RAM saving you'd get by tiering everything idler than `--cold-threshold` (default 30 min) to `valkey-flash`, with a 95 % Wilson-score confidence interval derived from the sample size.
5. Surfaces the caveats honestly — sampling variance, `OBJECT IDLETIME` approximation under LRU, non-tierable byte subtraction.

## What it does not do

- No writes. No `CONFIG SET`. No `MONITOR`. No telemetry upload.
- No `--bench` path yet (planned follow-up — would spin up a local `valkey-flash` instance to measure actual NVMe cold-read p99 on your hardware).
- No recommendation engine. No auto-install of the module.

## License

BSD 3-Clause. See `LICENSE`.
