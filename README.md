# valkey-flash-sizer

Read-only analyzer that samples a Valkey keyspace and projects the RAM savings you'd get from tiering cold entries to NVMe via [`valkey-flash`](https://github.com/mbocevski/valkey-flash).

```
uvx valkey-flash-sizer valkey://my-host:6379
```

The tool never mutates state — it only issues `SCAN`, `MEMORY USAGE`, `OBJECT IDLETIME`, `TYPE`, and `TTL`. Output is a Markdown report (or JSON with `--format json`) with honest confidence intervals on every projected number.

## Status

Pre-release. Not yet on PyPI. Install from source:

```
uvx --from git+https://github.com/mbocevski/valkey-flash-sizer flash-sizer valkey://my-host:6379
```

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
