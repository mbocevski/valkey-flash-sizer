# valkey-flash sizing report

**Target:** `valkey://example.com:6379`  
**Sampled at:** 2026-04-20T12:00:00Z  
**Valkey version:** 8.0.1  
**maxmemory-policy:** `allkeys-lru`  
**Cluster:** no

## Warnings

- Sample taken during off-peak (02:00 UTC); re-run at peak for a representative cold/hot split.

## Headline

**Projected RAM saving:** 14.5 MB (13.3 MB–15.7 MB, 95 % CI).

About **40.0 %** of your tierable working set has been idle for at least 30m — the prime migration candidates. CI: 36.7 % to 43.4 % (95 %, Wilson score, n=800).

Estimate assumes a hot-cache fraction of 5.0 % on the flash tier (module default). See Known biases below for what this projection does and does not account for.

## Sampling

- Keys scanned: **1,000** of 10,000 total (~10.00 %)
- Keys probed successfully: 990
- Keys skipped (vanished between SCAN and probe): 10
- Tierable (string/hash/list/zset): 800 keys, 3.8 MB
- Non-tierable (set/stream/pubsub/etc.): 190 keys, 976.6 KB
- Scale factor applied to byte totals: ×10.00 (sample size 1,000 / dbsize 10,000)

## Idle-time distribution

| Idle | Keys | Bytes |
|---|---:|---:|
| <1m | 200 | 781.2 KB |
| <10m | 180 | 683.6 KB |
| <1h | 100 | 390.6 KB |
| <1d | 120 | 585.9 KB |
| ≥1d | 200 | 1.1 MB |

## Value sizes (tierable types)

| Type | Count | p50 | p99 | Max |
|---|---:|---:|---:|---:|
| hash | 5 | 2.0 KB | 14.3 KB | 14.6 KB |
| string | 8 | 1000 B | 9.4 KB | 9.8 KB |

## Largest keys in the sample (top 5)

| Key | Type | Bytes | Idle |
|---|---|---:|---:|
| `session:user:12345` | string | 512.0 KB | 2h |
| `cache:page:home` | string | 256.0 KB | 1h |
| `leaderboard:daily` | zset | 128.0 KB | 2m |
| `\xff\xfe\x00binkey` | string | 64.0 KB | 1m |
| `todo:inbox:u42` | list | 32.0 KB | — |

## Known biases

- **SCAN sampling** visits every shard (in cluster mode) but does not guarantee strict stratification. On heavily-skewed keyspaces the sample may over-represent the largest shards.
- **`OBJECT IDLETIME` is approximate.** Under `allkeys-lru` / `volatile-lru` policies the idle timer is updated lazily during eviction, not on every access. Under LFU it is not maintained at all.
- **Size-neutrality assumption.** Cold-bytes and RAM-saving bounds scale the fraction CI by observed tierable bytes. This assumes cold keys have the same average byte size as hot keys; real workloads often skew — cold keys tend to be larger.
- **Workload drift.** A sample at 02:00 looks nothing like 14:00. Run this during peak and off-peak, compare the reports.
- **Non-tierable bytes** (sets, streams, pub-sub buffers, replication backlog) are counted in `total_bytes` but excluded from the migration projection — `valkey-flash` has no tiered equivalent for them today.
- **Confidence level** is 95 % (Wilson score, two-sided), n=800.

