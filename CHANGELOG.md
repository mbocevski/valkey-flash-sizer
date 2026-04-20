# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Stats primitives: Wilson-score confidence interval, linear-interpolated percentile, idle-time histogram buckets. Stdlib-only.
- Keyspace sampler: `SCAN` + pipelined `TYPE` / `MEMORY USAGE` / `OBJECT IDLETIME` / `TTL` per key, aggregation into a `SampleStats` dataclass. Handles cluster mode, LFU-policy idle errors, vanished keys, non-UTF-8 keys, and bounded top-N tracking.
- Projection: turns a `SampleStats` into a `Projection` with Wilson CI on the cold fraction, scaled byte projections, and a RAM-saving range. Declines cleanly under pure LFU.
- Markdown + JSON report renderers with golden-file tests. JSON shape carries `schema_version` so consumers can pin.
- CLI entry point (`flash-sizer`): connects over `valkey://` / `valkeys://` / `redis://` / `rediss://` / `unix://` URLs, auto-detects cluster mode, surfaces `maxmemory-policy` warnings.
- Integration test that seeds deterministic cold keys via `RESTORE ... IDLETIME` and asserts the full pipeline end-to-end against a live Valkey.
