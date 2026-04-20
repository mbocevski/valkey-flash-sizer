# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-04-20

First release.

### Added

- Read-only keyspace sampler: `SCAN` + pipelined `TYPE` / `MEMORY USAGE` / `OBJECT IDLETIME` / `TTL` per key, aggregation into a `SampleStats` dataclass. Handles cluster mode (auto-detected), LFU-policy idle errors, keys that vanish between scan and probe, non-UTF-8 keys, and bounded top-N tracking.
- Projection layer: turns a `SampleStats` into a `Projection` with a Wilson-score confidence interval on the fraction of tierable keys that are cold, scaled byte projections for the whole keyspace, and a RAM-saving range estimate. Declines cleanly when running against a pure-LFU server (no `OBJECT IDLETIME` data) rather than fabricating zeros.
- Markdown and JSON report renderers. Golden-file tests guard the Markdown wording and the JSON shape (`schema_version = 1`) against drift.
- Format helpers (`format_bytes`, `format_duration`, `format_percent`) matching Valkey's own `INFO memory` conventions.
- CLI entry point (`flash-sizer`): connects over `valkey://` / `valkeys://` / `redis://` / `rediss://` / `unix://` URLs, auto-detects cluster mode (reconnecting as `ValkeyCluster` when needed), parses short human duration strings for `--cold-threshold`, surfaces `maxmemory-policy` warnings pulled from the server.
- End-to-end integration test that seeds deterministic cold keys via `RESTORE ... IDLETIME` and drives the full pipeline against a live Valkey.
- PyPI release workflow with OIDC trusted publishing. Tag a `v*.*.*`, workflow builds sdist + wheel, verifies the tag matches `pyproject.toml` version, publishes without any API token on either side.
- Dependabot configuration matching the sibling `valkey-flash` repo: weekly SHA bumps for GitHub Actions, grouped minor/patch bumps for Python deps via the `uv` ecosystem.

[Unreleased]: https://github.com/mbocevski/valkey-flash-sizer/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/mbocevski/valkey-flash-sizer/releases/tag/v0.1.0
