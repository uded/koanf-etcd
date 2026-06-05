# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Removed (breaking)

- `WithLogger` and `WithRedactor` are removed. A configuration-loader library should not impose a logger choice on its consumers; the library now emits only structured callbacks. Migration: route the existing `WithOnReconnect`, `WithOnResync`, `WithOnWatchError`, and `OnEmpty` callbacks into whatever logger your application already uses.
- The internal `slog` wiring in the watch loop is gone — every emit site already had a callback, so removing the duplicate cleans the public surface without losing observability.

## [0.1.5] - 2026-06-05

Post-review hardening. Addresses 6 of 9 findings from an external code review; the remaining 3 (test-module split, dep bump, BYO `wasSet` flags) are queued for `v0.2.0`.

### Fixed

- **Reconnect backoff no longer pins at min.** `attempt = 0` was being reset at the top of every loop iteration, so the exponential climb to `reconnectMax` never happened — a downed cluster got hammered at ~1 s forever. `consumeWatch` now returns a `madeProgress` flag and the loop resets `attempt` only on real progress (≥1 non-error response or a successful resync).
- **Watch can be restarted after `WithWatchContext` cancellation.** Cancelling the watch ctx used to leave `watchCancel` non-nil, so every subsequent `Watch` / `WatchTyped` returned `"watch already active"` until `Close()`. The loop now clears its slot on exit via `defer p.clearWatchState()`. Regression test: `TestWatch_RestartAfterCtxCancel`.
- **Close/Watch race closed.** `Watch` and `WatchTyped` now check `p.closed` *inside* the `watchMu` critical section. Previously a concurrent `Close` could flip `closed` after the check but before the goroutine started, leaking a watcher against an about-to-close client.
- **`resync` honors the watch context.** It used to call `Read()` / `ReadBytes()` (each spinning their own `context.Background()` + `WithTimeout`), so a Close during a slow re-read blocked for `readTimeout`. Added internal `readCtx` / `readBytesCtx` helpers; the watch ctx threads through.
- **`onReconnect` no longer fires twice with inconsistent arguments.** The refactor in `63ced8b` collapsed the two call sites into `recordReconnect`.

### Refactored

- `validateSettings`, `watchLoop`, `consumeWatch`, and `TestOptions_Watch` extracted into smaller helpers to drop gocyclo complexity from `>15` to single digits across the source files. Go Report Card should land above 90 % once it re-scans.

### Documented

- `Event.Value` is now explicitly documented as **raw bytes** — `WatchTyped` does not apply `WithValueTransform`. Symmetric handling with `Read()` is the caller's choice (parse JSON/proto directly, or run the transform yourself).

### Deferred to v0.2.0

- `server/v3` pollutes the published dep graph (`testutil_test.go` imports `embed`) — moving embedded-etcd tests to a nested test module.
- Stale transitive deps (`grpc`, `x/net`, `x/crypto`, `jwt/v4`, `etcd`) — easier to bump cleanly after the test-module split.
- BYO-client conflict detection compares against default values (`s.dialTimeout != 5*time.Second`); switching to explicit `wasSet` flags removes the brittleness.

## [0.1.4] - 2026-06-05

### Added

- README "Related projects" section linking [koanf-structdefaults](https://github.com/uded/koanf-structdefaults) and [koanf-validate](https://github.com/uded/koanf-validate) as the documented floor + post-load gate in the load order.
- Go Report Card and Release badges in the README header.

### Fixed

- CI vuln scan pins `actions/setup-go` to `'stable'` with `check-latest: true` and sets `GOTOOLCHAIN=auto`, so `govulncheck` runs against the latest patched stdlib (where `GO-2026-5037` / `GO-2026-5039` / `GO-2026-4971` are fixed). The previous `v0.1.3` runner installed `go1.25.10` (not `1.25.11`), leaving stdlib advisories flagged.

## [0.1.3] - 2026-06-05

### Changed

- Bumped CI action majors past Node.js 20 (which GitHub has deprecated): `actions/checkout@v4` → `v6`, `actions/setup-go@v5` → `v6`, `actions/upload-artifact@v4` → `v7`. No workflow logic changes.

## [0.1.2] - 2026-06-05

### Fixed

- Added `toolchain go1.25.11` directive to `go.mod`. The previous `v0.1.1` CI still failed because `govulncheck` scans the standard library at the `go` directive's version (`1.23.0`), not the runner's Go version. The `toolchain` directive hints `go` and `govulncheck` to use Go 1.25.11's patched stdlib without raising the consumer floor (still `go 1.23`).

### Notes

- Consumers on Go 1.23 with toolchain auto-download disabled (`GOTOOLCHAIN=local`) will still build against their local stdlib. Default behavior auto-downloads 1.25.11 on first build.

## [0.1.1] - 2026-06-05

### Fixed

- Removed unused `loadTLSConfig` test helper that `staticcheck` (U1000) flagged in the CI lint job.

### Changed

- CI Go matrix bumped from `1.23 / 1.24` to `1.24 / 1.25`; `govulncheck` and the lint/build jobs now run on Go 1.25 so the stdlib advisories `GO-2026-5037` (`crypto/x509`), `GO-2026-5039` (`net/textproto`), and `GO-2026-4971` (`net`) are not surfaced. The `go.mod` floor remains `1.23`, so downstream consumers on Go 1.23+ are unaffected.

### Notes

- `v0.1.0` shipped with a broken CI run (the two issues above). The library code itself was unchanged; `v0.1.1` is a CI-only fix.

## [0.1.0] - 2026-06-04

### Added

- Initial release of `koanf-etcd`, a production-grade koanf v2 Provider for etcd v3.
- `Provider` with tree mode (`WithPrefix`), single-key mode (`WithKey`), and blob mode (`WithBlob`).
- Nested output by default (unflattens on configurable delimiter); prefix is trimmed; string values are `TrimSpace`'d.
- Bring-your-own `*clientv3.Client` via `WithClient`; not closed by the Provider.
- TLS via `WithTLS(*tls.Config)` or `WithTLSFiles(cert,key,ca)`.
- Auth via `WithAuth(user, pass)`.
- SRV/DNS endpoint discovery via `WithEndpointsFromSRV`.
- Serializable reads (`WithSerializable`), read-at-revision (`WithReadRevision`).
- Pagination for large prefixes (`WithLimit`).
- Strict-empty (`WithStrict`) and `OnEmpty` callback.
- `Watch(cb)` (koanf-compat, nil event) and `WatchTyped(ctx, cb)` ([]Event payload).
- Resume-from-revision (no read/watch gap), bounded exponential backoff reconnect, compaction-triggered resync.
- Debounce (`WithDebounce`), `WithProgressNotify`, `WithEventFilter`, `WithCreatedNotify`.
- Observability callbacks: `WithOnReconnect`, `WithOnResync`, `WithOnWatchError`.
- `Provider.Revision()` and `Provider.Stats()` accessors.
- Optional `etcdwrite` subpackage: `Put`, `Delete`, `DeletePrefix`, `PutAll` (atomic via etcd transaction).
- GitHub Actions CI: lint (gofmt, vet, staticcheck), test (Go 1.23/1.24 matrix, `-race`), `govulncheck`, build.
- Hermetic tests against embedded etcd via `go.etcd.io/etcd/server/v3/embed`.

### Pinned

- `go.etcd.io/etcd/client/v3 v3.5.17`
- `go.etcd.io/etcd/api/v3 v3.5.17`
- Go 1.23+

### Known issues

- `govulncheck` may flag advisories from etcd v3.5.17's transitive `google.golang.org/grpc` dep (e.g. GHSA-xr7q-jx4m-x55m). The Provider is call-graph-aware so symbols we don't reach are not exploitable, but the CI vuln job may surface findings until upstream etcd ships a patch release with an updated grpc.

### Deferred to future releases (filed as issues)

- Arbitrary `WithRange(start, end)`.
- `RejectOldCluster`.
- `MaxCallSendMsgSize` / `MaxCallRecvMsgSize`.
- `PermitWithoutStream`.
- End-to-end TLS integration test against embedded etcd (currently covered by Option-level tests + `loadTLSFromFiles` unit test).
- Nightly CI matrix entry for etcd 3.6 client.
