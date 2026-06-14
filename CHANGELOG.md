# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.2] - 2026-06-14

### Fixed

- The watch loop no longer fires a doomed `Watch` RPC after a compaction recovery whose `resync()` itself failed. Previously each wakeup re-opened a watch from the still-compacted revision, immediately re-receiving `ErrCompacted`. The loop now tracks a `pendingResync` state and retries the full re-read directly until etcd is reachable again.
- `WatchTyped(ctx, cb)` now returns a wrapped `context.Canceled` immediately when the supplied `ctx` is already cancelled at call time, instead of starting a goroutine that exits silently and leaves the caller waiting forever for a callback that never fires.

### Documented

- README gained a "Watch lifecycle" subsection covering start/stop/restart of the watch loop, including the v0.3.2 behavior change for `WatchTyped` with a pre-cancelled context.
- README "Options reference" was rewritten from an 8-row abridged table to a grouped, ~25-row near-complete reference covering every option whose default matters or where a gotcha would otherwise force a `go doc` lookup.
- README gained a "Troubleshooting" section with real gotchas (path collisions, callbacks never firing, `Close()` hangs, gofmt-drift CI failures, etc.).
- ARCHITECTURE.md explains why `etcdwrite` is a separate subpackage and not methods on `Provider`.

## [0.3.1] - 2026-06-05

### Added

- `WithAuthProvider(fn)` — fetches etcd credentials just before client construction so callers using STS-style ephemeral credentials or secret-manager rotation don't have to keep long-lived strings on the heap. Mutually exclusive with `WithAuth`.
- `SECURITY.md` — documents the threat model (what we defend against vs. don't), the SRV-discovery trust assumption, the Watch event-value trust boundary, and how to report vulnerabilities.

### Documented

- README "Security notes" section gained explicit pointers to the threat model, the Watch event-value trust caveat, and the SRV+DNS dependency.

## [0.3.0] - 2026-06-05

Second-wave Medium-severity work from the principal review. Two coordinated PRs land together: an architectural cleanup (no API change) plus an observability + error-surface extension (breaking on three callback signatures, additive on `Stats`).

### Changed (breaking)

- `WithOnReconnect` callback signature is now `func(attempt int, lastErr error, lastRevision int64)`. The added `lastRevision` lets callers correlate reconnects with potential data-gap windows. Migration: add the third parameter to existing closures; the value is the revision the next watch will resume from.
- `WithOnWatchError` callback signature is now `func(err error, class WatchErrorClass)`. Removes the need to string-match the error to decide whether to retry or page. Classes: `WatchErrorTransient`, `WatchErrorCompaction`, `WatchErrorAuth`, `WatchErrorFatal`.
- `Watch` and `WatchTyped` now return the sentinel `ErrWatchActive` when a watch is already running (was a raw `fmt.Errorf`). Detect with `errors.Is`.
- `ErrKeyNotFound` is new and is now returned by `ReadBytes` and by single-key `Read` (with `WithStrict`). `ErrEmptyPrefix` is retained for prefix-mode reads.

### Added

- `Stats` extended with `TotalWatchErrors`, `TotalEventsDelivered`, `TotalReads`, `LastWatchErrorAt`, `TotalDebounceFlushes`, `LastFlushCoalescedCount` for RED/USE-style dashboards.
- `WatchErrorClass` enum (`WatchErrorTransient`, `WatchErrorCompaction`, `WatchErrorAuth`, `WatchErrorFatal`) consumed by the new `OnWatchError` signature.

### Refactored

- **BYO-conflict detection now uses explicit per-option `wasSet` flags** instead of value-equality with defaults. The old check would silently miss `WithClient + WithDialTimeout(5s)` (5s being the default); the new check fires regardless of value, and stops being brittle to future default changes.
- **Switched unflattening to `github.com/knadh/koanf/maps`.** Deleted ~80 lines of home-grown `splitPath` / `unflattenMap` / `setNested` and replaced with a thin wrapper that adds the two behaviors the upstream helper deliberately omits: leading/trailing empty-segment normalization and `ErrPathCollision` detection for leaf-vs-subtree conflicts.

## [0.2.3] - 2026-06-05

First slice of the Medium-severity bug-fixes from the principal review. Two real correctness fixes, no API changes.

### Fixed

- **`splitPath` now strips trailing empty segments**, not just leading ones. A path like `app.db.` used to produce `["app", "db", ""]`, which after unflatten left an empty-string key at the leaf and could collide with sibling values via the recently-added `ErrPathCollision` check. The leading-strip already handled `.app.db`; the trailing-strip is the symmetric fix.
- **`AutoSyncInterval` defaults to 30 seconds** when no `WithAutoSync` is supplied. A flapping etcd member would otherwise leave the built-in client pinned to a dead endpoint indefinitely. The default is applied inside `buildClient` (not in the settings struct) so the value-equality check used by BYO-client conflict detection keeps working. Pass `WithAutoSync(0)` to opt out.

### Internal

- New helper `defaultIfZero(v, d time.Duration)` documents the "default at build-time, not in settings" pattern. Used by the new autoSync default.

## [0.2.2] - 2026-06-05

### Fixed

- `TestClose_HonorsCloseTimeout` upper bound raised from 2s to 8s — the previous bound was too tight for a loaded GitHub Actions runner with a slow embedded-etcd teardown. The lower bound (200ms) is unchanged and remains the load-bearing assertion (proves `Close` actually waited rather than returning instantly).

### Retracted

- `v0.2.1` is retracted in `go.mod`. The integration test flaked on CI; library behavior was correct. Use `v0.2.2`.

## [0.2.1] - 2026-06-05

### Fixed

- gofmt drift in `settings.go` and `watch.go` (struct-field alignment after the recent edits) — caught by the CI lint job. No behavioral change.

### Retracted

- `v0.2.0` is retracted in `go.mod`. It published successfully but its CI run failed on the gofmt check, so the release workflow never produced a GitHub Release or SBOM. Use `v0.2.1` or later.

## [0.2.0] - 2026-06-05

Post-principal-review hardening pass. Closes the seventeen High-severity findings from the multi-agent audit. Adds an architectural split that's load-bearing for every future dep bump.

### Removed (breaking)

- **`WithLogger` and `WithRedactor`.** A configuration-loader library should not impose a logger choice on its consumers; the library now emits only structured callbacks. Migration: route the existing `WithOnReconnect`, `WithOnResync`, `WithOnWatchError`, and `OnEmpty` callbacks into whatever logger your application already uses. The internal `log/slog` wiring in the watch loop is gone — every emit site already had a callback, so removing the duplicate cleans the public surface without losing observability.

### Changed (breaking)

- **Go floor bumped from `1.23` to `1.25`.** Forced by the mid-2026 ecosystem migration: latest `etcd/client/v3`, `grpc`, and the entire `golang.org/x/*` graph now declare `go 1.25.0`. The alternative — pinning every transitive down — costs ~10 etcd patches, ~7 grpc minors, and surfaces non-callable `govulncheck` advisories on stale `x/net`/`x/sys`. See README "Why `go 1.25`?" for the full reasoning. Consumers that need `go 1.23` should pin `koanf-etcd@v0.1.x` and stay on that line.
- **`DeletePrefix` and `Delete` in the `write/` subpackage now reject empty and root-only (`/`) prefix arguments** with `ErrUnsafePrefix`. The old behavior silently nuked the entire etcd cluster. Opt back in with `etcdwrite.AllowEmptyPrefix()` for the rare legitimate multi-tenant wipe.
- **`Read()` now returns `ErrPathCollision`** when two etcd keys map to paths where one is the prefix of the other (e.g. both `/svc/db` and `/svc/db/host` exist under the same `WithPrefix`). Previously a silent overwrite produced incomplete config with no log line.

### Fixed

- **Reconnect backoff honors the configured `min` floor.** `backoff()` used to return `[0, exp]` and could draw `0`, defeating the `WithReconnectBackoff(min, max)` contract. Now returns `[min, exp]` and uses `math/rand/v2` (no global mutex on the jitter draw). Added direct unit tests: `TestBackoff_HonorsMinFloor`, `TestBackoff_HandlesOverflow`, `TestBackoff_MinEqualsMax`.
- **Pending-event slice no longer aliases its backing array across `flush`.** Callers retaining a `[]Event` batch could observe silent mutation when the next `append` happened. Each flush now copies out; the slice trims its backing array if it grew far past the high-water mark.
- **Pending-event growth is bounded.** New `WithMaxPendingEvents(n int)` option (default `10000`) forces an early flush when the buffer fills, so a stuck consumer + sustained event flood can't grow `pending` without limit.
- **Watch goroutine survives consumer-callback panics.** A `defer recover()` in the watch loop and in `deliverBatch` catches panics from `Watch` / `WatchTyped` callbacks, fires `OnWatchError` with the recovered value, and lets the loop continue. Without this a single buggy callback crashed the host process. New test: `TestWatch_PanicInCallbackDoesNotCrash`.
- **Fatal RPC errors are classified and stop the retry loop.** `rpctypes.ErrPermissionDenied`, `ErrUserNotFound`, `ErrAuthFailed`, `ErrInvalidAuthToken`, and gRPC `codes.PermissionDenied` / `codes.Unauthenticated` now fire `OnWatchError` with a wrapped error and return. Previously these spun forever, generating audit-log noise.
- **Prefix pagination guards against an infinite loop** when etcd returns `More=true` with an empty `Kvs` page (impossible against current etcd but a defense against misbehaving proxies and future versions).
- **`Close()` blocks until the watch goroutine exits.** Adds `watchDone chan struct{}` that the loop closes on exit, plus `WithCloseTimeout` (default `5s`) so a wedged consumer callback can't hang application shutdown. Resolves the `goleak` flake potential the previous version had. New tests: `TestClose_WaitsForWatchGoroutine`, `TestClose_HonorsCloseTimeout`.
- **`loadTLSFromFiles` pins `MinVersion = TLS 1.2`**, never negotiating TLS 1.0/1.1. New `WithTLSServerName` option for callers connecting to etcd by IP address.
- **Watch state is cleared on loop exit** so a fresh `Watch` / `WatchTyped` can be attached after a `WithWatchContext` cancellation without going through `Close()`. Regression test: `TestWatch_RestartAfterCtxCancel`.
- **`Close` race with `Watch` closed.** Both `Watch` and `WatchTyped` now check `p.closed` *inside* the `watchMu` critical section.

### Refactored

- **Embedded-etcd integration tests moved to a nested module at `tests/integration/`.** The main module no longer carries `go.etcd.io/etcd/server/v3`, `goleak`, raft, bbolt, prometheus, opentelemetry, zap, grpc-gateway, lumberjack, or any of their transitive set. Consumer `go list -m all` is server-free. CI runs both modules; a new dep-hygiene step fails the build if `etcd/server` ever re-enters the main graph.
- **Bumped to `etcd client/api v3.5.31` and `grpc v1.81.1`** post-split — `govulncheck` now reports **zero advisories** in the main module. The previous `v3.5.17 + grpc v1.59.0` graph carried `golang-jwt v4.4.2`, stale `x/net`, stale `x/crypto`, and several other CVE-tagged transitives.

### Added

- **`WithMaxPendingEvents(n)`** — bound the debounced-event buffer; default 10 000.
- **`WithCloseTimeout(d)`** — bound `Close()`'s wait for the watch goroutine; default 5s.
- **`WithTLSServerName(name)`** — set `tls.Config.ServerName` for IP-endpoint deployments.
- **`AllowEmptyPrefix()`** option in `etcdwrite` — opt back into the dangerous-but-occasionally-legitimate cluster-wipe semantics.
- **`ErrUnsafePrefix`** sentinel in `write/` and **`ErrPathCollision`** sentinel in the main package.
- **`ARCHITECTURE.md`** and **`ROADMAP.md`** with design principles, watch state machine, key trade-offs, and the v0.2.0 / v0.2.1 / v0.3.0 work breakdown.
- **Benchmarks** (`BenchmarkSplitPath`, `BenchmarkUnflatten_100Keys`, `BenchmarkReadPrefix_1000Keys`, `BenchmarkBackoff`) so future perf work can be measured.
- **Release workflow** (`.github/workflows/release.yml`) — fires on tag push, runs the verify gate, generates an SPDX SBOM, attaches it to the auto-generated release.

### Documented

- Per-field godoc on every `Stats` exported field; the `EventType` constants now render under a const-block group header (`go doc EventPut` shows a body).
- README has a new "Why `go 1.25`?" subsection with the trade-off table.

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
