# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
