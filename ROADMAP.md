# Roadmap

## Status

Current release: **v0.1.5** — see [CHANGELOG.md](./CHANGELOG.md) for the full
history. v0.1.5 landed the major watch-path reliability hardening (backoff
escalation, watch-state reset on disconnect, close/watch race fix,
context-threaded resync). The next milestones below focus on cleaning up the
dependency surface that consumers inherit and on closing the smaller open
items from the post-review pass.

Dates below are illustrative direction, not commitments. The authoritative
tracker for individual items is GitHub Issues on this repo once filed.

## v0.2.0 — Hardening

- **Split embedded-etcd test infrastructure into a nested test module.** The
  spin-up of `embed.Etcd` for integration tests currently lives in the main
  package, which forces `go.etcd.io/etcd/server/v3` and its very large
  transitive tree into every consumer's `go mod why` output. Moving the test
  harness into a sibling module (`./internal/etcdtest` with its own
  `go.mod`) keeps the integration coverage we have while shrinking what
  library users actually pull in.
- **Refresh stale transitive dependencies.** Once the test module split lifts
  the version pins inherited from the embedded etcd server, bump
  `google.golang.org/grpc`, `golang.org/x/net`, `golang.org/x/crypto`, and
  `github.com/golang-jwt/jwt/v4`. These resolve outstanding `govulncheck`
  advisories that are currently visible to consumers via dependency tree
  scanning even though the library itself does not exercise the vulnerable
  code paths.
- **Replace value-comparison conflict detection with explicit `wasSet` flags.**
  Today the `WithClient` conflict detector compares option values against
  their zero defaults, which produces a false positive when a caller passes
  an explicit-but-equal-to-default option together with `WithClient`. Per-
  option `wasSet` flags make the contract explicit and unambiguous.
- **Watch loop reliability follow-ups.** Floor the reconnect backoff so a
  misconfigured option cannot produce a tight retry loop; bound the pending
  events slice so a paused consumer cannot drive unbounded growth; add
  `recover` around the dispatch callback so a panicking consumer does not
  take the loop down; classify `PermissionDenied` and `AuthFailed` as fatal
  so the loop does not spin forever against a misconfigured cluster.
- **Observability wiring.** Thread structured logging (`slog`) through the
  watch path with consistent fields; actually invoke the configured
  `WithRedactor` when emitting key/value debug logs; extend `Stats` with the
  fields that matter operationally (last revision seen, reconnect count,
  current backoff, last error).
- **Harden `loadTLSFromFiles`.** Set an explicit `MinVersion`, accept a
  `ServerName` option, and document the resulting `tls.Config` semantics.

## v0.2.1 — Polish

- Additional `Stats` fields for downstream dashboards.
- Benchmarks for the read path under realistic prefix sizes (1k / 10k / 100k
  keys) and a benchstat-tracked baseline in CI.
- Edge-case test coverage: empty prefix rejection, very large value handling,
  delimiter collision in keys, debounce coalescing under burst load.

## v0.3.0 and beyond

Deferred until there is a concrete consumer ask:

- Arbitrary `WithRange` option (today the Provider always uses a prefix
  range; some consumers may want explicit start/end keys).
- `RejectOldCluster` option for environments that want strict version
  gating.
- Tunable etcd message size knobs (`MaxCallSendMsgSize`,
  `MaxCallRecvMsgSize`) for very large payloads.
- `PermitWithoutStream` and other low-level grpc keepalive tuning.
- End-to-end TLS integration test against an embedded etcd with a generated
  CA, kept inside the test module.
- Nightly CI matrix entry covering the etcd `client/v3` v3.6 line so we
  catch any wire-level regressions before they reach release.

## How to contribute

Bug reports, design questions, and PRs are welcome. See the
[README](./README.md) for usage and quickstart, and file issues on
[GitHub Issues](https://github.com/uded/koanf-etcd/issues) — the items above
will be filed there as individual tickets so each can be discussed and scoped
independently.
