# koanf-etcd

A production-grade [koanf](https://github.com/knadh/koanf) v2 Provider for [etcd](https://etcd.io) v3.

[![CI](https://github.com/uded/koanf-etcd/actions/workflows/ci.yml/badge.svg)](https://github.com/uded/koanf-etcd/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/uded/koanf-etcd.svg)](https://pkg.go.dev/github.com/uded/koanf-etcd)
[![Go Report Card](https://goreportcard.com/badge/github.com/uded/koanf-etcd)](https://goreportcard.com/report/github.com/uded/koanf-etcd)
[![Release](https://img.shields.io/github/v/release/uded/koanf-etcd?sort=semver)](https://github.com/uded/koanf-etcd/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Related projects

This provider composes with two sibling packages in the same family:

- **[koanf-structdefaults](https://github.com/uded/koanf-structdefaults)** — populate a koanf instance from struct-tag defaults. The natural *floor* layer below this provider in the load order.
- **[koanf-validate](https://github.com/uded/koanf-validate)** — validate the assembled koanf config against struct-tag rules. Pair with this provider's watch loop to gate bad etcd writes (see [Watch + reload recipe](#watch--reload-recipe) below).

Recommended load order: `structdefaults` → file → `koanf-etcd` → env, with `koanf-validate` as the post-load gate.

## Why this exists

The bundled `github.com/knadh/koanf/providers/etcd` has caused real production incidents because:

1. **No auth, no TLS.** It cannot connect to authenticated or mTLS clusters.
2. **Returns flat keys including the prefix**, despite a doc comment claiming "nested." This silently breaks merges with nested layers (defaults, files) — overrides drop nondeterministically.
3. **No value normalization.** A trailing newline from `etcdctl` makes `http://x` and `http://x\n` distinct.
4. **No empty-result diagnostics.** A wrong prefix looks like a healthy boot on stale defaults.
5. **Weak Watch.** Uses `context.Background()`, never reconnects, no compaction recovery, no debounce.

This package fixes every one of those, by default.

### Comparison

| | Bundled `providers/etcd` | `koanf-etcd` |
| --- | --- | --- |
| TLS / auth | ❌ | ✅ |
| Nested output | ❌ (flat with prefix) | ✅ (unflattened, prefix-trimmed) |
| Value TrimSpace | ❌ | ✅ (replaceable) |
| Empty-prefix diagnostics | ❌ | ✅ (strict or callback) |
| Watch reconnect | ❌ | ✅ (bounded exponential backoff) |
| Compaction recovery | ❌ | ✅ (resync event) |
| Resume-from-revision | ❌ | ✅ (no read/watch gap) |
| Debounce | ❌ | ✅ (configurable window) |
| BYO `*clientv3.Client` | ❌ | ✅ (headline feature) |
| Blob mode (one-key documents) | ❌ | ✅ |
| Pagination | ❌ | ✅ |
| Atomic multi-key writes | ❌ | ✅ (separate `write` subpackage) |

## Quickstart — tree mode

```go
import (
    "github.com/knadh/koanf/v2"
    ketcd "github.com/uded/koanf-etcd"
)

p, err := ketcd.New(
    ketcd.WithEndpoints("localhost:2379"),
    ketcd.WithPrefix("/svc/"),
)
if err != nil { panic(err) }
defer p.Close()

k := koanf.New(".")
if err := k.Load(p, nil); err != nil { panic(err) }

fmt.Println(k.String("db.host"))
```

## Quickstart — blob mode

```go
import (
    "github.com/knadh/koanf/parsers/yaml"
    "github.com/knadh/koanf/v2"
    ketcd "github.com/uded/koanf-etcd"
)

p, _ := ketcd.New(
    ketcd.WithEndpoints("localhost:2379"),
    ketcd.WithKey("/cfg.yaml"),
    ketcd.WithBlob(),
    ketcd.WithUnflatten(false),
)
defer p.Close()

k := koanf.New(".")
k.Load(p, yaml.Parser())
```

## BYO `*clientv3.Client`

The headline feature. Share one client across this provider and your own watchers; control its lifecycle yourself.

```go
cli, _ := clientv3.New(clientv3.Config{
    Endpoints: []string{"etcd:2379"},
    TLS:       myTLS,
    Username:  "alice",
    Password:  os.Getenv("ETCD_PASS"),
})
defer cli.Close()  // koanf-etcd will NOT close this

p, _ := ketcd.New(
    ketcd.WithClient(cli),
    ketcd.WithPrefix("/svc/"),
)
```

`Close()` on the Provider closes only what the Provider built. A BYO client stays alive.

## Watch + reload recipe

The Provider tells you something changed. You decide how to reload. Below is the production-grade pattern — runs a validator gate and atomically swaps an immutable `*Config`.

```go
type Config struct { /* ... */ }

var current atomic.Pointer[Config]

p, _ := ketcd.New(
    ketcd.WithClient(cli),
    ketcd.WithPrefix("/svc/"),
    ketcd.WithDebounce(500 * time.Millisecond),
    ketcd.WithOnResync(func(reason string, rev int64) {
        slog.Info("etcd resynced", "reason", reason, "rev", rev)
    }),
)
defer p.Close()

reload := func() error {
    k := koanf.New(".")
    if err := k.Load(p, nil); err != nil { return err }
    var c Config
    if err := k.Unmarshal("", &c); err != nil { return err }
    if err := validate(&c); err != nil { return err }  // koanf-validate
    current.Store(&c)
    return nil
}
if err := reload(); err != nil { /* fail boot */ }

_ = p.Watch(func(_ any, err error) {
    if err != nil {
        slog.Error("watch", "err", err)
        return
    }
    if err := reload(); err != nil {
        slog.Warn("reload rejected; keeping last-good", "err", err)
    }
})
```

A bad write to etcd is rejected by `validate`; last-good keeps serving.

For per-key detail (e.g. to log which keys changed), use `WatchTyped`:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

_ = p.WatchTyped(ctx, func(evs []ketcd.Event, err error) {
    for _, e := range evs {
        slog.Info("change", "type", e.Type, "key", e.Key, "rev", e.Revision)
    }
})
```

### Watch lifecycle

`Watch(cb)` and `WatchTyped(ctx, cb)` are **mutually exclusive** — only one watch is active per Provider at a time. Calling either while a watch is already running returns the sentinel `ErrWatchActive`; detect with `errors.Is(err, ketcd.ErrWatchActive)` rather than string-matching.

A running watch stops on exactly three conditions:

- **`Close()`** — synchronous, waits for the loop to exit up to `WithCloseTimeout` (default `5s`), then falls back to closing the client regardless. Bound it tighter or looser to taste.
- **Parent context cancel** — `WithWatchContext` for `Watch`, or the explicit `ctx` argument for `WatchTyped`. This is the common runtime-switchover path.
- **Fatal RPC error** — auth or permission-denied is surfaced via `WithOnWatchError` with `class == WatchErrorAuth`, the loop returns, and the watch is not retried. Other classes (`Transient`, `Compaction`) are retried internally.

When a parent ctx cancels the loop, the Provider's internal watch state clears as the goroutine unwinds. You can then start a fresh watch — different callback, different filter — without going through `Close()`. This is the right escape hatch for "swap the callback at runtime" patterns:

```go
watchCtx, cancelWatch := context.WithCancel(context.Background())
_ = p.WatchTyped(watchCtx, oldCallback)
// ... later ...
cancelWatch()
// state clears after the loop exits (within a few ms — Close honors the same timeout via WithCloseTimeout)
_ = p.WatchTyped(context.Background(), newCallback)
```

One subtle behavior to know about: pre-cancelling the `ctx` you pass to `WatchTyped` is now rejected with a wrapped `context.Canceled` (detect with `errors.Is`). This is a v0.3.2 change; previously the loop would silently start, immediately exit, and the user's callback would never fire — a footgun in tests that reuse a cancelled context.

## Interop with `koanf-structdefaults` and friends

Load order: struct-defaults (floor) → file → etcd (live) → env (pins/secrets).

```go
k := koanf.New(".")
k.Load(structdefaults.Provider(MyConfig{}, "."), nil)        // floor
k.Load(file.Provider("config.yaml"), yaml.Parser())          // file
k.Load(etcdProvider, nil)                                    // live
k.Load(env.Provider("MYAPP_", ".", nil), nil)                // pins
```

Each later layer overrides earlier ones for keys it provides.

## Security notes

- **Threat model**: see [SECURITY.md](SECURITY.md) for what the library does and does not defend against, plus how to report vulnerabilities.
- **TLS**: pass `*tls.Config` via `WithTLS` or load cert/key/CA files via `WithTLSFiles`. The two are mutually exclusive.
- **Auth**: `WithAuth("user", "pass")`. Never hardcode — pull from env or a secret manager. For STS-style ephemeral credentials or secret-manager rotation, prefer `WithAuthProvider(fn)`; the function runs once just before client construction, so caller-owned credential buffers can be zeroed out immediately afterwards.
- **Watch event values**: `Event.Value` from `WatchTyped` is raw, unvalidated bytes from etcd — any process with write access can put any bytes there. Validate before passing to shells, SQL, HTML-rendered logs, or file paths. `Read()` applies the value transform (TrimSpace by default); `WatchTyped` deliberately does not, so consumers parsing structured payloads (JSON/proto) don't pay the round-trip cost.
- **SRV discovery**: `WithEndpointsFromSRV` trusts DNS at face value. Pair with TLS + `WithTLSServerName` if your DNS path isn't integrity-protected. See SECURITY.md for the full discussion.
- **BYO client lifecycle**: a BYO client is never closed by the Provider.

## Observability

The Provider does not log. It emits structured callbacks (`WithOnReconnect`, `WithOnResync`, `WithOnWatchError`, `OnEmpty`) and exposes a `Provider.Stats()` snapshot with cumulative counters (reads, puts, deletes, resyncs, reconnects, watch errors, events delivered, debounce flushes) plus the last-seen timestamps. Wire the snapshot into your RED/USE dashboard of choice; `WithOnWatchError` carries a `WatchErrorClass` so you can branch on transient vs. compaction vs. auth without parsing the error string.

## Atomic multi-key writes

etcd has no multi-key atomicity in plain `Put`. The optional `etcdwrite` subpackage provides a transactional `PutAll`:

```go
import etcdwrite "github.com/uded/koanf-etcd/write"

etcdwrite.PutAll(ctx, cli, map[string]string{
    "/svc/db.host": "newhost",
    "/svc/db.port": "5433",
    "/svc/db.user": "newuser",
})
```

All three land at the same revision or none do. A watcher with `WithDebounce` coalesces the resulting events into a single reload.

## Troubleshooting

Real gotchas, in rough order of frequency:

- **`Read()` returns `ErrPathCollision`.** Etcd holds both `/svc/db = ...` and `/svc/db/host = ...` under the same prefix; the nested map can't carry a leaf and a sub-tree at the same path. Either rename one of the keys in etcd, or pass `WithUnflatten(false)` to get a flat map and skip nesting entirely.
- **Watch callback never fires.** Three likely causes: (a) you passed a cancelled `ctx` to `WatchTyped` — since v0.3.2 this is rejected with a wrapped `context.Canceled` instead of starting a stillborn loop; (b) you called `p.Watch(cb)` without first calling `p.Read()` while `WithResumeFromRevision(true)` is on (default), so the loop is waiting at revision 0 and no event past that boundary delivers; (c) the parent context cancelled silently — wire `WithOnWatchError` to surface that.
- **`Close()` hangs.** A watch callback is blocking. Default `WithCloseTimeout` is `5s` — past that, `Close()` proceeds to tear down the client regardless. If you need a different bound (a CLI tool wants `1s`, a long-running daemon wants `30s`), supply `WithCloseTimeout(d)` at construction.
- **Reconnect backoff feels wrong.** It's exponential-with-full-jitter, bounded `[min, max]`. Defaults are `min=1s, max=2min`. Override with `WithReconnectBackoff(min, max)`. The `OnReconnect` callback's third argument (`lastRevision`) tells you where the next watch will resume — useful to correlate reconnects with potential data-gap windows.
- **CI green locally, red on GitHub Actions.** Most often a gofmt drift after struct-field edits (alignment shifts as field names get longer or shorter). Run `gofmt -w .` before pushing, or read the lint job's diff in the failure log.
- **`govulncheck` flags transitive vulnerabilities.** The main module is kept lean; the test harness lives in `tests/integration/` as a separate module and deliberately holds `go.etcd.io/etcd/server/v3` and its transitive tree. Run vulnerability scans against the main module only — the integration module isn't consumer-facing.
- **Want to log everything the library does.** The library doesn't log — by design. Wire the four observability callbacks (`WithOnReconnect`, `WithOnResync`, `WithOnWatchError`, `OnEmpty`) into your logger of choice, and read `Provider.Stats()` periodically for dashboards.

## Options reference

Full surface at [pkg.go.dev](https://pkg.go.dev/github.com/uded/koanf-etcd). The table below covers every option whose default matters or where there's a non-obvious gotcha — grouped by area for skimming. Trivially named options whose behavior is obvious from the signature (e.g. `WithEndpoints`) are omitted; see the godoc for those.

### Connection

| Option | Default | Notes |
| --- | --- | --- |
| `WithClient(c)` | — | BYO `*clientv3.Client`. Mutually exclusive with all other connection options. `Close()` will **not** close it. |
| `WithEndpointsFromSRV(svc, proto, domain)` | — | DNS SRV discovery at `New()` time. Pair with `WithTLS` + `WithTLSServerName` unless your DNS path is integrity-protected. |
| `WithDialTimeout(d)` | `5s` | Built-in client only. |
| `WithKeepAlive(t, timeout)` | off | gRPC keepalive for the built-in client. |
| `WithAutoSync(d)` | `30s` | `0` opts out. Stops a flapping member from pinning the built-in client to a dead endpoint. |
| `WithAuth(user, pass)` | — | Static credentials. Mutually exclusive with `WithAuthProvider`. |
| `WithAuthProvider(fn)` | — | Fetches credentials just before client construction — STS / secret-manager rotation. Caller can zero buffers immediately after. Mutually exclusive with `WithAuth`. |
| `WithTLS(cfg)` | — | Mutually exclusive with `WithTLSFiles`. |
| `WithTLSFiles(cert, key, ca)` | — | Convenience loader. |
| `WithTLSServerName(name)` | — | Required when connecting to etcd by IP — the cert's SAN must otherwise include the literal IP. |
| `WithClientContext(ctx)` | `context.Background()` | Lifecycle context for the built-in client. Ignored under `WithClient`. |

### Mode

| Option | Default | Notes |
| --- | --- | --- |
| `WithKey(k)` | — | Single-key mode. Mutually exclusive with `WithPrefix`. |
| `WithPrefix(p)` | — | Tree mode. Mutually exclusive with `WithKey`. |
| `WithBlob()` | off | Blob mode: `Read()` returns `ErrUseParser`, `ReadBytes()` returns the raw value. Requires `WithKey`. Implies `WithUnflatten(false)`. |

### Read shaping

| Option | Default | Notes |
| --- | --- | --- |
| `WithDelim(s)` | `"."` | Path delimiter used for unflattening. |
| `WithTrimPrefix(on)` | `true` (prefix mode) | Strips the configured prefix from each key before mapping. |
| `WithUnflatten(on)` | `true` | Convert flat dotted keys to nested maps. Disable for raw flat layout — also the workaround for `ErrPathCollision`. |
| `WithKeyTransform(fn)` | `/`→delim | Runs after prefix trim. Override only if you need non-trivial key shaping. |
| `WithValueTransform(fn)` | TrimSpace→string | Replace to e.g. parse JSON inline. Note `WatchTyped` skips this — `Event.Value` is raw bytes. |
| `WithLimit(n)` | `0` (no pagination) | Page size for prefix reads. Set `>0` for prefixes over ~1k keys. |
| `WithSerializable(on)` | `false` | Faster, lighter on the cluster, may return slightly stale data. Fine for boot-time defaults. |
| `WithReadRevision(rev)` | `0` (current) | Reproducible snapshot reads. |
| `WithReadTimeout(d)` | `5s` | Bounds each `Get`. |

### Empty handling

| Option | Default | Notes |
| --- | --- | --- |
| `WithStrict(on)` | `false` | A zero-key prefix read returns `ErrEmptyPrefix` instead of silently succeeding. Recommended for production boots. |
| `OnEmpty(fn)` | — | Callback fired (non-strict mode) when a prefix read yields zero keys. Wire to a metric or log line. |

### Watch

| Option | Default | Notes |
| --- | --- | --- |
| `WithWatchContext(ctx)` | provider's internal ctx | Cancellable parent for the watch goroutine. See "Watch lifecycle" above. |
| `WithDebounce(window)` | `0` (off) | Coalesces a burst into one callback. Recommended `100ms`-`1s` for scripted multi-put rollouts. |
| `WithMaxPendingEvents(n)` | `10000` | Cap on buffered debounced events; an early flush fires at the cap. `0` disables the cap. |
| `WithReconnectBackoff(min, max)` | `1s..2min` | Exponential with full jitter. `attempt` resets only on real progress, so flapping clusters can't reset the cap. |
| `WithResumeFromRevision(on)` | `true` | Starts the watch at `initialReadRevision + 1` — closes the read/watch gap. |
| `WithProgressNotify(on)` | `false` | Periodic empty responses from etcd to confirm liveness. |
| `WithEventFilter(put, del)` | both | Semantics are inclusive: each `true` means "deliver this type". `(false, false)` mutes the watch; `(true, true)` is equivalent to not calling. When exactly one is true, the filter is server-side — unwanted events never cross the wire. |
| `WithCreatedNotify(on)` | `false` | Empty response confirming the watch is established. Useful for tests and startup gating. |

### Observability

| Option | Default | Notes |
| --- | --- | --- |
| `WithOnReconnect(fn)` | — | `(attempt, lastErr, lastRevision)`. The revision is where the next watch resumes from — useful for data-gap correlation. |
| `WithOnResync(fn)` | — | Fires after a resync (typically compaction recovery) completes, with the new revision. |
| `WithOnWatchError(fn)` | — | `(err, class)`. Branch on `WatchErrorTransient` / `Compaction` / `Auth` / `Fatal` without string-matching. |
| `WithCloseTimeout(d)` | `5s` | How long `Close()` waits for the watch goroutine to exit before falling back to closing the client. `0` waits forever. |

## Versioning & Go floor

- **Go 1.25+** (this is higher than koanf v2 itself, which is on 1.23 — see below)
- **koanf v2** (current minor — currently `v2.3.x`)
- **etcd client/v3 `v3.5.x`** — pinned to the latest 3.5 patch; the 3.5 line is the widely-deployed LTS and is wire-compatible with 3.4/3.5/3.6 etcd servers

SemVer applies once `v1.0.0` is tagged. Pre-1.0 versions may have breaking changes between minor releases (CHANGELOG calls them out, conventional-commits `!` marker as well).

### Why `go 1.25` when koanf v2 is on `go 1.23`?

This is a real and deliberate gap. **koanf itself** pulls in a tiny runtime tree (`mapstructure` and friends), all of which still build cleanly on Go 1.23. **This provider** pulls in a much heavier dep tree — `go.etcd.io/etcd/client/v3` and its transitive `google.golang.org/grpc` graph, including a large slice of `golang.org/x/*` modules. In mid-2026 that entire grpc/etcd/x-tools ecosystem migrated their go.mod floor to `1.25.0`. Concretely:

- `go.etcd.io/etcd/client/v3 v3.5.31` — the latest 3.5 patch — requires `go 1.25.0`. The last 3.5 patch that allows `go 1.23.0` is `v3.5.21` (about ten patch releases stale).
- `google.golang.org/grpc v1.81+` requires `go 1.25.0`. `v1.74.x` was the last `go 1.23`-compatible line.
- Every `golang.org/x/*` module pulled in by mid-2026 grpc/etcd (`x/net`, `x/sys`, `x/text`, `x/crypto`) declares `go 1.25.0`.

We tried pinning all of those down to keep our floor at `1.23` to match koanf, and it works mechanically — but the price is real:

| | Stay at `go 1.25` (chosen) | Pin everything down to `go 1.23` |
| --- | --- | --- |
| etcd | latest 3.5 patch | `v3.5.21` (~10 patches stale) |
| grpc | latest stable | `v1.74.2` (~7 minors stale) |
| `golang.org/x/*` | latest | manually pinned-old, fragile |
| `govulncheck` | **0 advisories** | several non-callable advisories on older `x/net` + `x/sys` |
| Maintenance | tracks upstream; clean for 6+ months | needs constant pinning every time a new transitive arrives |

We picked **`go 1.25`**. Reasoning: this is a brand-new library, the security argument is binding, Go 1.25 has been the stable release line since early 2026, and the cost of asking new adopters to use a 6-month-old Go release is small. If you genuinely need `go 1.23` compatibility (build farm on an older toolchain, vendored CI), pin to `v0.1.x` of this library — that line was on `go 1.23` end-to-end. Pre-`v1.0.0` we may revisit if upstream catches up.

The `go` directive controls only the consumer floor. The `toolchain` directive in `go.mod` is `go1.25.11`, which means anyone on an older Go toolchain (with the default `GOTOOLCHAIN=auto`) will auto-download `1.25.11` on first build — Go ships toolchain self-management out of the box. Consumers can disable that with `GOTOOLCHAIN=local`, in which case their local Go must satisfy the `1.25` floor.

## License

MIT — see [LICENSE](LICENSE).
