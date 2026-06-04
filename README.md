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
| `*slog.Logger` | ❌ | ✅ |
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

- **TLS**: pass `*tls.Config` via `WithTLS` or load cert/key/CA files via `WithTLSFiles`. The two are mutually exclusive.
- **Auth**: `WithAuth("user", "pass")`. Never hardcode — pull from env or a secret manager.
- **Redaction**: the default redactor returns `"[REDACTED]"`. No value is ever logged. Override with `WithRedactor` only if you have a specific safe transform.
- **BYO client lifecycle**: a BYO client is never closed by the Provider.

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

## Options reference (abridged)

See `go doc github.com/uded/koanf-etcd` for the full surface. Highlights:

| Option | Default |
| --- | --- |
| `WithDelim(s)` | `"."` |
| `WithUnflatten(on)` | `true` |
| `WithTrimPrefix(on)` | `true` (in prefix mode) |
| `WithReadTimeout(d)` | `5s` |
| `WithReconnectBackoff(min, max)` | `1s..2min` |
| `WithDebounce(d)` | `0` (off) |
| `WithResumeFromRevision(on)` | `true` |
| `WithStrict(on)` | `false` |

## Versioning

- Go 1.23+
- koanf v2 (current minor)
- etcd client/v3 v3.5.x

SemVer applies once `v1.0.0` is tagged. Pre-1.0 versions may have breaking changes between minor releases.

## License

MIT — see [LICENSE](LICENSE).
