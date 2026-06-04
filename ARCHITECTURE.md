# Architecture

## Overview

`koanf-etcd` is a [koanf](https://github.com/knadh/koanf) v2 Provider that loads
configuration from etcd v3 and surfaces live updates via a watch loop. In one
sentence: a small, focused read-only Provider for etcd v3 that plays nicely with
the rest of the koanf ecosystem.

What it intentionally **is not**:

- It does not validate configuration. Compose with
  [`koanf-validate`](https://github.com/uded/koanf-validate) if you need schema
  enforcement.
- It does not apply defaults. Compose with
  [`koanf-structdefaults`](https://github.com/uded/koanf-structdefaults) (or
  `koanf.WithMergeFunc`) for that.
- The `Provider` type exposes **no write methods**. Atomic write helpers exist
  as a deliberately separate package (`write/`) so the Provider stays a
  config-read surface.

## Design principles

- **Single responsibility.** The Provider reads keys under a prefix from etcd
  and emits change signals. Nothing else.
- **Bring-your-own client.** The headline escape hatch is `WithClient(*clientv3.Client)`.
  Production deployments typically already have a configured, traced,
  metric-instrumented etcd client; the Provider must not force a second one.
- **Safety by default.** Values are `TrimSpace`d, the output is nested via the
  configured delimiter, and empty values are dropped unless the caller opts in
  with `WithKeepEmpty`.
- **Compose with siblings.** Treat the Provider as one node in a koanf pipeline
  alongside structdefaults, validate, file, env, and other providers.

## Module structure

The public surface lives in a single package, `etcd`, with one subpackage for
write helpers.

| File | Owns |
|------|------|
| `doc.go` | Package-level godoc and high-level usage examples. |
| `errors.go` | Sentinel errors (`ErrClosed`, `ErrEmptyPrefix`, watch-fatal sentinels). |
| `settings.go` | Internal `settings` struct, defaults, and the public `Stats` type. |
| `options.go` | Functional `Option` constructors (`WithClient`, `WithDelimiter`, `WithReconnectBackoff`, `WithDebounce`, `WithKeepEmpty`, `WithRedactor`, TLS options, …). |
| `transform.go` | Default key (`/` → delimiter) and value (`TrimSpace`) transforms. |
| `etcd.go` | The `Provider` type, `New`, `Close`, the koanf `Read`/`ReadBytes` wiring, the `Watch`/`WatchTyped` shims, and the `Event`/`EventType` types. |
| `read.go` | `Read` implementation: paginated range, key/value transform, unflatten. |
| `watch.go` | The watch state machine: connect, dispatch, reconnect with backoff, resync after compaction, debounce. |
| `write/` | Atomic multi-key write helpers built on etcd transactions. Separate package by design. |

Test infrastructure currently lives in `testutil_test.go` inside the main
package. The embedded-etcd server it spins up drags
`go.etcd.io/etcd/server/v3` and its transitive tree into the module graph, so
consumers see those deps via tooling like `go mod why`. Splitting this into a
nested test module is the headline v0.2.0 item.

## Watch state machine

`Watch(cb)` and `WatchTyped(cb)` both attach to a single long-lived loop.

```
                  +---------------------+
   start  ----->  |     watchLoop       |  <-----+
                  +----------+----------+        |
                             |                   |  ctx still live
                             v                   |  AND non-fatal err
                  +---------------------+        |
                  |    consumeWatch     |  ------+
                  +----+-------+--------+
                       |       |
       compaction      |       |     transport / lease loss
       (resync)        |       |     (reconnect with backoff)
                       v       v
              +---------------+   +-------------------+
              |   Read(ctx)   |   | sleep(jittered    |
              |  → rev, data  |   |   exp backoff)    |
              +-------+-------+   +---------+---------+
                      |                     |
                      +----------+----------+
                                 |
                                 v
                       resume Watch at rev+1
```

The loop captures the latest revision from `Read()` and resumes the next
`Watch` call at `rev+1`, which closes the read/watch gap that would otherwise
allow a write to slip past unseen. Reconnect uses exponential backoff with full
jitter, bounded by `WithReconnectBackoff`. The `attempt` counter resets only
when the session made real progress (received at least one event or stayed up
past the floor), so a fast-flapping cluster cannot reset the cap on every
failure.

## Key design decisions and trade-offs

**Why a nil event for `Watch(cb)`.** `Watch` mirrors the convention of the
koanf file provider: it is a pure edge-trigger signal that says "something
changed, reload". This keeps the simple-watch contract identical across
providers, so users can swap file for etcd without rewriting their reload
logic. Per-key detail is available through `WatchTyped(cb func(Event))`.

**Why a separate `write/` subpackage.** The Provider's only job is to read.
Write helpers exist to support common patterns (atomic multi-key rollouts via
etcd transactions, value-with-lease writes), but living off the read path
means the Provider's API never tempts a caller into writing a config back
through it. The separation is intentional and structural, not stylistic.

**Why raw bytes in `WatchTyped`'s `Event.Value`.** Consumers may want to
parse JSON, protobuf, or some other binary payload without first round-tripping
through the default `TrimSpace`/string conversion that `Read()` applies.
`Read()` honors the configured value transform; `WatchTyped` deliberately
hands out the raw bytes from etcd. This is documented on the `Event` godoc.

**Why etcd `client/v3` v3.5.x not v3.6.** The 3.5 line is the broadly-deployed
client today. The client API is wire-compatible with 3.4, 3.5, and 3.6
servers, so pinning to 3.5 maximizes the cluster matrix consumers can target
without forcing them to upgrade. v3.6 client support is on the roadmap as a
nightly matrix entry.

## Open invariants

The watch loop and the Provider rely on a small set of contracts that must
hold across all options and refactors:

- Cancelling the context passed to `New` tears down the watch and the
  internal goroutines deterministically.
- A `*clientv3.Client` passed via `WithClient` is owned by the caller. The
  Provider never calls `Close` on it.
- Values are `TrimSpace`d by the default value transform; callers can
  override or disable this via `WithValueTransform`.
- The revision counter the watch loop tracks advances monotonically across
  reconnects, resyncs, and debounce windows.
