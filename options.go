package etcd

import (
	"context"
	"crypto/tls"
	"log/slog"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Option configures a Provider. Options are applied left-to-right by New
// and validated as a group; conflicting combinations return ErrOptionConflict.
type Option func(*settings) error

// WithClient supplies a pre-constructed *clientv3.Client. Mutually
// exclusive with all other connection options. Close() does NOT close a
// BYO client — the caller retains ownership.
func WithClient(c *clientv3.Client) Option {
	return func(s *settings) error {
		s.client = c
		return nil
	}
}

// WithEndpoints sets the etcd endpoints used to construct the client.
// Ignored when WithClient is set.
func WithEndpoints(eps ...string) Option {
	return func(s *settings) error {
		s.endpoints = append([]string(nil), eps...)
		return nil
	}
}

// WithEndpointsFromSRV configures DNS SRV lookup to discover endpoints
// at New() time. service/proto/domain map to clientv3 SRV discovery.
func WithEndpointsFromSRV(service, proto, domain string) Option {
	return func(s *settings) error {
		s.srvService = service
		s.srvProto = proto
		s.srvDomain = domain
		return nil
	}
}

// WithDialTimeout sets the dial timeout for built-in client construction.
func WithDialTimeout(d time.Duration) Option {
	return func(s *settings) error { s.dialTimeout = d; return nil }
}

// WithKeepAlive sets gRPC keepalive time and timeout for the built-in
// client.
func WithKeepAlive(t, timeout time.Duration) Option {
	return func(s *settings) error {
		s.keepAliveT = t
		s.keepAliveTO = timeout
		return nil
	}
}

// WithAutoSync sets the AutoSyncInterval for the built-in client.
func WithAutoSync(d time.Duration) Option {
	return func(s *settings) error { s.autoSync = d; return nil }
}

// WithAuth supplies username/password for the built-in client.
func WithAuth(user, pass string) Option {
	return func(s *settings) error {
		s.username = user
		s.password = pass
		return nil
	}
}

// WithTLS supplies a *tls.Config for the built-in client.
func WithTLS(cfg *tls.Config) Option {
	return func(s *settings) error { s.tlsCfg = cfg; return nil }
}

// WithTLSFiles is a convenience that loads a client cert, key, and CA
// from disk at New() time and builds a *tls.Config. Mutually exclusive
// with WithTLS (both being set returns ErrOptionConflict in New()).
func WithTLSFiles(certFile, keyFile, caFile string) Option {
	return func(s *settings) error {
		s.tlsCertFile = certFile
		s.tlsKeyFile = keyFile
		s.tlsCAFile = caFile
		return nil
	}
}

// WithLogger sets a *slog.Logger for the provider. Default: slog.Default().
// The provider never logs values; redacted-by-default. See WithRedactor.
func WithLogger(l *slog.Logger) Option {
	return func(s *settings) error { s.logger = l; return nil }
}

// WithClientContext sets the Context the built-in client uses for its
// lifecycle. Ignored when WithClient is set.
func WithClientContext(ctx context.Context) Option {
	return func(s *settings) error { s.clientCtx = ctx; return nil }
}

// --- mode ---

// WithKey selects single-key mode. The Provider reads exactly that key.
// Mutually exclusive with WithPrefix.
func WithKey(key string) Option {
	return func(s *settings) error { s.key = key; return nil }
}

// WithPrefix selects tree mode. The Provider reads all keys under the
// given prefix. Mutually exclusive with WithKey.
func WithPrefix(prefix string) Option {
	return func(s *settings) error { s.prefix = prefix; return nil }
}

// WithBlob selects blob mode. Read() returns ErrUseParser; ReadBytes()
// returns the raw value of the configured key. Requires WithKey.
// Mutually exclusive with WithUnflatten(true).
func WithBlob() Option {
	return func(s *settings) error { s.blob = true; return nil }
}

// --- read shaping ---

// WithDelim sets the key path delimiter used for unflattening keys into
// nested maps. Default ".".
func WithDelim(d string) Option {
	return func(s *settings) error { s.delim = d; return nil }
}

// WithTrimPrefix toggles stripping the configured prefix from each key
// before mapping to a koanf path. Default true in prefix mode.
func WithTrimPrefix(on bool) Option {
	return func(s *settings) error { s.trimPrefix = on; return nil }
}

// WithUnflatten toggles converting flat dotted keys into nested maps.
// Default true. Set to false if you want the raw flat layout.
func WithUnflatten(on bool) Option {
	return func(s *settings) error { s.unflatten = on; return nil }
}

// WithKeyTransform installs a function that maps an etcd key to a koanf
// path. Runs after prefix trim (if enabled). Default: replace any "/" in
// the (trimmed) key with the configured delimiter.
func WithKeyTransform(fn func(string) string) Option {
	return func(s *settings) error { s.keyTransform = fn; return nil }
}

// WithValueTransform installs a function that converts raw bytes to the
// final value placed in the koanf map. Default: TrimSpace and return as
// string.
func WithValueTransform(fn func(key string, raw []byte) (any, error)) Option {
	return func(s *settings) error { s.valueTransform = fn; return nil }
}

// WithLimit sets a page size for prefix reads. The Provider paginates
// with WithLimit + WithFromKey across the prefix. 0 = no pagination
// (one Get call). Recommended >0 for prefixes >1k keys.
func WithLimit(n int64) Option {
	return func(s *settings) error { s.limit = n; return nil }
}

// WithSerializable opts in to serializable (vs linearizable) reads. The
// resulting Get is faster and lighter on the cluster but may return
// slightly stale data. Suitable for boot-time defaults.
func WithSerializable(on bool) Option {
	return func(s *settings) error { s.serializable = on; return nil }
}

// WithReadRevision reads at an explicit revision (clientv3.WithRev). Used
// for reproducible snapshot reads.
func WithReadRevision(rev int64) Option {
	return func(s *settings) error { s.readRevision = rev; return nil }
}

// WithReadTimeout bounds each Get call. Default 5s.
func WithReadTimeout(d time.Duration) Option {
	return func(s *settings) error { s.readTimeout = d; return nil }
}

// --- empty handling ---

// WithStrict makes Read return ErrEmptyPrefix when a prefix read yields
// zero keys. Default false (the OnEmpty callback fires instead).
func WithStrict(on bool) Option {
	return func(s *settings) error { s.strict = on; return nil }
}

// OnEmpty registers a callback fired when a non-strict prefix read
// returns zero keys. Default: a slog.Warn.
func OnEmpty(fn func(prefix string)) Option {
	return func(s *settings) error { s.onEmpty = fn; return nil }
}

// --- watch ---

// WithWatchContext sets the parent context for the watch goroutine.
// Cancelling it cleanly stops the watcher. Default: the Provider's
// internal context (cancelled by Close).
func WithWatchContext(ctx context.Context) Option {
	return func(s *settings) error { s.watchCtx = ctx; return nil }
}

// WithDebounce coalesces a burst of watch events into one callback,
// firing after the window elapses with no new event. 0 = no debounce
// (every event fires immediately). Recommended 100ms-1s for scripted
// multi-put rollouts.
func WithDebounce(window time.Duration) Option {
	return func(s *settings) error { s.debounce = window; return nil }
}

// WithReconnectBackoff sets the bounds for exponential backoff (with full
// jitter) used when the watch channel closes. Default min=1s, max=2min.
func WithReconnectBackoff(min, max time.Duration) Option {
	return func(s *settings) error {
		s.reconnectMin = min
		s.reconnectMax = max
		return nil
	}
}

// WithResumeFromRevision toggles starting the watch at
// (initialReadRevision + 1) so no event between Read() and Watch.Start
// is missed. Default true.
func WithResumeFromRevision(on bool) Option {
	return func(s *settings) error { s.resumeFromRevision = on; return nil }
}

// WithProgressNotify enables clientv3.WithProgressNotify on the watch
// stream — etcd sends periodic empty responses so the watcher can confirm
// liveness even when no keys change.
func WithProgressNotify(on bool) Option {
	return func(s *settings) error { s.progressNotify = on; return nil }
}

// WithEventFilter selects which event types Watch / WatchTyped delivers.
// The booleans declare "deliver this type":
//
//   - WithEventFilter(true, false)  → deliver only puts
//   - WithEventFilter(false, true)  → deliver only deletes
//   - WithEventFilter(true, true)   → deliver both (same as not calling)
//   - WithEventFilter(false, false) → deliver neither (effectively a mute)
//
// Default (option not set) is "deliver both". When exactly one boolean
// is true, the corresponding server-side clientv3 filter is applied so
// unwanted events never cross the wire.
func WithEventFilter(put, deleteEv bool) Option {
	return func(s *settings) error {
		s.wantPut = put
		s.wantDelete = deleteEv
		s.filterSet = true
		return nil
	}
}

// WithCreatedNotify enables clientv3.WithCreatedNotify on the watch
// stream — etcd sends an empty response confirming the watch is
// established. Useful for tests and as a startup gate.
func WithCreatedNotify(on bool) Option {
	return func(s *settings) error { s.createdNotify = on; return nil }
}

// WithRedactor sets a function that produces a safe display string for a
// (key, value) pair. Used by all internal logging touching values.
// Default: returns "[REDACTED]". Set to func(k string, raw []byte) string
// { return string(raw) } to disable redaction (NOT recommended in
// production).
func WithRedactor(fn func(key string, raw []byte) string) Option {
	return func(s *settings) error { s.redactor = fn; return nil }
}

// WithOnReconnect fires after each successful watch reconnect. attempt is
// the 1-indexed retry count since the last successful connect.
func WithOnReconnect(fn func(attempt int, lastErr error)) Option {
	return func(s *settings) error { s.onReconnect = fn; return nil }
}

// WithOnResync fires after a resync (typically compaction recovery)
// completes, with the new revision the watcher resumes from.
func WithOnResync(fn func(reason string, newRevision int64)) Option {
	return func(s *settings) error { s.onResync = fn; return nil }
}

// WithOnWatchError fires on each non-recoverable watch error before the
// watcher retries.
func WithOnWatchError(fn func(err error)) Option {
	return func(s *settings) error { s.onWatchError = fn; return nil }
}
