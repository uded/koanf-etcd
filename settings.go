package etcd

import (
	"context"
	"crypto/tls"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// settings is the internal, immutable-after-New configuration produced
// by applying functional Options. It is never exposed to callers.
type settings struct {
	// connection — either client (BYO) or built-in fields, never both
	client        *clientv3.Client
	endpoints     []string
	srvService    string // SRV discovery: service / proto / domain
	srvProto      string
	srvDomain     string
	dialTimeout   time.Duration
	keepAliveT    time.Duration
	keepAliveTO   time.Duration
	autoSync      time.Duration
	username      string
	password      string
	tlsCfg        *tls.Config
	tlsCertFile   string
	tlsKeyFile    string
	tlsCAFile     string
	tlsServerName string
	clientCtx     context.Context

	// wasSet tracks which connection options the caller explicitly
	// supplied so BYO-client conflict detection doesn't depend on
	// value-equality with defaults.
	endpointsSet   bool
	srvSet         bool
	dialTimeoutSet bool
	keepAliveSet   bool
	autoSyncSet    bool
	authSet        bool
	tlsSet         bool // any of WithTLS / WithTLSFiles / WithTLSServerName
	clientCtxSet   bool

	// mode
	key    string
	prefix string
	blob   bool

	// read shaping
	delim          string
	trimPrefix     bool
	unflatten      bool
	keyTransform   func(string) string
	valueTransform func(key string, raw []byte) (any, error)
	limit          int64
	serializable   bool
	readRevision   int64
	readTimeout    time.Duration

	// empty handling
	strict  bool
	onEmpty func(prefix string)

	// watch
	watchCtx           context.Context
	debounce           time.Duration
	maxPendingEvents   int // 0 = unbounded (legacy), positive = force flush at threshold
	reconnectMin       time.Duration
	reconnectMax       time.Duration
	resumeFromRevision bool
	progressNotify     bool
	wantPut            bool // see WithEventFilter — meaningful only when filterSet
	wantDelete         bool
	filterSet          bool // true once WithEventFilter explicitly applied
	createdNotify      bool
	onReconnect        func(attempt int, lastErr error, lastRevision int64)
	onResync           func(reason string, newRevision int64)
	onWatchError       func(err error, class WatchErrorClass)

	// lifecycle
	closeTimeout time.Duration // 0 = no timeout; default 5s
}

// newSettings returns a settings populated with default values. Options
// applied via New() override these defaults.
func newSettings() *settings {
	return &settings{
		dialTimeout:        5 * time.Second,
		delim:              ".",
		trimPrefix:         true,
		unflatten:          true,
		readTimeout:        5 * time.Second,
		debounce:           0,
		maxPendingEvents:   10000,
		reconnectMin:       1 * time.Second,
		reconnectMax:       2 * time.Minute,
		resumeFromRevision: true,
		strict:             false,
		closeTimeout:       5 * time.Second,
	}
}

// Stats is a snapshot of provider counters. Read via Provider.Stats().
// Safe to call from any goroutine, including from inside Watch /
// WatchTyped callbacks. Fields are atomic snapshots; there is no
// cross-field consistency guarantee.
type Stats struct {
	// Revision is the most recent etcd revision observed by Read() or
	// the watch loop. Zero before the first successful Read.
	Revision int64

	// TotalPuts is the cumulative count of put events delivered to
	// Watch / WatchTyped callbacks since New().
	TotalPuts uint64

	// TotalDeletes is the cumulative count of delete events delivered.
	TotalDeletes uint64

	// TotalResyncs is the cumulative count of full-state re-reads
	// triggered by etcd compaction.
	TotalResyncs uint64

	// TotalReconnects is the cumulative count of watch reconnect
	// attempts, including both successful and backoff-pending ones.
	TotalReconnects uint64

	// LastEventBatch is the number of events delivered in the most
	// recent callback invocation. Useful for spotting flush coalescing.
	LastEventBatch int

	// LastResyncAt is the wall-clock time of the most recent resync.
	// Zero if no resync has occurred.
	LastResyncAt time.Time

	// LastReconnectAt is the wall-clock time of the most recent
	// reconnect attempt. Zero if no reconnect has occurred.
	LastReconnectAt time.Time

	// TotalWatchErrors is the cumulative count of non-recoverable
	// errors the watch loop has observed (compaction does NOT count;
	// it's counted under TotalResyncs).
	TotalWatchErrors uint64

	// TotalEventsDelivered is the cumulative count of individual Event
	// objects delivered to callbacks. Roughly TotalPuts + TotalDeletes
	// + TotalResyncs, but tracked independently for cheaper assertions.
	TotalEventsDelivered uint64

	// TotalReads is the cumulative count of Read() and ReadBytes()
	// invocations that returned without error.
	TotalReads uint64

	// LastWatchErrorAt is the wall-clock time of the most recent watch
	// error (recoverable or not). Zero if no watch error has occurred.
	LastWatchErrorAt time.Time

	// TotalDebounceFlushes is the cumulative count of debounce window
	// flushes that delivered at least one event.
	TotalDebounceFlushes uint64

	// LastFlushCoalescedCount is the event count of the most recent
	// debounce flush. A useful "are we coalescing usefully?" signal.
	LastFlushCoalescedCount int
}

// atomicStats holds the live counters. Snapshotted to Stats via
// snapshot().
type atomicStats struct {
	revision             atomic.Int64
	totalPuts            atomic.Uint64
	totalDeletes         atomic.Uint64
	totalResyncs         atomic.Uint64
	totalReconnects      atomic.Uint64
	lastBatch            atomic.Int32
	lastResyncUnix       atomic.Int64
	lastReconnUnix       atomic.Int64
	totalWatchErrors     atomic.Uint64
	totalEventsDelivered atomic.Uint64
	totalReads           atomic.Uint64
	lastWatchErrorUnix   atomic.Int64
	totalDebounceFlushes atomic.Uint64
	lastFlushCoalesced   atomic.Int32
}

func (a *atomicStats) snapshot() Stats {
	rs := a.lastResyncUnix.Load()
	rc := a.lastReconnUnix.Load()
	we := a.lastWatchErrorUnix.Load()
	var lr, lrc, lwe time.Time
	if rs != 0 {
		lr = time.Unix(0, rs)
	}
	if rc != 0 {
		lrc = time.Unix(0, rc)
	}
	if we != 0 {
		lwe = time.Unix(0, we)
	}
	return Stats{
		Revision:                a.revision.Load(),
		TotalPuts:               a.totalPuts.Load(),
		TotalDeletes:            a.totalDeletes.Load(),
		TotalResyncs:            a.totalResyncs.Load(),
		TotalReconnects:         a.totalReconnects.Load(),
		LastEventBatch:          int(a.lastBatch.Load()),
		LastResyncAt:            lr,
		LastReconnectAt:         lrc,
		TotalWatchErrors:        a.totalWatchErrors.Load(),
		TotalEventsDelivered:    a.totalEventsDelivered.Load(),
		TotalReads:              a.totalReads.Load(),
		LastWatchErrorAt:        lwe,
		TotalDebounceFlushes:    a.totalDebounceFlushes.Load(),
		LastFlushCoalescedCount: int(a.lastFlushCoalesced.Load()),
	}
}
