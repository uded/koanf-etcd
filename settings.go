package etcd

import (
	"context"
	"crypto/tls"
	"log/slog"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// settings is the internal, immutable-after-New configuration produced
// by applying functional Options. It is never exposed to callers.
type settings struct {
	// connection — either client (BYO) or built-in fields, never both
	client      *clientv3.Client
	endpoints   []string
	srvService  string // SRV discovery: service / proto / domain
	srvProto    string
	srvDomain   string
	dialTimeout time.Duration
	keepAliveT  time.Duration
	keepAliveTO time.Duration
	autoSync    time.Duration
	username    string
	password    string
	tlsCfg      *tls.Config
	tlsCertFile string
	tlsKeyFile  string
	tlsCAFile   string
	clientCtx   context.Context
	logger      *slog.Logger

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
	reconnectMin       time.Duration
	reconnectMax       time.Duration
	resumeFromRevision bool
	progressNotify     bool
	wantPut            bool // see WithEventFilter — meaningful only when filterSet
	wantDelete         bool
	filterSet          bool // true once WithEventFilter explicitly applied
	createdNotify      bool
	redactor           func(key string, raw []byte) string
	onReconnect        func(attempt int, lastErr error)
	onResync           func(reason string, newRevision int64)
	onWatchError       func(err error)
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
		reconnectMin:       1 * time.Second,
		reconnectMax:       2 * time.Minute,
		resumeFromRevision: true,
		strict:             false,
	}
}

// Stats is a snapshot of provider counters. Read via Provider.Stats().
// Safe to call from any goroutine, including from inside Watch / WatchTyped
// callbacks. Fields are atomic snapshots; no cross-field consistency
// guarantee.
type Stats struct {
	Revision        int64
	TotalPuts       uint64
	TotalDeletes    uint64
	TotalResyncs    uint64
	TotalReconnects uint64
	LastEventBatch  int
	LastResyncAt    time.Time
	LastReconnectAt time.Time
}

// atomicStats holds the live counters. Snapshotted to Stats via
// snapshot().
type atomicStats struct {
	revision        atomic.Int64
	totalPuts       atomic.Uint64
	totalDeletes    atomic.Uint64
	totalResyncs    atomic.Uint64
	totalReconnects atomic.Uint64
	lastBatch       atomic.Int32
	lastResyncUnix  atomic.Int64
	lastReconnUnix  atomic.Int64
}

func (a *atomicStats) snapshot() Stats {
	rs := a.lastResyncUnix.Load()
	rc := a.lastReconnUnix.Load()
	var lr, lrc time.Time
	if rs != 0 {
		lr = time.Unix(0, rs)
	}
	if rc != 0 {
		lrc = time.Unix(0, rc)
	}
	return Stats{
		Revision:        a.revision.Load(),
		TotalPuts:       a.totalPuts.Load(),
		TotalDeletes:    a.totalDeletes.Load(),
		TotalResyncs:    a.totalResyncs.Load(),
		TotalReconnects: a.totalReconnects.Load(),
		LastEventBatch:  int(a.lastBatch.Load()),
		LastResyncAt:    lr,
		LastReconnectAt: lrc,
	}
}
