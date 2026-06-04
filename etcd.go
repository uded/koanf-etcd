package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Provider is a koanf v2 Provider for etcd v3.
type Provider struct {
	settings   *settings
	client     *clientv3.Client
	ownsClient bool

	stats atomicStats

	// watch state
	watchMu      sync.Mutex
	watchCancel  context.CancelFunc
	watchCb      func(any, error)
	watchTypedCb func([]Event, error)

	closed atomic.Bool
}

// New constructs a Provider from options. Returns ErrNoMode,
// ErrOptionConflict, or a client-construction error as appropriate.
func New(opts ...Option) (*Provider, error) {
	s := newSettings()

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(s); err != nil {
			return nil, fmt.Errorf("koanf-etcd: apply option: %w", err)
		}
	}

	if err := validateSettings(s); err != nil {
		return nil, err
	}

	if s.logger == nil {
		s.logger = slog.Default()
	}
	if s.redactor == nil {
		s.redactor = defaultRedactor
	}
	if s.onEmpty == nil {
		s.onEmpty = func(prefix string) {
			s.logger.Warn("koanf-etcd: prefix read returned zero keys", "prefix", prefix)
		}
	}
	if s.keyTransform == nil {
		s.keyTransform = makeDefaultKeyTransform(s)
	}
	if s.valueTransform == nil {
		s.valueTransform = defaultValueTransform
	}

	p := &Provider{settings: s}

	if s.client != nil {
		p.client = s.client
		p.ownsClient = false
	} else {
		cli, err := buildClient(s)
		if err != nil {
			return nil, fmt.Errorf("koanf-etcd: build client: %w", err)
		}
		p.client = cli
		p.ownsClient = true
	}

	return p, nil
}

// Close stops any active watch and, if the Provider built its own client,
// closes it. A BYO client supplied via WithClient is not closed.
// Calling Close more than once is a no-op.
func (p *Provider) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}
	p.watchMu.Lock()
	if p.watchCancel != nil {
		p.watchCancel()
		p.watchCancel = nil
	}
	p.watchMu.Unlock()
	if p.ownsClient && p.client != nil {
		return p.client.Close()
	}
	return nil
}

// Revision returns the last etcd revision observed by Read or Watch.
// Zero before the first successful Read.
func (p *Provider) Revision() int64 {
	return p.stats.revision.Load()
}

// Stats returns a snapshot of provider counters.
func (p *Provider) Stats() Stats {
	return p.stats.snapshot()
}

// hasNonDefaultClientConfig reports whether any built-in client option
// has been explicitly set. Used by validateSettings to detect
// `WithClient + WithEndpoints/WithTLS/...` conflicts.
func hasNonDefaultClientConfig(s *settings) bool {
	return len(s.endpoints) > 0 ||
		s.srvService != "" ||
		s.dialTimeout != 5*time.Second || // non-default
		s.keepAliveT != 0 ||
		s.keepAliveTO != 0 ||
		s.autoSync != 0 ||
		s.username != "" ||
		s.password != "" ||
		s.tlsCfg != nil ||
		s.tlsCertFile != "" ||
		s.tlsKeyFile != "" ||
		s.tlsCAFile != "" ||
		s.clientCtx != nil
}

// validateModeSettings enforces the WithKey / WithPrefix / WithBlob /
// WithUnflatten invariants.
func validateModeSettings(s *settings) error {
	hasKey := s.key != ""
	hasPrefix := s.prefix != ""
	switch {
	case !hasKey && !hasPrefix:
		return ErrNoMode
	case hasKey && hasPrefix:
		return fmt.Errorf("%w: WithKey and WithPrefix are mutually exclusive", ErrOptionConflict)
	}
	if !s.blob {
		return nil
	}
	if !hasKey {
		return fmt.Errorf("%w: WithBlob requires WithKey", ErrOptionConflict)
	}
	if s.unflatten {
		return fmt.Errorf("%w: WithBlob is incompatible with WithUnflatten(true)", ErrOptionConflict)
	}
	return nil
}

// validateSettings enforces option-combination invariants.
func validateSettings(s *settings) error {
	if s.client != nil && hasNonDefaultClientConfig(s) {
		return fmt.Errorf("%w: WithClient mixed with built-in connection options", ErrOptionConflict)
	}
	if err := validateModeSettings(s); err != nil {
		return err
	}
	if s.tlsCfg != nil && (s.tlsCertFile != "" || s.tlsKeyFile != "" || s.tlsCAFile != "") {
		return fmt.Errorf("%w: WithTLS and WithTLSFiles are mutually exclusive", ErrOptionConflict)
	}
	return nil
}

// buildClient constructs a *clientv3.Client from settings.
func buildClient(s *settings) (*clientv3.Client, error) {
	cfg := clientv3.Config{
		Endpoints:            s.endpoints,
		DialTimeout:          s.dialTimeout,
		DialKeepAliveTime:    s.keepAliveT,
		DialKeepAliveTimeout: s.keepAliveTO,
		AutoSyncInterval:     s.autoSync,
		Username:             s.username,
		Password:             s.password,
		Context:              s.clientCtx,
	}

	if s.srvService != "" {
		eps, err := lookupSRVEndpoints(s.srvService, s.srvProto, s.srvDomain)
		if err != nil {
			return nil, fmt.Errorf("SRV lookup: %w", err)
		}
		cfg.Endpoints = append(cfg.Endpoints, eps...)
	}

	switch {
	case s.tlsCfg != nil:
		cfg.TLS = s.tlsCfg
	case s.tlsCertFile != "" || s.tlsKeyFile != "" || s.tlsCAFile != "":
		tlsCfg, err := loadTLSFromFiles(s.tlsCertFile, s.tlsKeyFile, s.tlsCAFile)
		if err != nil {
			return nil, err
		}
		cfg.TLS = tlsCfg
	}

	return clientv3.New(cfg)
}

// loadTLSFromFiles builds a *tls.Config from cert/key/CA file paths.
// Any of the three may be empty (e.g. server-auth only loads CA).
func loadTLSFromFiles(certFile, keyFile, caFile string) (*tls.Config, error) {
	cfg := &tls.Config{}
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load keypair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("CA file contains no valid certs")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// defaultRedactor never reveals values.
func defaultRedactor(key string, raw []byte) string {
	return "[REDACTED]"
}

// Watch implements koanf's watch convention. The event payload is nil —
// matches the file provider convention. Callers wanting per-key detail
// should use WatchTyped. Calling Watch when a watch is already active
// returns an error.
func (p *Provider) Watch(cb func(event any, err error)) error {
	if p.closed.Load() {
		return ErrClosed
	}
	p.watchMu.Lock()
	defer p.watchMu.Unlock()
	if p.watchCancel != nil {
		return fmt.Errorf("koanf-etcd: watch already active")
	}
	parent := p.settings.watchCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	p.watchCancel = cancel
	p.watchCb = cb
	go p.watchLoop(ctx)
	return nil
}

// WatchTyped delivers []Event batches to cb. ctx cancels the watcher.
// Mutually exclusive with Watch().
func (p *Provider) WatchTyped(ctx context.Context, cb func([]Event, error)) error {
	if p.closed.Load() {
		return ErrClosed
	}
	p.watchMu.Lock()
	defer p.watchMu.Unlock()
	if p.watchCancel != nil {
		return fmt.Errorf("koanf-etcd: watch already active")
	}
	loopCtx, cancel := context.WithCancel(ctx)
	p.watchCancel = cancel
	p.watchTypedCb = cb
	go p.watchLoop(loopCtx)
	return nil
}

// Event is a single change observed by WatchTyped. Resync events have
// empty Key and Value; the entire current state was re-read.
type Event struct {
	Type     EventType
	Key      string
	Value    []byte
	Revision int64
}

// EventType classifies an Event.
type EventType int

const (
	// EventPut is delivered when a key is created or updated.
	EventPut EventType = iota + 1
	// EventDelete is delivered when a key is removed.
	EventDelete
	// EventResync is delivered after a full re-read following compaction.
	EventResync
)
