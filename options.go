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
