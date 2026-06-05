package integration_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
)

// TestMain configures goleak to ignore well-known noisy goroutines from
// gRPC, etcd, and the standard library so per-test goleak.VerifyNone is
// meaningful.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/v3.(*lessor).deadlineLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	)
}

// embeddedEtcd boots a single-node embedded etcd on random localhost ports
// and returns a connected clientv3.Client plus a teardown func. The
// teardown is registered with t.Cleanup so callers don't need to defer it.
func embeddedEtcd(t testing.TB) *clientv3.Client {
	t.Helper()

	dir, err := os.MkdirTemp("", "koanf-etcd-test-*")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}

	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{{Scheme: "http", Host: pickAddr(t)}}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{{Scheme: "http", Host: pickAddr(t)}}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = fmt.Sprintf("default=%s", cfg.ListenPeerUrls[0].String())

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("start embedded etcd: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		e.Close()
		os.RemoveAll(dir)
		t.Fatalf("embedded etcd did not become ready in 15s")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.ListenClientUrls[0].Host},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		e.Close()
		os.RemoveAll(dir)
		t.Fatalf("new client: %v", err)
	}

	t.Cleanup(func() {
		_ = cli.Close()
		e.Close()
		select {
		case <-e.Server.StopNotify():
		case <-time.After(5 * time.Second):
		}
		os.RemoveAll(dir)
	})

	return cli
}

// embeddedEtcdB is a thin alias for embeddedEtcd that takes *testing.B.
// It exists so benchmarks read naturally; embeddedEtcd already accepts
// testing.TB.
func embeddedEtcdB(b *testing.B) *clientv3.Client {
	b.Helper()
	return embeddedEtcd(b)
}

// pickAddr finds a free localhost TCP port and returns "127.0.0.1:N".
func pickAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick free port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// ctxWithTimeout returns a context with the given timeout and registers
// cancel via t.Cleanup.
func ctxWithTimeout(t testing.TB, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// TestEmbeddedEtcd_Sanity asserts the harness can boot etcd, accept a
// put, and read it back. If this fails, nothing else can be trusted.
func TestEmbeddedEtcd_Sanity(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)

	if _, err := cli.Put(ctx, "/sanity", "ok"); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := cli.Get(ctx, "/sanity")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "ok" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}
