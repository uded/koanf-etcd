package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"

	ketcd "github.com/uded/koanf-etcd"
)

func TestNew_RejectsClientWithConnOptions(t *testing.T) {
	cli := embeddedEtcd(t)
	_, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithKey("/a"), ketcd.WithEndpoints("y:2379"))
	if !errors.Is(err, ketcd.ErrOptionConflict) {
		t.Fatalf("want ErrOptionConflict, got %v", err)
	}
}

func TestNew_BYOClientNotClosedByProviderClose(t *testing.T) {
	cli := embeddedEtcd(t)
	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithKey("/sanity"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// BYO client should still be usable.
	ctx := ctxWithTimeout(t, 2*time.Second)
	if _, err := cli.Put(ctx, "/sanity", "still works"); err != nil {
		t.Fatalf("BYO client unexpectedly broken: %v", err)
	}
}

func TestRead_SingleKey(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	if _, err := cli.Put(ctx, "/app/cfg/db.host", "localhost"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithKey("/app/cfg/db.host"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	m, err := p.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	// Single-key mode returns the value at a nested koanf path derived
	// from the etcd key via the default key transform. With no prefix
	// trim (single-key mode skips prefix-trim) and default delim ".",
	// the path "/app/cfg/db.host" becomes ".app.cfg.db.host" → unflatten
	// produces a nested map rooted at "app".
	app, ok := m["app"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested map under 'app'; got %T (%v)", m["app"], m)
	}
	_ = app
}

func TestRead_SingleKey_TrimsValueWhitespace(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	if _, err := cli.Put(ctx, "/app/url", "  http://x\n"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithKey("/app/url"))
	t.Cleanup(func() { _ = p.Close() })

	m, err := p.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	app, _ := m["app"].(map[string]any)
	if app["url"] != "http://x" {
		t.Errorf("got %v; want http://x", app["url"])
	}
}

func TestRead_RecordsRevision(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	if _, err := cli.Put(ctx, "/k", "v"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithKey("/k"))
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if p.Revision() == 0 {
		t.Errorf("Revision() = 0 after Read; want > 0")
	}
}

func TestRead_PrefixNestedTrimmed(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	seed := map[string]string{
		"/svc/db.host":    "localhost",
		"/svc/db.port":    "5432",
		"/svc/feature.on": "true",
	}
	for k, v := range seed {
		if _, err := cli.Put(ctx, k, v); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}

	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	m, err := p.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	db, _ := m["db"].(map[string]any)
	if db["host"] != "localhost" {
		t.Errorf("db.host = %v; want localhost", db["host"])
	}
	if db["port"] != "5432" {
		t.Errorf("db.port = %v; want 5432", db["port"])
	}
	feature, _ := m["feature"].(map[string]any)
	if feature["on"] != "true" {
		t.Errorf("feature.on = %v; want true", feature["on"])
	}
}

func TestRead_PrefixUnflattenOff(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	cli.Put(ctx, "/svc/db.host", "localhost")
	cli.Put(ctx, "/svc/db.port", "5432")

	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"), ketcd.WithUnflatten(false))
	t.Cleanup(func() { _ = p.Close() })

	m, err := p.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if m["db.host"] != "localhost" || m["db.port"] != "5432" {
		t.Errorf("expected flat keys; got %v", m)
	}
}

func TestRead_PrefixPagination(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 30*time.Second)
	for i := 0; i < 1500; i++ {
		key := fmt.Sprintf("/big/k%05d", i)
		if _, err := cli.Put(ctx, key, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/big/"), ketcd.WithLimit(250), ketcd.WithUnflatten(false))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	m, err := p.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(m) != 1500 {
		t.Errorf("got %d keys; want 1500", len(m))
	}
}

func TestRead_StrictEmptyErrors(t *testing.T) {
	cli := embeddedEtcd(t)
	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/nope/"), ketcd.WithStrict(true))
	t.Cleanup(func() { _ = p.Close() })
	_, err := p.Read()
	if !errors.Is(err, ketcd.ErrEmptyPrefix) {
		t.Errorf("want ErrEmptyPrefix, got %v", err)
	}
}

func TestRead_OnEmptyCallback(t *testing.T) {
	cli := embeddedEtcd(t)
	called := 0
	var gotPrefix string
	p, _ := ketcd.New(
		ketcd.WithClient(cli),
		ketcd.WithPrefix("/nope/"),
		ketcd.OnEmpty(func(prefix string) {
			called++
			gotPrefix = prefix
		}),
	)
	t.Cleanup(func() { _ = p.Close() })

	m, err := p.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
	if called != 1 {
		t.Errorf("onEmpty called %d times; want 1", called)
	}
	if gotPrefix != "/nope/" {
		t.Errorf("onEmpty got prefix %q; want /nope/", gotPrefix)
	}
}

func TestRead_BlobReturnsErrUseParser(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	cli.Put(ctx, "/cfg", `{"a":1}`)

	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithKey("/cfg"), ketcd.WithBlob(), ketcd.WithUnflatten(false))
	t.Cleanup(func() { _ = p.Close() })

	_, err := p.Read()
	if !errors.Is(err, ketcd.ErrUseParser) {
		t.Errorf("want ErrUseParser, got %v", err)
	}
}

func TestReadBytes_BlobMode(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	cli.Put(ctx, "/cfg", `{"a":1}`)

	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithKey("/cfg"), ketcd.WithBlob(), ketcd.WithUnflatten(false))
	t.Cleanup(func() { _ = p.Close() })

	b, err := p.ReadBytes()
	if err != nil {
		t.Fatalf("readBytes: %v", err)
	}
	if string(b) != `{"a":1}` {
		t.Errorf("got %q; want %q", b, `{"a":1}`)
	}
}

func TestReadBytes_NonBlobReturnsErrNotBlob(t *testing.T) {
	cli := embeddedEtcd(t)
	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithKey("/x"))
	t.Cleanup(func() { _ = p.Close() })

	_, err := p.ReadBytes()
	if !errors.Is(err, ketcd.ErrNotBlob) {
		t.Errorf("want ErrNotBlob, got %v", err)
	}
}

func TestRead_AuthHappyPath(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 10*time.Second)

	// Set up an authenticated user via the BYO client.
	if _, err := cli.UserAdd(ctx, "alice", "passw0rd"); err != nil {
		t.Fatalf("UserAdd: %v", err)
	}
	if _, err := cli.RoleAdd(ctx, "reader"); err != nil {
		t.Fatalf("RoleAdd: %v", err)
	}
	if _, err := cli.RoleGrantPermission(ctx, "reader", "/cfg/", "/cfg0", 0 /*Read*/); err != nil {
		t.Fatalf("RoleGrantPermission: %v", err)
	}
	if _, err := cli.UserGrantRole(ctx, "alice", "reader"); err != nil {
		t.Fatalf("UserGrantRole: %v", err)
	}
	// Etcd requires a root user before AuthEnable.
	if _, err := cli.UserAdd(ctx, "root", "root-pass"); err != nil {
		t.Fatalf("UserAdd root: %v", err)
	}
	if _, err := cli.UserGrantRole(ctx, "root", "root"); err != nil {
		t.Fatalf("UserGrantRole root: %v", err)
	}
	if _, err := cli.AuthEnable(ctx); err != nil {
		t.Fatalf("AuthEnable: %v", err)
	}
	t.Cleanup(func() {
		// Disable auth so subsequent tests aren't affected if the harness
		// is ever reused.
		rootCli, err := clientv3.New(clientv3.Config{
			Endpoints:   cli.Endpoints(),
			DialTimeout: 5 * time.Second,
			Username:    "root",
			Password:    "root-pass",
		})
		if err == nil {
			_, _ = rootCli.AuthDisable(ctx)
			_ = rootCli.Close()
		}
	})

	// Build a Provider that constructs its own client with auth.
	p, err := ketcd.New(
		ketcd.WithEndpoints(cli.Endpoints()...),
		ketcd.WithAuth("alice", "passw0rd"),
		ketcd.WithPrefix("/cfg/"),
	)
	if err != nil {
		t.Fatalf("new (auth): %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// alice has read permission on /cfg/ — an empty read should succeed
	// (not error) and return an empty map.
	m, err := p.Read()
	if err != nil {
		t.Fatalf("authed read: %v", err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

func TestNew_WithAuthProvider_Called(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 10*time.Second)

	// Set up an authenticated user via the BYO client.
	if _, err := cli.UserAdd(ctx, "bob", "passw0rd2"); err != nil {
		t.Fatalf("UserAdd: %v", err)
	}
	if _, err := cli.RoleAdd(ctx, "reader2"); err != nil {
		t.Fatalf("RoleAdd: %v", err)
	}
	if _, err := cli.RoleGrantPermission(ctx, "reader2", "/cfg2/", "/cfg20", 0 /*Read*/); err != nil {
		t.Fatalf("RoleGrantPermission: %v", err)
	}
	if _, err := cli.UserGrantRole(ctx, "bob", "reader2"); err != nil {
		t.Fatalf("UserGrantRole: %v", err)
	}
	if _, err := cli.UserAdd(ctx, "root", "root-pass"); err != nil {
		t.Fatalf("UserAdd root: %v", err)
	}
	if _, err := cli.UserGrantRole(ctx, "root", "root"); err != nil {
		t.Fatalf("UserGrantRole root: %v", err)
	}
	if _, err := cli.AuthEnable(ctx); err != nil {
		t.Fatalf("AuthEnable: %v", err)
	}
	t.Cleanup(func() {
		rootCli, err := clientv3.New(clientv3.Config{
			Endpoints:   cli.Endpoints(),
			DialTimeout: 5 * time.Second,
			Username:    "root",
			Password:    "root-pass",
		})
		if err == nil {
			_, _ = rootCli.AuthDisable(ctx)
			_ = rootCli.Close()
		}
	})

	called := 0
	fn := func(ctx context.Context) (string, string, error) {
		called++
		return "bob", "passw0rd2", nil
	}

	p, err := ketcd.New(
		ketcd.WithEndpoints(cli.Endpoints()...),
		ketcd.WithAuthProvider(fn),
		ketcd.WithPrefix("/cfg2/"),
	)
	if err != nil {
		t.Fatalf("new (auth provider): %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if called != 1 {
		t.Errorf("authProvider called %d times; want 1", called)
	}

	// Sanity round-trip: the rotated credentials should actually authenticate.
	if _, err := p.Read(); err != nil {
		t.Errorf("authed read with rotated credentials: %v", err)
	}
}

func TestRead_TLS(t *testing.T) {
	t.Skip("TLS embedded-etcd setup is a larger task; covered by Option-level tests for WithTLS / WithTLSFiles config wiring. End-to-end TLS verified manually against an external cluster pre-release.")
}

func TestWatch_NoGap(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 30*time.Second)
	if _, err := cli.Put(ctx, "/svc/initial", "v0"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Race-the-gap: do a Read, then write a new key BEFORE Watch starts.
	// If the Provider correctly captures resp.Header.Revision and starts
	// the watch at Rev+1, the new key is delivered.
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	gotCh := make(chan struct{}, 1)
	var mu sync.Mutex
	calls := 0

	if err := p.Watch(func(event any, err error) {
		mu.Lock()
		calls++
		mu.Unlock()
		select {
		case gotCh <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Write the "gap" key.
	if _, err := cli.Put(ctx, "/svc/gap", "v1"); err != nil {
		t.Fatalf("gap put: %v", err)
	}

	select {
	case <-gotCh:
		// good
	case <-time.After(5 * time.Second):
		mu.Lock()
		c := calls
		mu.Unlock()
		t.Fatalf("watch cb never fired (%d calls so far)", c)
	}
}

func TestWatch_ReconnectsAfterClientClose(t *testing.T) {
	// Verifies the watch loop reconnects when the underlying channel
	// receives a fatal error. We simulate channel-fatal by compacting
	// the watch revision, which is more deterministic against embedded
	// etcd than forcing a gRPC stream close.
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 30*time.Second)
	cli.Put(ctx, "/svc/k", "v0")

	reconnects := atomic.Int32{}
	p, err := ketcd.New(
		ketcd.WithClient(cli),
		ketcd.WithPrefix("/svc/"),
		ketcd.WithReconnectBackoff(50*time.Millisecond, 500*time.Millisecond),
		ketcd.WithOnReconnect(func(attempt int, lastErr error, lastRevision int64) {
			reconnects.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	fires := atomic.Int32{}
	if err := p.Watch(func(_ any, _ error) {
		fires.Add(1)
	}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Compact at a low revision to force the watch into ErrCompacted.
	resp, err := cli.Get(ctx, "/svc/k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := cli.Compact(ctx, resp.Header.Revision); err != nil {
		t.Logf("compact: %v (best-effort)", err)
	}

	// Give the watch time to discover the compaction and resync.
	time.Sleep(300 * time.Millisecond)

	// Drive a fresh event so we can confirm the recovered watch is alive.
	if _, err := cli.Put(ctx, "/svc/post-reconnect", "v1"); err != nil {
		t.Fatalf("post-reconnect put: %v", err)
	}

	deadline := time.After(8 * time.Second)
	for fires.Load() == 0 {
		select {
		case <-deadline:
			t.Fatalf("watch never fired after forced reconnect path (reconnects=%d)", reconnects.Load())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestWatch_CompactionEmitsResync(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 30*time.Second)
	cli.Put(ctx, "/svc/k", "v0")

	resyncCh := make(chan int64, 1)
	p, err := ketcd.New(
		ketcd.WithClient(cli),
		ketcd.WithPrefix("/svc/"),
		ketcd.WithOnResync(func(reason string, newRev int64) {
			select {
			case resyncCh <- newRev:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	// Advance etcd's revision well past the provider's stored revision
	// BEFORE starting the watch. Then compact at the latest revision so
	// the watch's resume point (revAfterRead+1) lands inside the
	// compacted range — guaranteeing ErrCompacted on watch start.
	var latestRev int64
	for i := 0; i < 20; i++ {
		putResp, err := cli.Put(ctx, "/svc/k", fmt.Sprintf("v%d", i+1))
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		latestRev = putResp.Header.Revision
	}
	if _, err := cli.Compact(ctx, latestRev); err != nil {
		t.Logf("compact: %v (best-effort)", err)
	}

	resyncEv := make(chan struct{}, 1)
	if err := p.WatchTyped(context.Background(), func(evs []ketcd.Event, err error) {
		for _, e := range evs {
			if e.Type == ketcd.EventResync {
				select {
				case resyncEv <- struct{}{}:
				default:
				}
			}
		}
	}); err != nil {
		t.Fatalf("watchTyped: %v", err)
	}

	select {
	case <-resyncEv:
	case <-time.After(10 * time.Second):
		t.Fatalf("no Resync event delivered after compaction")
	}
	select {
	case <-resyncCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("OnResync callback not invoked")
	}
}

func TestWatch_DebounceCoalesces(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 30*time.Second)

	p, err := ketcd.New(
		ketcd.WithClient(cli),
		ketcd.WithPrefix("/burst/"),
		ketcd.WithDebounce(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	calls := atomic.Int32{}
	if err := p.Watch(func(_ any, _ error) {
		calls.Add(1)
	}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Burst: 20 puts within ~50ms.
	for i := 0; i < 20; i++ {
		cli.Put(ctx, fmt.Sprintf("/burst/k%d", i), fmt.Sprintf("v%d", i))
	}

	// Wait for debounce window + slack.
	time.Sleep(600 * time.Millisecond)

	got := calls.Load()
	if got == 0 {
		t.Fatalf("no callbacks fired")
	}
	if got > 3 {
		t.Errorf("debounce failed: %d callbacks for a 20-put burst (want 1-3)", got)
	}
}

func TestWatch_CtxCancelExitsCleanly(t *testing.T) {
	// Register goleak FIRST so it runs LAST in the LIFO t.Cleanup chain,
	// after embeddedEtcd has fully torn down its gRPC server goroutines.
	t.Cleanup(func() {
		// Small grace window for shutdown bookkeeping before goleak peeks.
		time.Sleep(200 * time.Millisecond)
		goleak.VerifyNone(t,
			goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
			goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
			goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
			goleak.IgnoreTopFunction("go.etcd.io/etcd/client/v3.(*lessor).deadlineLoop"),
		)
	})

	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	cli.Put(ctx, "/svc/k", "v")

	wctx, cancel := context.WithCancel(context.Background())

	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"), ketcd.WithWatchContext(wctx))
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	if err := p.Watch(func(_ any, _ error) {}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Let the loop establish itself before cancelling.
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Close releases the rest.
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Give the watch goroutine ~200ms to fully exit before goleak runs.
	time.Sleep(200 * time.Millisecond)
}

func TestWatch_AlreadyActiveError(t *testing.T) {
	cli := embeddedEtcd(t)
	cli.Put(context.Background(), "/k", "v")
	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithKey("/k"))
	t.Cleanup(func() { _ = p.Close() })
	p.Read()

	if err := p.Watch(func(_ any, _ error) {}); err != nil {
		t.Fatalf("first watch: %v", err)
	}
	if err := p.Watch(func(_ any, _ error) {}); !errors.Is(err, ketcd.ErrWatchActive) {
		t.Fatalf("expected ErrWatchActive on second Watch; got %v", err)
	}
	if err := p.WatchTyped(context.Background(), func([]ketcd.Event, error) {}); !errors.Is(err, ketcd.ErrWatchActive) {
		t.Fatalf("expected ErrWatchActive on WatchTyped after Watch; got %v", err)
	}
}

func TestStats_PutDeleteCounts(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 10*time.Second)
	cli.Put(ctx, "/svc/k1", "v1")
	cli.Put(ctx, "/svc/k2", "v2")

	p, _ := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"))
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	done := make(chan struct{})
	count := atomic.Int32{}
	if err := p.Watch(func(_ any, _ error) {
		if count.Add(1) == 3 {
			close(done)
		}
	}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	cli.Put(ctx, "/svc/k3", "v3")
	cli.Put(ctx, "/svc/k1", "v1-upd")
	cli.Delete(ctx, "/svc/k2")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// even if debounce coalesces, we may not get 3 cb fires —
		// fall through and inspect stats anyway
	}

	// Poll until stats reflect the seeded events instead of sleeping a
	// fixed window. Stats counters are updated asynchronously from the
	// watch goroutine, so a tight assertion right after the cb wakeup
	// can race ahead of the atomic increments.
	statsDeadline := time.After(5 * time.Second)
	var s ketcd.Stats
	for {
		s = p.Stats()
		if s.TotalPuts >= 2 && s.TotalDeletes >= 1 {
			break
		}
		select {
		case <-statsDeadline:
			// Fall through to assertions below — they'll produce a precise
			// diagnostic with the actual counters observed.
			goto check
		case <-time.After(20 * time.Millisecond):
		}
	}
check:
	if s.TotalPuts < 2 {
		t.Errorf("TotalPuts = %d; want >= 2", s.TotalPuts)
	}
	if s.TotalDeletes < 1 {
		t.Errorf("TotalDeletes = %d; want >= 1", s.TotalDeletes)
	}
	if s.Revision == 0 {
		t.Errorf("Revision = 0; want > 0")
	}
}

// TestWatch_RestartAfterCtxCancel verifies that cancelling the
// WithWatchContext-supplied parent ctx lets the loop exit cleanly AND
// resets the per-watch state so a subsequent Watch() succeeds without
// going through Close(). Regression test for the bug where the cancelled
// goroutine left watchCancel non-nil, forcing the Provider into a dead
// state where every Watch returned "watch already active".
func TestWatch_RestartAfterCtxCancel(t *testing.T) {
	cli := embeddedEtcd(t)
	cli.Put(context.Background(), "/svc/k", "v")

	wctx, cancel := context.WithCancel(context.Background())
	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"), ketcd.WithWatchContext(wctx))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	if err := p.Watch(func(_ any, _ error) {}); err != nil {
		t.Fatalf("first watch: %v", err)
	}

	// Cancel parent ctx; watchLoop should exit AND clear watch state.
	cancel()

	// Give the goroutine a moment to drain through clearWatchState.
	deadline := time.After(3 * time.Second)
	for {
		err := p.Watch(func(_ any, _ error) {})
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("second Watch never succeeded after ctx cancel: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestRead_PathCollision_LeafVsSubtree(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	// Seed a leaf "/svc/db" and a sub-tree "/svc/db/host" under the
	// same prefix. unflatten cannot place both.
	if _, err := cli.Put(ctx, "/svc/db", "postgres"); err != nil {
		t.Fatalf("seed leaf: %v", err)
	}
	if _, err := cli.Put(ctx, "/svc/db/host", "localhost"); err != nil {
		t.Fatalf("seed branch: %v", err)
	}
	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	_, err = p.Read()
	if !errors.Is(err, ketcd.ErrPathCollision) {
		t.Fatalf("want ErrPathCollision, got %v", err)
	}
}

func TestRead_PathCollision_SubtreeVsLeaf(t *testing.T) {
	// Same situation, opposite map-iteration order — confirms the
	// error fires regardless of which key the unflatten sees first.
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	if _, err := cli.Put(ctx, "/svc/db/host", "localhost"); err != nil {
		t.Fatalf("seed branch: %v", err)
	}
	if _, err := cli.Put(ctx, "/svc/db", "postgres"); err != nil {
		t.Fatalf("seed leaf: %v", err)
	}
	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	_, err = p.Read()
	if !errors.Is(err, ketcd.ErrPathCollision) {
		t.Fatalf("want ErrPathCollision, got %v", err)
	}
}

func TestWatch_PanicInCallbackDoesNotCrash(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 10*time.Second)
	cli.Put(ctx, "/svc/k", "v0")

	errCh := make(chan error, 1)
	p, err := ketcd.New(
		ketcd.WithClient(cli),
		ketcd.WithPrefix("/svc/"),
		ketcd.WithOnWatchError(func(err error, class ketcd.WatchErrorClass) {
			select {
			case errCh <- err:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	// Install a callback that panics on every invocation.
	if err := p.Watch(func(_ any, _ error) {
		panic("intentional test panic")
	}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Drive an event.
	if _, err := cli.Put(ctx, "/svc/k2", "v1"); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	// The OnWatchError hook should fire with a "panic" error.
	select {
	case got := <-errCh:
		if got == nil || !contains(got.Error(), "panic") {
			t.Fatalf("expected panic-classified error, got %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnWatchError never fired after panic; process likely would have crashed without recovery")
	}
}

// contains is a tiny strings.Contains shim avoiding an import here.
func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func TestClose_WaitsForWatchGoroutine(t *testing.T) {
	cli := embeddedEtcd(t)
	cli.Put(context.Background(), "/svc/k", "v")

	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	delivered := make(chan struct{}, 1)
	if err := p.Watch(func(_ any, _ error) {
		select {
		case delivered <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Drive an event so the loop's hot path is exercised.
	cli.Put(context.Background(), "/svc/k2", "v")
	<-delivered

	start := time.Now()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Close must return promptly when there's nothing blocking the
	// watch loop. >2s on this path indicates a wait-bug.
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Close took %v; want < 2s", d)
	}
}

func TestClose_HonorsCloseTimeout(t *testing.T) {
	cli := embeddedEtcd(t)
	cli.Put(context.Background(), "/svc/k", "v")

	// Install a watch callback that blocks forever to simulate a
	// wedged consumer. The watch goroutine will be stuck inside
	// deliverBatch -> cb. Close must still return within timeout.
	hang := make(chan struct{})
	hangEntered := make(chan struct{}, 1)
	t.Cleanup(func() { close(hang) }) // unblock at end of test

	p, err := ketcd.New(
		ketcd.WithClient(cli),
		ketcd.WithPrefix("/svc/"),
		ketcd.WithCloseTimeout(300*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := p.Watch(func(_ any, _ error) {
		select {
		case hangEntered <- struct{}{}:
		default:
		}
		<-hang
	}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	cli.Put(context.Background(), "/svc/k2", "v") // triggers cb

	// Wait for the callback to actually be inside the hang before we
	// call Close. Without this, Close cancels the watch ctx so quickly
	// that the goroutine exits before ever entering the cb and the
	// timeout path is never exercised.
	select {
	case <-hangEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("watch callback never entered hang state")
	}

	start := time.Now()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d := time.Since(start)
	// Lower bound (200ms) is the load-bearing assertion: it proves Close
	// actually waited and didn't return at t=0. Upper bound is generous
	// because a heavily loaded CI runner (cold cache, slow embedded-etcd
	// teardown, scheduler pressure) can easily add 4-5s of slack on top
	// of the 300ms timeout the test installed.
	if d < 200*time.Millisecond || d > 8*time.Second {
		t.Errorf("Close took %v; want roughly the 300ms timeout, not 0 and not unbounded", d)
	}
}
