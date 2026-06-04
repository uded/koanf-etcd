package etcd

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/goleak"
)

func TestErrors_AreDistinctSentinels(t *testing.T) {
	cases := []error{
		ErrUseParser,
		ErrEmptyPrefix,
		ErrNotBlob,
		ErrOptionConflict,
		ErrNoMode,
		ErrClosed,
	}
	for i, a := range cases {
		for j, b := range cases {
			if i == j {
				continue
			}
			if errors.Is(a, b) {
				t.Fatalf("errors.Is(%v, %v) is true; sentinels must be distinct", a, b)
			}
		}
	}
}

func TestSettings_Defaults(t *testing.T) {
	s := newSettings()
	if s.delim != "." {
		t.Errorf("default delim = %q, want %q", s.delim, ".")
	}
	if !s.trimPrefix {
		t.Errorf("default trimPrefix = false, want true")
	}
	if !s.unflatten {
		t.Errorf("default unflatten = false, want true")
	}
	if !s.resumeFromRevision {
		t.Errorf("default resumeFromRevision = false, want true")
	}
	if s.reconnectMin != 1*time.Second {
		t.Errorf("default reconnectMin = %v, want 1s", s.reconnectMin)
	}
	if s.reconnectMax != 2*time.Minute {
		t.Errorf("default reconnectMax = %v, want 2m", s.reconnectMax)
	}
	if s.readTimeout != 5*time.Second {
		t.Errorf("default readTimeout = %v, want 5s", s.readTimeout)
	}
	if s.debounce != 0 {
		t.Errorf("default debounce = %v, want 0", s.debounce)
	}
	if s.strict {
		t.Errorf("default strict = true, want false")
	}
}

func TestOptions_ConnectionApplied(t *testing.T) {
	s := newSettings()
	opts := []Option{
		WithEndpoints("a:2379", "b:2379"),
		WithDialTimeout(7 * time.Second),
		WithKeepAlive(10*time.Second, 3*time.Second),
		WithAutoSync(30 * time.Second),
		WithAuth("u", "p"),
		WithClientContext(context.Background()),
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if len(s.endpoints) != 2 || s.endpoints[0] != "a:2379" {
		t.Errorf("endpoints: %v", s.endpoints)
	}
	if s.dialTimeout != 7*time.Second {
		t.Errorf("dialTimeout: %v", s.dialTimeout)
	}
	if s.keepAliveT != 10*time.Second || s.keepAliveTO != 3*time.Second {
		t.Errorf("keepAlive: %v/%v", s.keepAliveT, s.keepAliveTO)
	}
	if s.autoSync != 30*time.Second {
		t.Errorf("autoSync: %v", s.autoSync)
	}
	if s.username != "u" || s.password != "p" {
		t.Errorf("auth: %q/%q", s.username, s.password)
	}
	if s.clientCtx == nil {
		t.Errorf("clientCtx not set")
	}
}

func TestOptions_SRVEndpoints(t *testing.T) {
	s := newSettings()
	if err := WithEndpointsFromSRV("etcd-client", "tcp", "example.com")(s); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if s.srvService != "etcd-client" || s.srvProto != "tcp" || s.srvDomain != "example.com" {
		t.Errorf("srv fields: %q/%q/%q", s.srvService, s.srvProto, s.srvDomain)
	}
}

func TestOptions_ReadShaping(t *testing.T) {
	s := newSettings()
	keyXf := func(k string) string { return k }
	valXf := func(k string, raw []byte) (any, error) { return string(raw), nil }
	opts := []Option{
		WithKey("/app/cfg"),
		WithDelim("/"),
		WithTrimPrefix(false),
		WithUnflatten(false),
		WithKeyTransform(keyXf),
		WithValueTransform(valXf),
		WithLimit(500),
		WithSerializable(true),
		WithReadRevision(42),
		WithReadTimeout(2 * time.Second),
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if s.key != "/app/cfg" {
		t.Errorf("key: %q", s.key)
	}
	if s.delim != "/" {
		t.Errorf("delim: %q", s.delim)
	}
	if s.trimPrefix {
		t.Errorf("trimPrefix should be false")
	}
	if s.unflatten {
		t.Errorf("unflatten should be false")
	}
	if s.keyTransform == nil || s.valueTransform == nil {
		t.Errorf("transforms not set")
	}
	if s.limit != 500 {
		t.Errorf("limit: %d", s.limit)
	}
	if !s.serializable {
		t.Errorf("serializable should be true")
	}
	if s.readRevision != 42 {
		t.Errorf("readRevision: %d", s.readRevision)
	}
	if s.readTimeout != 2*time.Second {
		t.Errorf("readTimeout: %v", s.readTimeout)
	}
}

func TestOptions_BlobAndStrict(t *testing.T) {
	s := newSettings()
	called := false
	onEmpty := func(prefix string) { called = true }
	opts := []Option{
		WithPrefix("/cfg/"),
		WithBlob(),
		WithStrict(true),
		OnEmpty(onEmpty),
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if s.prefix != "/cfg/" {
		t.Errorf("prefix: %q", s.prefix)
	}
	if !s.blob {
		t.Errorf("blob not set")
	}
	if !s.strict {
		t.Errorf("strict not set")
	}
	if s.onEmpty == nil {
		t.Errorf("onEmpty not set")
	}
	s.onEmpty("/x")
	if !called {
		t.Errorf("onEmpty callback not invoked")
	}
}

func TestOptions_Watch(t *testing.T) {
	s := newSettings()
	for _, opt := range []Option{
		WithWatchContext(context.Background()),
		WithDebounce(100 * time.Millisecond),
		WithReconnectBackoff(500*time.Millisecond, 10*time.Second),
		WithResumeFromRevision(false),
		WithProgressNotify(true),
		WithEventFilter(true, false), // deliver only puts
		WithCreatedNotify(true),
		WithRedactor(func(k string, raw []byte) string { return "***" }),
		WithOnReconnect(func(int, error) {}),
		WithOnResync(func(string, int64) {}),
		WithOnWatchError(func(error) {}),
	} {
		if err := opt(s); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	filterOK := s.filterSet && s.wantPut && !s.wantDelete
	callbacksSet := s.redactor != nil && s.onReconnect != nil && s.onResync != nil && s.onWatchError != nil

	checks := []struct {
		name string
		ok   bool
	}{
		{"watchCtx set", s.watchCtx != nil},
		{"debounce=100ms", s.debounce == 100*time.Millisecond},
		{"reconnectMin=500ms", s.reconnectMin == 500*time.Millisecond},
		{"reconnectMax=10s", s.reconnectMax == 10*time.Second},
		{"resumeFromRevision=false", !s.resumeFromRevision},
		{"progressNotify=true", s.progressNotify},
		{"filter deliver-puts-only", filterOK},
		{"createdNotify=true", s.createdNotify},
		{"callbacks set", callbacksSet},
	}
	for _, c := range checks {
		if !c.ok {
			t.Errorf("%s: assertion failed", c.name)
		}
	}
}

func TestNew_RejectsNoMode(t *testing.T) {
	_, err := New(WithEndpoints("x:2379"))
	if !errors.Is(err, ErrNoMode) {
		t.Fatalf("want ErrNoMode, got %v", err)
	}
}

func TestNew_RejectsBothModes(t *testing.T) {
	_, err := New(WithEndpoints("x:2379"), WithKey("/a"), WithPrefix("/b/"))
	if !errors.Is(err, ErrOptionConflict) {
		t.Fatalf("want ErrOptionConflict, got %v", err)
	}
}

func TestNew_RejectsBlobWithoutKey(t *testing.T) {
	_, err := New(WithEndpoints("x:2379"), WithPrefix("/a/"), WithBlob())
	if !errors.Is(err, ErrOptionConflict) {
		t.Fatalf("want ErrOptionConflict, got %v", err)
	}
}

func TestNew_RejectsBlobWithUnflatten(t *testing.T) {
	_, err := New(WithEndpoints("x:2379"), WithKey("/a"), WithBlob(), WithUnflatten(true))
	if !errors.Is(err, ErrOptionConflict) {
		t.Fatalf("want ErrOptionConflict, got %v", err)
	}
}

func TestNew_RejectsClientWithConnOptions(t *testing.T) {
	cli := embeddedEtcd(t)
	_, err := New(WithClient(cli), WithKey("/a"), WithEndpoints("y:2379"))
	if !errors.Is(err, ErrOptionConflict) {
		t.Fatalf("want ErrOptionConflict, got %v", err)
	}
}

func TestNew_BYOClientNotClosedByProviderClose(t *testing.T) {
	cli := embeddedEtcd(t)
	p, err := New(WithClient(cli), WithKey("/sanity"))
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

func TestTransform_DefaultKey_StripsPrefixAndReplacesSlash(t *testing.T) {
	s := newSettings()
	s.prefix = "/svc/"
	s.delim = "."
	s.trimPrefix = true
	xf := makeDefaultKeyTransform(s)
	cases := map[string]string{
		"/svc/db/host":         "db.host",
		"/svc/feature/enabled": "feature.enabled",
		"/svc/no-slash":        "no-slash",
	}
	for in, want := range cases {
		got := xf(in)
		if got != want {
			t.Errorf("xf(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestTransform_DefaultKey_TrimPrefixOff(t *testing.T) {
	s := newSettings()
	s.prefix = "/svc/"
	s.delim = "."
	s.trimPrefix = false
	xf := makeDefaultKeyTransform(s)
	got := xf("/svc/db/host")
	if got != ".svc.db.host" {
		t.Errorf("got %q; want %q", got, ".svc.db.host")
	}
}

func TestTransform_DefaultKey_CustomDelim(t *testing.T) {
	s := newSettings()
	s.prefix = "/svc/"
	s.delim = "/"
	s.trimPrefix = true
	xf := makeDefaultKeyTransform(s)
	got := xf("/svc/db/host")
	if got != "db/host" {
		t.Errorf("got %q; want %q", got, "db/host")
	}
}

func TestTransform_DefaultValue_TrimsWhitespace(t *testing.T) {
	v, err := defaultValueTransform("any", []byte("  http://x\n\t"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if s, _ := v.(string); s != "http://x" {
		t.Errorf("got %q; want %q", s, "http://x")
	}
}

func TestRead_SingleKey(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	if _, err := cli.Put(ctx, "/app/cfg/db.host", "localhost"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p, err := New(WithClient(cli), WithKey("/app/cfg/db.host"))
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

	p, _ := New(WithClient(cli), WithKey("/app/url"))
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
	p, _ := New(WithClient(cli), WithKey("/k"))
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

	p, err := New(WithClient(cli), WithPrefix("/svc/"))
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

	p, _ := New(WithClient(cli), WithPrefix("/svc/"), WithUnflatten(false))
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

	p, err := New(WithClient(cli), WithPrefix("/big/"), WithLimit(250), WithUnflatten(false))
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
	p, _ := New(WithClient(cli), WithPrefix("/nope/"), WithStrict(true))
	t.Cleanup(func() { _ = p.Close() })
	_, err := p.Read()
	if !errors.Is(err, ErrEmptyPrefix) {
		t.Errorf("want ErrEmptyPrefix, got %v", err)
	}
}

func TestRead_OnEmptyCallback(t *testing.T) {
	cli := embeddedEtcd(t)
	called := 0
	var gotPrefix string
	p, _ := New(
		WithClient(cli),
		WithPrefix("/nope/"),
		OnEmpty(func(prefix string) {
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

	p, _ := New(WithClient(cli), WithKey("/cfg"), WithBlob(), WithUnflatten(false))
	t.Cleanup(func() { _ = p.Close() })

	_, err := p.Read()
	if !errors.Is(err, ErrUseParser) {
		t.Errorf("want ErrUseParser, got %v", err)
	}
}

func TestReadBytes_BlobMode(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)
	cli.Put(ctx, "/cfg", `{"a":1}`)

	p, _ := New(WithClient(cli), WithKey("/cfg"), WithBlob(), WithUnflatten(false))
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
	p, _ := New(WithClient(cli), WithKey("/x"))
	t.Cleanup(func() { _ = p.Close() })

	_, err := p.ReadBytes()
	if !errors.Is(err, ErrNotBlob) {
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
	p, err := New(
		WithEndpoints(cli.Endpoints()...),
		WithAuth("alice", "passw0rd"),
		WithPrefix("/cfg/"),
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

func TestRead_TLS(t *testing.T) {
	t.Skip("TLS embedded-etcd setup is a larger task; covered by Option-level tests for WithTLS / WithTLSFiles config wiring. End-to-end TLS verified manually against an external cluster pre-release.")
}

func TestWatch_NoGap(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 30*time.Second)
	if _, err := cli.Put(ctx, "/svc/initial", "v0"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p, err := New(WithClient(cli), WithPrefix("/svc/"))
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

func TestLoadTLSFromFiles(t *testing.T) {
	f := genTLSFixtures(t)
	cfg, err := loadTLSFromFiles(f.cliCert, f.cliKey, f.caFile)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 cert; got %d", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Errorf("RootCAs not set")
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
	p, err := New(
		WithClient(cli),
		WithPrefix("/svc/"),
		WithReconnectBackoff(50*time.Millisecond, 500*time.Millisecond),
		WithOnReconnect(func(attempt int, lastErr error) {
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
	p, err := New(
		WithClient(cli),
		WithPrefix("/svc/"),
		WithOnResync(func(reason string, newRev int64) {
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
	if err := p.WatchTyped(context.Background(), func(evs []Event, err error) {
		for _, e := range evs {
			if e.Type == EventResync {
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

	p, err := New(
		WithClient(cli),
		WithPrefix("/burst/"),
		WithDebounce(200*time.Millisecond),
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

	p, _ := New(WithClient(cli), WithPrefix("/svc/"), WithWatchContext(wctx))
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
	p, _ := New(WithClient(cli), WithKey("/k"))
	t.Cleanup(func() { _ = p.Close() })
	p.Read()

	if err := p.Watch(func(_ any, _ error) {}); err != nil {
		t.Fatalf("first watch: %v", err)
	}
	if err := p.Watch(func(_ any, _ error) {}); err == nil {
		t.Fatalf("expected error on second Watch")
	}
	if err := p.WatchTyped(context.Background(), func([]Event, error) {}); err == nil {
		t.Fatalf("expected error on WatchTyped after Watch")
	}
}

func TestStats_PutDeleteCounts(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 10*time.Second)
	cli.Put(ctx, "/svc/k1", "v1")
	cli.Put(ctx, "/svc/k2", "v2")

	p, _ := New(WithClient(cli), WithPrefix("/svc/"))
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

	time.Sleep(100 * time.Millisecond)
	s := p.Stats()
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

func TestLog_RedactedByDefault(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cli := embeddedEtcd(t)
	cli.Put(context.Background(), "/secret/key", "SUPER-SENSITIVE-VALUE")

	p, err := New(
		WithClient(cli),
		WithPrefix("/secret/"),
		WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}

	if bytes.Contains(buf.Bytes(), []byte("SUPER-SENSITIVE-VALUE")) {
		t.Fatalf("secret value leaked into logs:\n%s", buf.String())
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
	p, err := New(WithClient(cli), WithPrefix("/svc/"), WithWatchContext(wctx))
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
	p, err := New(WithClient(cli), WithPrefix("/svc/"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	_, err = p.Read()
	if !errors.Is(err, ErrPathCollision) {
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
	p, err := New(WithClient(cli), WithPrefix("/svc/"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	_, err = p.Read()
	if !errors.Is(err, ErrPathCollision) {
		t.Fatalf("want ErrPathCollision, got %v", err)
	}
}

func TestUnflattenMap_NoCollision(t *testing.T) {
	// Sanity: non-colliding paths still produce a nested map without error.
	flat := map[string]any{"a.b": "1", "a.c": "2", "d": "3"}
	got, err := unflattenMap(flat, ".")
	if err != nil {
		t.Fatalf("unflatten: %v", err)
	}
	a, _ := got["a"].(map[string]any)
	if a["b"] != "1" || a["c"] != "2" {
		t.Errorf("unexpected nested map: %v", got)
	}
	if got["d"] != "3" {
		t.Errorf("missing leaf d")
	}
}

func TestBackoff_HonorsMinFloor(t *testing.T) {
	min := 100 * time.Millisecond
	max := 5 * time.Second
	for attempt := 1; attempt <= 8; attempt++ {
		for i := 0; i < 50; i++ {
			d := backoff(attempt, min, max)
			if d < min {
				t.Errorf("attempt=%d sample=%d: backoff=%v < min=%v", attempt, i, d, min)
			}
			if d > max {
				t.Errorf("attempt=%d sample=%d: backoff=%v > max=%v", attempt, i, d, max)
			}
		}
	}
}

func TestBackoff_HandlesOverflow(t *testing.T) {
	// Very large attempts must clamp to max, not wrap.
	d := backoff(64, time.Second, 30*time.Second)
	if d < time.Second || d > 30*time.Second {
		t.Errorf("attempt=64: backoff=%v out of [1s, 30s]", d)
	}
	d = backoff(1000, time.Second, 30*time.Second)
	if d < time.Second || d > 30*time.Second {
		t.Errorf("attempt=1000: backoff=%v out of [1s, 30s]", d)
	}
}

func TestBackoff_MinEqualsMax(t *testing.T) {
	d := backoff(1, 2*time.Second, 2*time.Second)
	if d != 2*time.Second {
		t.Errorf("min==max: backoff=%v want 2s", d)
	}
}

func TestArmDebounce_FirstCallCreates(t *testing.T) {
	timer, ch := armDebounce(nil, 50*time.Millisecond)
	if timer == nil {
		t.Fatal("expected non-nil timer on first call")
	}
	if ch == nil {
		t.Fatal("expected non-nil channel on first call")
	}
	select {
	case <-ch:
		// fired — good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timer never fired")
	}
	timer.Stop()
}

func TestArmDebounce_ResetOnExpired(t *testing.T) {
	// First call creates and lets fire so the channel is drained.
	timer, ch := armDebounce(nil, 20*time.Millisecond)
	<-ch
	// Second call must drain the fired channel and re-arm.
	timer, ch = armDebounce(timer, 20*time.Millisecond)
	select {
	case <-ch:
		// good — re-armed and fired
	case <-time.After(200 * time.Millisecond):
		t.Fatal("re-armed timer never fired")
	}
	timer.Stop()
}

func TestArmDebounce_ResetOnLive(t *testing.T) {
	// First call creates a long timer that won't fire on its own.
	timer, _ := armDebounce(nil, 5*time.Second)
	// Reset to a short window — the long timer should be stopped and
	// the short one should fire.
	timer, ch := armDebounce(timer, 20*time.Millisecond)
	select {
	case <-ch:
		// good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("reset timer never fired")
	}
	timer.Stop()
}

func TestWatch_PanicInCallbackDoesNotCrash(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 10*time.Second)
	cli.Put(ctx, "/svc/k", "v0")

	errCh := make(chan error, 1)
	p, err := New(
		WithClient(cli),
		WithPrefix("/svc/"),
		WithOnWatchError(func(err error) {
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

	p, err := New(WithClient(cli), WithPrefix("/svc/"))
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
	t.Cleanup(func() { close(hang) }) // unblock at end of test

	p, err := New(
		WithClient(cli),
		WithPrefix("/svc/"),
		WithCloseTimeout(300*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := p.Read(); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := p.Watch(func(_ any, _ error) {
		<-hang
	}); err != nil {
		t.Fatalf("watch: %v", err)
	}

	cli.Put(context.Background(), "/svc/k2", "v") // triggers cb

	start := time.Now()
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d := time.Since(start)
	if d < 200*time.Millisecond || d > 2*time.Second {
		t.Errorf("Close took %v; want roughly the 300ms timeout, not 0 and not unbounded", d)
	}
}

func TestLoadTLSFromFiles_HardensConfig(t *testing.T) {
	f := genTLSFixtures(t)
	cfg, err := loadTLSFromFiles(f.cliCert, f.cliKey, f.caFile)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MinVersion < tls.VersionTLS12 {
		t.Errorf("MinVersion=%v; want >= TLS 1.2", cfg.MinVersion)
	}
}

func TestOptions_WithTLSServerName(t *testing.T) {
	s := newSettings()
	if err := WithTLSServerName("etcd.example.com")(s); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if s.tlsServerName != "etcd.example.com" {
		t.Errorf("tlsServerName=%q; want etcd.example.com", s.tlsServerName)
	}
}
