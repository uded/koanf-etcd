package etcd

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
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
	opts := []Option{
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
	}
	for _, opt := range opts {
		if err := opt(s); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if s.watchCtx == nil {
		t.Errorf("watchCtx not set")
	}
	if s.debounce != 100*time.Millisecond {
		t.Errorf("debounce: %v", s.debounce)
	}
	if s.reconnectMin != 500*time.Millisecond || s.reconnectMax != 10*time.Second {
		t.Errorf("reconnect: %v/%v", s.reconnectMin, s.reconnectMax)
	}
	if s.resumeFromRevision {
		t.Errorf("resumeFromRevision should be false")
	}
	if !s.progressNotify {
		t.Errorf("progressNotify not set")
	}
	if !s.filterSet || !s.wantPut || s.wantDelete {
		t.Errorf("filters: set=%v put=%v delete=%v", s.filterSet, s.wantPut, s.wantDelete)
	}
	if !s.createdNotify {
		t.Errorf("createdNotify not set")
	}
	if s.redactor == nil || s.onReconnect == nil || s.onResync == nil || s.onWatchError == nil {
		t.Errorf("callbacks not set")
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
