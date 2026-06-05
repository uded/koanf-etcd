package etcd

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestErrors_AreDistinctSentinels(t *testing.T) {
	cases := []error{
		ErrUseParser,
		ErrEmptyPrefix,
		ErrKeyNotFound,
		ErrWatchActive,
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

func TestBuildClient_DefaultsAutoSyncTo30s(t *testing.T) {
	// autoSync's default is applied inside buildClient, not in newSettings,
	// so the value-equality check in hasNonDefaultClientConfig keeps
	// working. Verify the build-time default is what we expect.
	if got := defaultIfZero(0, 30*time.Second); got != 30*time.Second {
		t.Errorf("defaultIfZero(0, 30s) = %v; want 30s", got)
	}
	if got := defaultIfZero(7*time.Second, 30*time.Second); got != 7*time.Second {
		t.Errorf("defaultIfZero(7s, 30s) = %v; want 7s (caller-supplied wins)", got)
	}
}

func TestSplitCleanPath_StripsLeadingAndTrailingEmpties(t *testing.T) {
	cases := []struct {
		in    string
		delim string
		want  []string
	}{
		{"app.db.", ".", []string{"app", "db"}},
		{"app.db..", ".", []string{"app", "db"}},
		{".app.db.", ".", []string{"app", "db"}},
		{"app.db", ".", []string{"app", "db"}},
		{"", ".", nil},
		{"...", ".", nil},
		{"a/b/", "/", []string{"a", "b"}},
	}
	for _, c := range cases {
		got := splitCleanPath(c.in, c.delim)
		if len(got) != len(c.want) {
			t.Errorf("splitCleanPath(%q, %q) = %v; want %v", c.in, c.delim, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("splitCleanPath(%q, %q)[%d] = %q; want %q", c.in, c.delim, i, got[i], c.want[i])
			}
		}
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
		WithOnReconnect(func(int, error, int64) {}),
		WithOnResync(func(string, int64) {}),
		WithOnWatchError(func(error, WatchErrorClass) {}),
	} {
		if err := opt(s); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	filterOK := s.filterSet && s.wantPut && !s.wantDelete
	callbacksSet := s.onReconnect != nil && s.onResync != nil && s.onWatchError != nil

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

// TestLoadTLSFromFiles_HardensConfig verifies that loadTLSFromFiles pins
// MinVersion to TLS 1.2 even when called with no cert/key/CA files. This
// avoids pulling the TLS-fixture generator (and its embedded etcd
// neighbour) into the main module's test dep graph.
func TestLoadTLSFromFiles_HardensConfig(t *testing.T) {
	cfg, err := loadTLSFromFiles("", "", "")
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
	if !s.tlsSet {
		t.Errorf("WithTLSServerName must flip tlsSet for BYO-conflict detection")
	}
}

// TestNew_BYOPlusDefaultEqualValueStillConflicts is the regression test
// that motivated the wasSet refactor. The earlier value-equality check
// would silently accept WithClient(...) + WithDialTimeout(5*time.Second)
// because 5s matched the default. The wasSet flag flips on regardless
// of value, so the conflict must be detected.
func TestNew_BYOPlusDefaultEqualValueStillConflicts(t *testing.T) {
	cli := newFakeClient()
	_, err := New(WithClient(cli), WithKey("/x"), WithDialTimeout(5*time.Second))
	if !errors.Is(err, ErrOptionConflict) {
		t.Fatalf("expected ErrOptionConflict for BYO + WithDialTimeout(5s); got %v", err)
	}
}

// newFakeClient returns a non-nil *clientv3.Client that is never
// actually dialed. New() only assigns the pointer when WithClient is
// supplied — it does not ping or otherwise touch the client — so an
// empty struct is sufficient for BYO-conflict tests that fail before
// any RPC could happen.
func newFakeClient() *clientv3.Client {
	return &clientv3.Client{}
}

// TestStats_NewFieldsSnapshotZeroByDefault verifies that the extended
// Stats fields all default to their zero value when no counter has
// ever fired. Guards against a future field landing without snapshot()
// wiring.
func TestStats_NewFieldsSnapshotZeroByDefault(t *testing.T) {
	a := &atomicStats{}
	s := a.snapshot()
	if s.TotalWatchErrors != 0 || s.TotalEventsDelivered != 0 || s.TotalReads != 0 ||
		s.TotalDebounceFlushes != 0 || s.LastFlushCoalescedCount != 0 ||
		!s.LastWatchErrorAt.IsZero() {
		t.Errorf("zero-value snapshot leaks non-zero stats: %+v", s)
	}
}

// TestStats_NewFieldsRoundTrip verifies that every new atomic counter
// surfaces through snapshot() with the value it was loaded with.
func TestStats_NewFieldsRoundTrip(t *testing.T) {
	a := &atomicStats{}
	a.totalWatchErrors.Add(5)
	a.totalEventsDelivered.Add(7)
	a.totalReads.Add(11)
	a.totalDebounceFlushes.Add(13)
	a.lastFlushCoalesced.Store(17)
	a.lastWatchErrorUnix.Store(time.Now().UnixNano())
	s := a.snapshot()
	if s.TotalWatchErrors != 5 || s.TotalEventsDelivered != 7 || s.TotalReads != 11 ||
		s.TotalDebounceFlushes != 13 || s.LastFlushCoalescedCount != 17 ||
		s.LastWatchErrorAt.IsZero() {
		t.Errorf("snapshot mismatch: %+v", s)
	}
}

// TestClassifyWatchError_AuthIsAuth verifies that etcd permission
// errors land in the WatchErrorAuth bucket — important because the
// watch loop does NOT retry on auth failures.
func TestClassifyWatchError_AuthIsAuth(t *testing.T) {
	got := classifyWatchError(rpctypes.ErrPermissionDenied)
	if got != WatchErrorAuth {
		t.Errorf("PermissionDenied class = %v; want WatchErrorAuth", got)
	}
}

// TestClassifyWatchError_CompactionIsCompaction verifies the
// compaction signal is its own class so callers can distinguish a
// resync-recoverable error from a hard auth failure.
func TestClassifyWatchError_CompactionIsCompaction(t *testing.T) {
	got := classifyWatchError(rpctypes.ErrCompacted)
	if got != WatchErrorCompaction {
		t.Errorf("ErrCompacted class = %v; want WatchErrorCompaction", got)
	}
}

// TestClassifyWatchError_OtherIsTransient verifies that any error not
// recognized by isFatalRPCError or compaction-detection lands in the
// transient bucket — the watch loop will reconnect with backoff.
func TestClassifyWatchError_OtherIsTransient(t *testing.T) {
	got := classifyWatchError(fmt.Errorf("network glitch"))
	if got != WatchErrorTransient {
		t.Errorf("generic err class = %v; want WatchErrorTransient", got)
	}
}
