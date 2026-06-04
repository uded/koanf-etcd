package etcd

import (
	"context"
	"errors"
	"testing"
	"time"
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
