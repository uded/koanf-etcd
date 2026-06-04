package etcd

import (
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
