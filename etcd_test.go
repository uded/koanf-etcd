package etcd

import (
	"errors"
	"testing"
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
