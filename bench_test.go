package etcd

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkSplitPath measures the per-call cost of splitting a flat
// key path. This runs once per kv in a prefix Read.
func BenchmarkSplitPath(b *testing.B) {
	cases := []struct {
		name  string
		path  string
		delim string
	}{
		{"short_dot", "app.db.host", "."},
		{"deep_dot", "a.b.c.d.e.f.g.h.i.j", "."},
		{"short_slash", "app/db/host", "/"},
		{"with_leading_delim", ".app.db.host", "."},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = splitPath(c.path, c.delim)
			}
		})
	}
}

// BenchmarkUnflatten_100Keys measures the cost of converting a 100-key
// flat map into a nested map. Runs once per Read in tree mode.
func BenchmarkUnflatten_100Keys(b *testing.B) {
	flat := make(map[string]any, 100)
	for i := 0; i < 100; i++ {
		flat[fmt.Sprintf("svc.tier%d.host%d", i%5, i)] = "v"
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = unflattenMap(flat, ".")
	}
}

// BenchmarkBackoff measures the per-call cost of the jittered backoff
// computation. Runs once per reconnect attempt.
func BenchmarkBackoff(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = backoff(5, 1*time.Second, 2*time.Minute)
	}
}
