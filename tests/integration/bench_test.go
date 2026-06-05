package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	ketcd "github.com/uded/koanf-etcd"
)

// BenchmarkReadPrefix_1000Keys exercises the full prefix-read code path
// (etcd round trip + value/key transform + unflatten) against an
// embedded etcd with 1k keys.
func BenchmarkReadPrefix_1000Keys(b *testing.B) {
	cli := embeddedEtcdB(b)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < 1000; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("/svc/k%04d", i), "v"); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	p, err := ketcd.New(ketcd.WithClient(cli), ketcd.WithPrefix("/svc/"))
	if err != nil {
		b.Fatalf("new: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := p.Read(); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}
