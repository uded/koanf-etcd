package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"

	etcdwrite "github.com/uded/koanf-etcd/write"
)

func TestPut_And_Get(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := context.Background()
	if err := etcdwrite.Put(ctx, cli, "/k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := cli.Get(ctx, "/k")
	if err != nil || len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v" {
		t.Fatalf("get back: %v / %+v", err, resp)
	}
}

func TestDeletePrefix(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		etcdwrite.Put(ctx, cli, fmt.Sprintf("/p/%d", i), "x")
	}
	if err := etcdwrite.DeletePrefix(ctx, cli, "/p/"); err != nil {
		t.Fatalf("delete prefix: %v", err)
	}
	resp, _ := cli.Get(ctx, "/p/", clientv3.WithPrefix())
	if len(resp.Kvs) != 0 {
		t.Errorf("got %d keys; want 0", len(resp.Kvs))
	}
}

func TestDeletePrefix_RejectsEmpty(t *testing.T) {
	cli := embeddedEtcd(t)
	if err := etcdwrite.DeletePrefix(context.Background(), cli, ""); !errors.Is(err, etcdwrite.ErrUnsafePrefix) {
		t.Fatalf("want ErrUnsafePrefix for empty prefix, got %v", err)
	}
	if err := etcdwrite.DeletePrefix(context.Background(), cli, "/"); !errors.Is(err, etcdwrite.ErrUnsafePrefix) {
		t.Fatalf("want ErrUnsafePrefix for root prefix, got %v", err)
	}
}

func TestDeletePrefix_AllowEmptyOptIn(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := context.Background()
	etcdwrite.Put(ctx, cli, "/seed/a", "1")
	if err := etcdwrite.DeletePrefix(ctx, cli, "", etcdwrite.AllowEmptyPrefix()); err != nil {
		t.Fatalf("AllowEmptyPrefix should permit empty: %v", err)
	}
	resp, _ := cli.Get(ctx, "", clientv3.WithPrefix())
	if len(resp.Kvs) != 0 {
		t.Errorf("expected cluster wiped; got %d kvs", len(resp.Kvs))
	}
}

func TestDelete_RejectsEmpty(t *testing.T) {
	cli := embeddedEtcd(t)
	if err := etcdwrite.Delete(context.Background(), cli, ""); !errors.Is(err, etcdwrite.ErrUnsafePrefix) {
		t.Fatalf("want ErrUnsafePrefix for empty key, got %v", err)
	}
}

func TestPutAll_Atomic(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := context.Background()
	kvs := map[string]string{"/atomic/a": "1", "/atomic/b": "2", "/atomic/c": "3"}
	if err := etcdwrite.PutAll(ctx, cli, kvs); err != nil {
		t.Fatalf("PutAll: %v", err)
	}
	for k, want := range kvs {
		resp, _ := cli.Get(ctx, k)
		if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != want {
			t.Errorf("%s = %v; want %q", k, resp.Kvs, want)
		}
	}
}
