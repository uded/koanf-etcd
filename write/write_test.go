package etcdwrite_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	etcdwrite "github.com/uded/koanf-etcd/write"
)

func newEmbedded(t *testing.T) *clientv3.Client {
	t.Helper()
	dir, _ := os.MkdirTemp("", "kwrite-*")
	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{{Scheme: "http", Host: freeAddr(t)}}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{{Scheme: "http", Host: freeAddr(t)}}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = fmt.Sprintf("default=%s", cfg.ListenPeerUrls[0].String())
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	<-e.Server.ReadyNotify()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{cfg.ListenClientUrls[0].Host}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() {
		cli.Close()
		e.Close()
		os.RemoveAll(dir)
	})
	return cli
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	return l.Addr().String()
}

func TestPut_And_Get(t *testing.T) {
	cli := newEmbedded(t)
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
	cli := newEmbedded(t)
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

func TestPutAll_Atomic(t *testing.T) {
	cli := newEmbedded(t)
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
