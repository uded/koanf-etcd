package etcd_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/v2"
	clientv3 "go.etcd.io/etcd/client/v3"

	ketcd "github.com/uded/koanf-etcd"
)

// ExampleNew demonstrates tree-mode usage: read a prefix and merge into
// a koanf instance as a nested map.
func ExampleNew() {
	cli, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	defer cli.Close()

	p, err := ketcd.New(
		ketcd.WithClient(cli),
		ketcd.WithPrefix("/svc/"),
	)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	k := koanf.New(".")
	if err := k.Load(p, nil); err != nil {
		panic(err)
	}
	fmt.Println("db.host =", k.String("db.host"))
}

// ExampleNew_blob demonstrates blob mode: one key holds a whole YAML
// document, parsed by the caller's chosen parser.
func ExampleNew_blob() {
	cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	defer cli.Close()

	p, err := ketcd.New(
		ketcd.WithClient(cli),
		ketcd.WithKey("/cfg.yaml"),
		ketcd.WithBlob(),
		ketcd.WithUnflatten(false),
	)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	k := koanf.New(".")
	if err := k.Load(p, yaml.Parser()); err != nil {
		panic(err)
	}
	fmt.Println("loaded")
}

// ExampleProvider_Watch demonstrates a watch+reload pattern. Production
// code typically also runs a validator and atomically swaps a *Config
// pointer; see the README for the full recipe.
func ExampleProvider_Watch() {
	cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
	defer cli.Close()

	logger := slog.Default()
	p, _ := ketcd.New(
		ketcd.WithClient(cli),
		ketcd.WithPrefix("/svc/"),
		ketcd.WithDebounce(500*time.Millisecond),
		ketcd.WithOnWatchError(func(err error) {
			logger.Error("etcd watch error", "err", err)
		}),
	)
	defer p.Close()

	k := koanf.New(".")
	_ = k.Load(p, nil)

	_ = p.Watch(func(_ any, err error) {
		if err != nil {
			logger.Error("watch", "err", err)
			return
		}
		fresh := koanf.New(".")
		if err := fresh.Load(p, nil); err != nil {
			logger.Error("reload", "err", err)
			return
		}
		// atomic-swap your *Config here; see README
		_ = fresh
	})

	// hold here in real code
	_ = context.Background()
}
