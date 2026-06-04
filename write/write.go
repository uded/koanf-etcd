package etcdwrite

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Put writes a single key.
func Put(ctx context.Context, cli *clientv3.Client, key, value string) error {
	if _, err := cli.Put(ctx, key, value); err != nil {
		return fmt.Errorf("etcdwrite: put %q: %w", key, err)
	}
	return nil
}

// Delete removes a single key.
func Delete(ctx context.Context, cli *clientv3.Client, key string) error {
	if _, err := cli.Delete(ctx, key); err != nil {
		return fmt.Errorf("etcdwrite: delete %q: %w", key, err)
	}
	return nil
}

// DeletePrefix removes every key under the given prefix.
func DeletePrefix(ctx context.Context, cli *clientv3.Client, prefix string) error {
	if _, err := cli.Delete(ctx, prefix, clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("etcdwrite: delete prefix %q: %w", prefix, err)
	}
	return nil
}

// PutAll writes a set of key/value pairs in a single etcd transaction.
// Either all writes land (and become visible at the same revision) or
// none do.
func PutAll(ctx context.Context, cli *clientv3.Client, kvs map[string]string) error {
	if len(kvs) == 0 {
		return nil
	}
	ops := make([]clientv3.Op, 0, len(kvs))
	for k, v := range kvs {
		ops = append(ops, clientv3.OpPut(k, v))
	}
	txn := cli.Txn(ctx).Then(ops...)
	resp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("etcdwrite: PutAll commit: %w", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("etcdwrite: PutAll transaction did not succeed")
	}
	return nil
}
