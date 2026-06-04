package etcdwrite

import (
	"context"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ErrUnsafePrefix is returned when DeletePrefix is called with an empty
// or root-only ("/") prefix, which would wipe every key in the cluster.
// Pass AllowEmptyPrefix() as the trailing option to opt in.
var ErrUnsafePrefix = errors.New("etcdwrite: refusing to delete cluster-wide; pass AllowEmptyPrefix to override")

// Option configures write-helper behavior.
type Option func(*opts)

type opts struct {
	allowEmptyPrefix bool
}

// AllowEmptyPrefix lifts the empty-prefix guard on DeletePrefix.
// Intended only for multi-tenant cluster-wipe operations; callers
// must accept the blast radius.
func AllowEmptyPrefix() Option {
	return func(o *opts) { o.allowEmptyPrefix = true }
}

// Put writes a single key.
func Put(ctx context.Context, cli *clientv3.Client, key, value string) error {
	if _, err := cli.Put(ctx, key, value); err != nil {
		return fmt.Errorf("etcdwrite: put %q: %w", key, err)
	}
	return nil
}

// Delete removes a single key.
func Delete(ctx context.Context, cli *clientv3.Client, key string) error {
	if key == "" {
		return fmt.Errorf("%w: empty key", ErrUnsafePrefix)
	}
	if _, err := cli.Delete(ctx, key); err != nil {
		return fmt.Errorf("etcdwrite: delete %q: %w", key, err)
	}
	return nil
}

// DeletePrefix removes every key under the given prefix.
//
// By default, an empty or root-only ("/") prefix is rejected with
// ErrUnsafePrefix to prevent accidental cluster-wide deletion. Pass
// AllowEmptyPrefix() to opt in to that behavior.
func DeletePrefix(ctx context.Context, cli *clientv3.Client, prefix string, options ...Option) error {
	o := opts{}
	for _, opt := range options {
		opt(&o)
	}
	if !o.allowEmptyPrefix && (prefix == "" || prefix == "/") {
		return fmt.Errorf("%w: prefix=%q", ErrUnsafePrefix, prefix)
	}
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
