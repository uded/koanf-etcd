package etcd

import (
	"context"
	"fmt"
	"strings"

	"github.com/knadh/koanf/maps"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Read implements koanf.Provider. In single-key mode it returns a one-key
// nested map. In prefix mode it returns a nested map of every key under
// the prefix, with the prefix stripped and "/" replaced by the delimiter
// (configurable). In blob mode it returns ErrUseParser (use ReadBytes
// with a parser).
//
// Read is equivalent to ReadCtx(context.Background()). Use ReadCtx when
// you need to bound the underlying etcd RPCs with a caller-supplied
// deadline or cancel.
func (p *Provider) Read() (map[string]any, error) {
	return p.readCtx(context.Background())
}

// ReadBytes implements koanf.Provider for blob mode. In blob mode it
// returns the raw value of the configured key. Outside blob mode it
// returns ErrNotBlob — use Read() instead.
//
// ReadBytes is equivalent to ReadBytesCtx(context.Background()). Use
// ReadBytesCtx when you need to bound the underlying etcd RPC with a
// caller-supplied deadline or cancel.
func (p *Provider) ReadBytes() ([]byte, error) {
	return p.readBytesCtx(context.Background())
}

// ReadCtx is a context-aware variant of Read. The supplied ctx bounds
// the underlying etcd RPCs; the per-request timeout from WithReadTimeout
// still applies independently. Cancelling ctx aborts an in-flight read
// promptly with a wrapped context.Canceled or context.DeadlineExceeded.
//
// Use ReadCtx (rather than Read) when the caller owns a deadline that
// should govern the whole read — for example a request-scoped timeout
// in an HTTP handler or a graceful-shutdown ctx that must short-circuit
// a slow prefix scan.
func (p *Provider) ReadCtx(ctx context.Context) (map[string]any, error) {
	return p.readCtx(ctx)
}

// ReadBytesCtx is a context-aware variant of ReadBytes for blob mode.
// See ReadCtx for the semantics of the supplied ctx.
func (p *Provider) ReadBytesCtx(ctx context.Context) ([]byte, error) {
	return p.readBytesCtx(ctx)
}

// readCtx is the context-aware backend for Read. The watch loop's resync
// path threads its own ctx so shutdown isn't blocked by a slow re-read.
// Increments TotalReads on a successful return.
func (p *Provider) readCtx(parent context.Context) (map[string]any, error) {
	if p.closed.Load() {
		return nil, ErrClosed
	}
	if p.settings.blob {
		return nil, ErrUseParser
	}
	var (
		out map[string]any
		err error
	)
	if p.settings.key != "" {
		out, err = p.readSingle(parent)
	} else {
		out, err = p.readPrefix(parent)
	}
	if err != nil {
		return nil, err
	}
	p.stats.totalReads.Add(1)
	return out, nil
}

// readBytesCtx is the context-aware backend for ReadBytes. Increments
// TotalReads on a successful return.
func (p *Provider) readBytesCtx(parent context.Context) ([]byte, error) {
	if p.closed.Load() {
		return nil, ErrClosed
	}
	if !p.settings.blob {
		return nil, ErrNotBlob
	}
	ctx, cancel := context.WithTimeout(parent, p.settings.readTimeout)
	defer cancel()

	resp, err := p.client.Get(ctx, p.settings.key, p.readOpts(false)...)
	if err != nil {
		return nil, fmt.Errorf("read blob %q: %w", p.settings.key, err)
	}
	p.stats.revision.Store(resp.Header.Revision)

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("%w: key=%q", ErrKeyNotFound, p.settings.key)
	}
	p.stats.totalReads.Add(1)
	return resp.Kvs[0].Value, nil
}

// readSingle reads exactly the configured key and returns a nested map
// based on the koanf path produced by the key transform.
func (p *Provider) readSingle(parent context.Context) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(parent, p.settings.readTimeout)
	defer cancel()

	resp, err := p.client.Get(ctx, p.settings.key, p.readOpts(false)...)
	if err != nil {
		return nil, fmt.Errorf("read single key %q: %w", p.settings.key, err)
	}
	p.stats.revision.Store(resp.Header.Revision)

	if len(resp.Kvs) == 0 {
		if p.settings.strict {
			return nil, fmt.Errorf("%w: key=%q", ErrKeyNotFound, p.settings.key)
		}
		return map[string]any{}, nil
	}

	kv := resp.Kvs[0]
	v, err := p.settings.valueTransform(string(kv.Key), kv.Value)
	if err != nil {
		return nil, fmt.Errorf("value transform %q: %w", kv.Key, err)
	}
	path := p.settings.keyTransform(string(kv.Key))

	flat := map[string]any{path: v}
	if p.settings.unflatten {
		nested, err := unflattenMap(flat, p.settings.delim)
		if err != nil {
			return nil, fmt.Errorf("read: %w", err)
		}
		return nested, nil
	}
	return flat, nil
}

// readPrefix performs a (paginated) prefix read and returns either a flat
// or nested map depending on settings.unflatten.
func (p *Provider) readPrefix(parent context.Context) (map[string]any, error) {
	flat := map[string]any{}
	var lastRev int64
	var lastKey string
	more := true
	first := true

	// Hoist the option fragments that don't change per page out of the
	// loop. A 10k-key prefix paginated at limit=500 would otherwise
	// rebuild this slice 20× per call for no semantic benefit.
	staticOpts := []clientv3.OpOption{}
	if p.settings.serializable {
		staticOpts = append(staticOpts, clientv3.WithSerializable())
	}
	if p.settings.readRevision > 0 {
		staticOpts = append(staticOpts, clientv3.WithRev(p.settings.readRevision))
	}
	if p.settings.limit > 0 {
		staticOpts = append(staticOpts,
			clientv3.WithLimit(p.settings.limit),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		)
	}
	rangeEnd := prefixEnd(p.settings.prefix)

	for more {
		ctx, cancel := context.WithTimeout(parent, p.settings.readTimeout)
		// Copy the static options and append the page-specific bound.
		opts := append([]clientv3.OpOption(nil), staticOpts...)
		var key string
		if first {
			key = p.settings.prefix
			opts = append(opts, clientv3.WithPrefix())
		} else {
			// Paginate from lastKey+\x00 through the end of the prefix range.
			key = lastKey + "\x00"
			opts = append(opts, clientv3.WithRange(rangeEnd))
		}

		resp, err := p.client.Get(ctx, key, opts...)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("read prefix %q: %w", p.settings.prefix, err)
		}
		lastRev = resp.Header.Revision

		for _, kv := range resp.Kvs {
			v, err := p.settings.valueTransform(string(kv.Key), kv.Value)
			if err != nil {
				return nil, fmt.Errorf("value transform %q: %w", kv.Key, err)
			}
			path := p.settings.keyTransform(string(kv.Key))
			flat[path] = v
			lastKey = string(kv.Key)
		}

		if resp.More && len(resp.Kvs) == 0 {
			return nil, fmt.Errorf("read prefix %q: etcd reported More=true with empty page", p.settings.prefix)
		}

		first = false
		more = resp.More && p.settings.limit > 0
	}

	p.stats.revision.Store(lastRev)

	if len(flat) == 0 {
		if p.settings.strict {
			return nil, fmt.Errorf("%w: prefix=%q", ErrEmptyPrefix, p.settings.prefix)
		}
		if p.settings.onEmpty != nil {
			p.settings.onEmpty(p.settings.prefix)
		}
		return map[string]any{}, nil
	}

	if p.settings.unflatten {
		nested, err := unflattenMap(flat, p.settings.delim)
		if err != nil {
			return nil, fmt.Errorf("read: %w", err)
		}
		return nested, nil
	}
	return flat, nil
}

// prefixEnd returns the smallest byte string strictly greater than every
// key with the given prefix. Mirrors etcd clientv3's internal helper.
func prefixEnd(prefix string) string {
	if prefix == "" {
		return "\x00"
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return "\x00"
}

// readOpts builds clientv3.OpOption based on settings. The `withPrefix`
// flag determines whether to add WithPrefix (used by readPrefix).
func (p *Provider) readOpts(withPrefix bool) []clientv3.OpOption {
	opts := []clientv3.OpOption{}
	if withPrefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	if p.settings.serializable {
		opts = append(opts, clientv3.WithSerializable())
	}
	if p.settings.readRevision > 0 {
		opts = append(opts, clientv3.WithRev(p.settings.readRevision))
	}
	return opts
}

// unflattenMap turns a flat map keyed by delim-separated paths into a
// nested map[string]any, delegating the heavy lifting to
// github.com/knadh/koanf/maps.Unflatten. It adds two behaviors that the
// upstream helper deliberately omits:
//
//  1. Normalizes leading and trailing empty segments produced by keys
//     like ".app.db" or "app.db." — otherwise those produce an empty-
//     string key in the nested map.
//  2. Reports ErrPathCollision when two etcd keys would resolve to
//     paths where one is a prefix of the other (a leaf value cannot
//     coexist with a sub-tree at the same path).
//
// When delim is empty, every key is treated literally and no
// unflattening occurs.
func unflattenMap(flat map[string]any, delim string) (map[string]any, error) {
	if delim == "" {
		out := make(map[string]any, len(flat))
		for k, v := range flat {
			out[k] = v
		}
		return out, nil
	}

	cleaned := make(map[string]any, len(flat))
	leaves := make(map[string]struct{}, len(flat))
	subtrees := make(map[string]struct{}, len(flat))

	for k, v := range flat {
		parts := splitCleanPath(k, delim)
		if len(parts) == 0 {
			// All-empty path (e.g. "" or "...") — nothing meaningful to
			// place; skip rather than producing an empty-key entry.
			continue
		}
		path := strings.Join(parts, delim)

		// A previously-seen sub-tree at this exact path would be
		// shadowed by placing a leaf here.
		if _, isSubtree := subtrees[path]; isSubtree {
			return nil, fmt.Errorf("%w: key=%q would shadow sub-tree", ErrPathCollision, path)
		}
		// Walk parents — none may already be a leaf.
		for i := 1; i < len(parts); i++ {
			parent := strings.Join(parts[:i], delim)
			if _, isLeaf := leaves[parent]; isLeaf {
				return nil, fmt.Errorf("%w: key=%q parent %q is a leaf", ErrPathCollision, path, parent)
			}
			subtrees[parent] = struct{}{}
		}

		leaves[path] = struct{}{}
		cleaned[path] = v
	}

	return maps.Unflatten(cleaned, delim), nil
}

// splitCleanPath splits s on delim and trims empty segments produced by
// a leading or trailing delim (e.g. ".app.db" or "app.db."). Interior
// empty segments are preserved — keys like "a..b" still place a "" key
// inside "a", which matches our pre-koanf/maps behavior. This wrapper
// exists because koanf/maps.Unflatten uses raw strings.Split, which
// would otherwise create an empty-string key in the nested map for
// edge-shaped keys.
func splitCleanPath(s, delim string) []string {
	parts := strings.Split(s, delim)
	for len(parts) > 0 && parts[0] == "" {
		parts = parts[1:]
	}
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}
