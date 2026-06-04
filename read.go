package etcd

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Read implements koanf.Provider. In single-key mode it returns a one-key
// nested map. In prefix mode it returns a nested map of every key under
// the prefix, with the prefix stripped and "/" replaced by the delimiter
// (configurable). In blob mode it returns ErrUseParser (use ReadBytes
// with a parser).
func (p *Provider) Read() (map[string]any, error) {
	if p.closed.Load() {
		return nil, ErrClosed
	}
	if p.settings.blob {
		return nil, ErrUseParser
	}
	if p.settings.key != "" {
		return p.readSingle()
	}
	return p.readPrefix()
}

// readSingle reads exactly the configured key and returns a nested map
// based on the koanf path produced by the key transform.
func (p *Provider) readSingle() (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.settings.readTimeout)
	defer cancel()

	resp, err := p.client.Get(ctx, p.settings.key, p.readOpts(false)...)
	if err != nil {
		return nil, fmt.Errorf("read single key %q: %w", p.settings.key, err)
	}
	p.stats.revision.Store(resp.Header.Revision)

	if len(resp.Kvs) == 0 {
		if p.settings.strict {
			return nil, fmt.Errorf("%w: key=%q", ErrEmptyPrefix, p.settings.key)
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
		return unflattenMap(flat, p.settings.delim), nil
	}
	return flat, nil
}

// readPrefix performs a (paginated) prefix read and returns either a flat
// or nested map depending on settings.unflatten.
func (p *Provider) readPrefix() (map[string]any, error) {
	flat := map[string]any{}
	var lastRev int64
	var lastKey string
	more := true
	first := true

	for more {
		ctx, cancel := context.WithTimeout(context.Background(), p.settings.readTimeout)
		var opts []clientv3.OpOption
		var key string
		if first {
			key = p.settings.prefix
			opts = append(opts, clientv3.WithPrefix())
		} else {
			// Paginate from lastKey+\x00 through the end of the prefix range.
			key = lastKey + "\x00"
			opts = append(opts, clientv3.WithRange(prefixEnd(p.settings.prefix)))
		}
		if p.settings.serializable {
			opts = append(opts, clientv3.WithSerializable())
		}
		if p.settings.readRevision > 0 {
			opts = append(opts, clientv3.WithRev(p.settings.readRevision))
		}
		if p.settings.limit > 0 {
			opts = append(opts, clientv3.WithLimit(p.settings.limit))
			opts = append(opts, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
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
		return unflattenMap(flat, p.settings.delim), nil
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
// nested map[string]any. Identical semantics to confmap.Provider's
// internal unflatten.
func unflattenMap(flat map[string]any, delim string) map[string]any {
	out := make(map[string]any, len(flat))
	for k, v := range flat {
		setNested(out, k, v, delim)
	}
	return out
}

func setNested(m map[string]any, path string, v any, delim string) {
	if delim == "" {
		m[path] = v
		return
	}
	parts := splitPath(path, delim)
	cur := m
	for i, p := range parts {
		if i == len(parts)-1 {
			cur[p] = v
			return
		}
		next, ok := cur[p].(map[string]any)
		if !ok {
			next = make(map[string]any)
			cur[p] = next
		}
		cur = next
	}
}

func splitPath(s, delim string) []string {
	out := []string{}
	last := 0
	for i := 0; i+len(delim) <= len(s); i++ {
		if s[i:i+len(delim)] == delim {
			out = append(out, s[last:i])
			i += len(delim) - 1
			last = i + 1
		}
	}
	out = append(out, s[last:])
	// drop leading empty produced when path starts with delim (e.g. ".app.x")
	for len(out) > 0 && out[0] == "" {
		out = out[1:]
	}
	return out
}
