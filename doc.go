// Package etcd provides a koanf v2 Provider backed by etcd v3.
//
// The Provider supports:
//
//   - Tree mode (WithPrefix): read all keys under a prefix and return a nested
//     map by unflattening on a configurable delimiter. The prefix is stripped
//     and string values are TrimSpace'd by default.
//
//   - Blob mode (WithBlob, WithKey): read a single key whose value is a
//     whole document; Read returns ErrUseParser and the caller supplies a
//     koanf.Parser via ReadBytes.
//
//   - Watch with resume-from-revision (no read/watch gap), bounded
//     exponential reconnect backoff, compaction recovery (Resync event),
//     debounce, and clean context cancellation.
//
//   - Bring-your-own *clientv3.Client (WithClient) — shared with the
//     caller's own watchers; not closed by the Provider.
//
// See the README for motivation, quickstart, options reference,
// security notes, and an interop recipe with koanf-structdefaults,
// koanf-validate, and file/env providers.
package etcd
