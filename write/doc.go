// Package etcdwrite contains optional write helpers for etcd v3 that
// compose with the koanf-etcd Provider. The Provider itself exposes no
// write methods by design — this subpackage is the only way to mutate
// etcd from the koanf-etcd module.
//
// PutAll uses a single etcd transaction so multi-key rollouts land
// atomically — the answer to etcd's lack of multi-key write atomicity
// in plain Puts.
package etcdwrite
