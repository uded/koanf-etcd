package etcd

import "errors"

// Sentinel errors returned by Provider. Use errors.Is to detect.
var (
	// ErrUseParser is returned by Read() in blob mode. Callers should use
	// ReadBytes() with an appropriate koanf.Parser instead.
	ErrUseParser = errors.New("koanf-etcd: blob mode — use ReadBytes with a parser")

	// ErrEmptyPrefix is returned by Read() when WithStrict(true) is set
	// and a prefix read returns zero keys.
	ErrEmptyPrefix = errors.New("koanf-etcd: prefix read returned zero keys (strict mode)")

	// ErrNotBlob is returned by ReadBytes() when the Provider is not in
	// blob mode.
	ErrNotBlob = errors.New("koanf-etcd: ReadBytes only valid in blob mode")

	// ErrOptionConflict is returned by New() when mutually exclusive
	// options are supplied (e.g. WithKey and WithPrefix together, or
	// WithClient together with built-in connection options).
	ErrOptionConflict = errors.New("koanf-etcd: conflicting options")

	// ErrNoMode is returned by New() when neither WithKey nor WithPrefix
	// has been supplied.
	ErrNoMode = errors.New("koanf-etcd: must set exactly one of WithKey or WithPrefix")

	// ErrClosed is returned by methods called after Close().
	ErrClosed = errors.New("koanf-etcd: provider closed")
)
