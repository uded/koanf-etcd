package etcd

import "errors"

// Sentinel errors returned by Provider. Use errors.Is to detect.
var (
	// ErrUseParser is returned by Read() in blob mode. Callers should use
	// ReadBytes() with an appropriate koanf.Parser instead.
	ErrUseParser = errors.New("koanf-etcd: blob mode — use ReadBytes with a parser")

	// ErrEmptyPrefix is returned by Read() (prefix mode only) when
	// WithStrict(true) is set and a prefix read returns zero keys.
	// Single-key Read and blob ReadBytes use ErrKeyNotFound instead.
	ErrEmptyPrefix = errors.New("koanf-etcd: prefix read returned zero keys (strict mode)")

	// ErrKeyNotFound is returned by ReadBytes() (blob mode) and by Read()
	// in single-key mode when WithStrict(true) is set and the configured
	// key does not exist. ErrEmptyPrefix remains the sentinel for the
	// prefix-mode equivalent.
	ErrKeyNotFound = errors.New("koanf-etcd: key not found")

	// ErrWatchActive is returned by Watch / WatchTyped when a watch is
	// already running. Use errors.Is to detect. Resolve by calling Close
	// or by cancelling the parent context (when set via WithWatchContext)
	// and letting the loop exit.
	ErrWatchActive = errors.New("koanf-etcd: watch already active")

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

	// ErrPathCollision is returned by Read() when two etcd keys
	// resolve to paths where one is a prefix of the other (e.g. both
	// `/svc/db` and `/svc/db/host` exist under the same prefix). The
	// nested-map representation cannot hold both a leaf value and a
	// sub-map at the same path.
	ErrPathCollision = errors.New("koanf-etcd: path collision: a key value cannot coexist with a sub-tree at the same path")
)

// WatchErrorClass classifies non-recoverable watch errors so callbacks
// don't have to string-match the error to decide whether to retry,
// page someone, or surface the failure to the user.
type WatchErrorClass int

const (
	// WatchErrorTransient is a recoverable error — typically a network
	// glitch or a transient gRPC code. The watch loop will reconnect.
	WatchErrorTransient WatchErrorClass = iota + 1

	// WatchErrorCompaction is etcd's signal that the watch revision has
	// been compacted. The watch loop triggers a Resync.
	WatchErrorCompaction

	// WatchErrorAuth is a permission or authentication failure. The
	// watch loop does NOT retry; the caller must reconfigure
	// credentials or RBAC.
	WatchErrorAuth

	// WatchErrorFatal is any other non-recoverable error (e.g. invalid
	// argument, malformed response, internal bug, panic in a consumer
	// callback). The watch loop does NOT retry.
	WatchErrorFatal
)
