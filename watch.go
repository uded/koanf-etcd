package etcd

import (
	"context"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// watchLoop runs in its own goroutine. It owns the watch lifecycle:
// establish watch -> dispatch events to cb -> on chan close, reconnect
// with backoff -> on compaction, resync and resume.
//
// On return, the loop clears its watch-state slot so a subsequent call
// to Watch/WatchTyped can install a fresh callback — important when the
// caller cancels via WithWatchContext and wants to resubscribe without
// going through Close().
func (p *Provider) watchLoop(ctx context.Context) {
	// LIFO defer order matters:
	//   1. signalWatchDone runs FIRST — unblocks any Close() waiting on
	//      <-watchDone before we touch other state.
	//   2. recoverWatchPanic catches a panic while state is still live.
	//   3. clearWatchState resets the slot so a future Watch() can install
	//      a fresh callback after the goroutine exits.
	defer p.clearWatchState()
	defer p.recoverWatchPanic("watch loop")
	defer p.signalWatchDone()
	startRev := p.initialWatchRevision()
	attempt := 0
	// pendingResync is set when compaction is observed but the follow-up
	// resync() fails (e.g. etcd is also unreachable). In that state the
	// next loop iteration must retry the resync DIRECTLY rather than
	// opening a fresh Watch from the still-compacted startRev, which
	// would just receive ErrCompacted again and burn a Watch RPC per
	// wakeup while backoff escalates.
	pendingResync := false
	for {
		if ctx.Err() != nil {
			return
		}

		if pendingResync {
			resyncRev, err := p.doResync(ctx)
			if err == nil {
				startRev = resyncRev + 1
				pendingResync = false
				attempt = 0
				continue
			}
			// Still no etcd — back off and try the resync again on the
			// next iteration without firing a Watch RPC we know will
			// immediately ErrCompacted.
			attempt++
			wrapped := fmt.Errorf("pending resync: %w", err)
			p.recordReconnect(attempt, wrapped)
			if p.settings.onWatchError != nil {
				p.settings.onWatchError(wrapped, WatchErrorTransient)
			}
			if !p.sleepBackoff(ctx, attempt) {
				return
			}
			continue
		}

		ch := p.client.Watch(ctx, p.watchKey(), p.watchOpts(startRev)...)
		drained, newRev, madeProgress, fatalErr := p.consumeWatch(ctx, ch)

		// Reset attempt only when the session actually delivered events.
		// A session that fails immediately (no responses) keeps the
		// previous attempt count so backoff escalates properly toward
		// reconnectMax instead of pinning at min.
		if madeProgress {
			attempt = 0
		}

		if resyncRev, ok := p.tryHandleCompaction(ctx, fatalErr); ok {
			startRev = resyncRev + 1
			attempt = 0 // resync IS progress — restart cleanly
			continue
		}
		// tryHandleCompaction returned ok=false: either fatalErr wasn't
		// compaction, or it was compaction but the resync inside it
		// failed. In the latter case, flip pendingResync so the next
		// iteration retries the resync directly instead of re-opening a
		// doomed Watch from the still-compacted revision.
		if errors.Is(fatalErr, rpctypes.ErrCompacted) {
			pendingResync = true
		}
		if isFatalRPCError(fatalErr) {
			p.recordWatchError()
			if p.settings.onWatchError != nil {
				p.settings.onWatchError(fmt.Errorf("koanf-etcd: fatal: %w", fatalErr), WatchErrorAuth)
			}
			return
		}
		if drained || ctx.Err() != nil {
			return
		}
		if newRev > 0 {
			startRev = newRev + 1
		}

		attempt++
		p.recordReconnect(attempt, fatalErr)
		if !p.sleepBackoff(ctx, attempt) {
			return
		}
	}
}

// initialWatchRevision computes the revision the watch should start at
// based on resume-from-revision settings and any prior Read.
func (p *Provider) initialWatchRevision() int64 {
	startRev := p.stats.revision.Load()
	if !p.settings.resumeFromRevision {
		return 0
	}
	if startRev > 0 {
		startRev++
	}
	return startRev
}

// tryHandleCompaction handles a fatal watch error. If the error is
// ErrCompacted, it re-reads full state, emits a Resync event, and
// returns (newRevision, true). Otherwise it returns (0, false).
// It also invokes the OnWatchError callback for any fatal error.
// Compaction is classified as WatchErrorCompaction (recoverable via
// resync); other fatal errors are classified by classifyWatchError.
func (p *Provider) tryHandleCompaction(ctx context.Context, fatalErr error) (int64, bool) {
	if fatalErr == nil {
		return 0, false
	}
	compacted := errors.Is(fatalErr, rpctypes.ErrCompacted)
	// Only count non-compaction errors here; compaction is counted via
	// TotalResyncs below to avoid double-counting one event in two
	// buckets.
	if !compacted {
		p.recordWatchError()
	} else {
		// Compaction is a noteworthy event but not an "error" for the
		// totalWatchErrors counter. Still track the timestamp so
		// dashboards can correlate.
		p.stats.lastWatchErrorUnix.Store(time.Now().UnixNano())
	}
	if p.settings.onWatchError != nil {
		p.settings.onWatchError(fatalErr, classifyWatchError(fatalErr))
	}
	if !compacted {
		return 0, false
	}
	resyncRev, err := p.doResync(ctx)
	if err != nil {
		p.recordWatchError()
		if p.settings.onWatchError != nil {
			p.settings.onWatchError(fmt.Errorf("resync: %w", err), WatchErrorTransient)
		}
		return 0, false
	}
	return resyncRev, true
}

// doResync re-reads full state and, on success, fires all the bookkeeping
// associated with a successful compaction recovery: stats counters,
// OnResync callback, and the EventResync delivery to the watch callback.
// Returns the new revision the watch should resume after, or the resync
// error verbatim on failure so the caller can decide how to surface it.
//
// Lives here so both the first-shot path (tryHandleCompaction) and the
// retry path (pendingResync in watchLoop) share one source of truth.
func (p *Provider) doResync(ctx context.Context) (int64, error) {
	resyncRev, err := p.resync(ctx)
	if err != nil {
		return 0, err
	}
	p.stats.totalResyncs.Add(1)
	p.stats.lastResyncUnix.Store(time.Now().UnixNano())
	if p.settings.onResync != nil {
		p.settings.onResync("compaction", resyncRev)
	}
	p.deliverResync(resyncRev)
	return resyncRev, nil
}

// recordWatchError bumps the watch-error counter and timestamp. Called
// from each non-recoverable error path; compaction is excluded because
// it lands under TotalResyncs.
func (p *Provider) recordWatchError() {
	p.stats.totalWatchErrors.Add(1)
	p.stats.lastWatchErrorUnix.Store(time.Now().UnixNano())
}

// classifyWatchError maps an underlying watch error to a WatchErrorClass.
// The classifier is intentionally narrow: compaction is its own class,
// auth/permission failures are WatchErrorAuth, everything else maps to
// WatchErrorTransient. Callers that need to distinguish further can
// inspect the wrapped error directly.
func classifyWatchError(err error) WatchErrorClass {
	if err == nil {
		// Unreachable in current call sites; documented for safety so
		// callers that pass nil get a defined value back.
		return WatchErrorTransient
	}
	if errors.Is(err, rpctypes.ErrCompacted) {
		return WatchErrorCompaction
	}
	if isFatalRPCError(err) {
		return WatchErrorAuth
	}
	return WatchErrorTransient
}

// clearWatchState resets the per-watch slot under watchMu so a future
// Watch/WatchTyped call can succeed even though the previous watcher
// exited via its parent ctx (rather than Close). Safe to call when the
// Provider has already been Closed — fields are reassigned to nil only.
func (p *Provider) clearWatchState() {
	p.watchMu.Lock()
	p.watchCancel = nil
	p.watchCb = nil
	p.watchTypedCb = nil
	p.watchDone = nil
	p.watchMu.Unlock()
}

// signalWatchDone closes the watchDone channel so Close() can observe
// that the watch goroutine has exited. Safe to call once per goroutine
// lifetime; the channel is allocated in Watch/WatchTyped and replaced
// with nil by clearWatchState only after this defer runs.
func (p *Provider) signalWatchDone() {
	p.watchMu.Lock()
	if p.watchDone != nil {
		close(p.watchDone)
	}
	p.watchMu.Unlock()
}

// recordReconnect updates stats and fires the OnReconnect callback. The
// revision passed to the callback is the one the next watch will resume
// from — callers can correlate reconnects with potential data-gap windows.
func (p *Provider) recordReconnect(attempt int, lastErr error) {
	p.stats.totalReconnects.Add(1)
	p.stats.lastReconnUnix.Store(time.Now().UnixNano())
	if p.settings.onReconnect != nil {
		p.settings.onReconnect(attempt, lastErr, p.stats.revision.Load())
	}
}

// sleepBackoff waits the backoff duration or honors ctx cancellation.
// Returns false if the wait was interrupted by ctx cancel.
func (p *Provider) sleepBackoff(ctx context.Context, attempt int) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(backoff(attempt, p.settings.reconnectMin, p.settings.reconnectMax)):
		return true
	}
}

// consumeWatch reads from a watch channel until it closes or the context
// is cancelled. Returns:
//
//   - drained: true if ctx was cancelled (caller should exit).
//   - newRev:  last observed revision (for resume on reconnect).
//   - madeProgress: true if the session received at least one non-error
//     response — signals to watchLoop that backoff should reset.
//   - fatalErr: a non-nil terminating error from the channel (compaction
//     or otherwise).
func (p *Provider) consumeWatch(ctx context.Context, ch clientv3.WatchChan) (drained bool, newRev int64, madeProgress bool, fatalErr error) {
	debounceWindow := p.settings.debounce
	var debounceTimer *time.Timer
	pending := []Event{}
	flush := func() {
		if len(pending) == 0 {
			return
		}
		// Copy out so the caller's batch is independent from the
		// pending backing array. Without this, the next append(pending,...)
		// can mutate batch[i] if cap(pending) was large enough.
		batch := make([]Event, len(pending))
		copy(batch, pending)
		pending = pending[:0]
		// Optional: trim backing array if it grew far beyond the high
		// water mark to release memory back to the runtime.
		if cap(pending) > 4*len(batch) && cap(pending) > 1024 {
			pending = nil
		}
		p.stats.totalDebounceFlushes.Add(1)
		p.stats.lastFlushCoalesced.Store(int32(len(batch)))
		p.deliverBatch(batch)
	}
	defer func() {
		if debounceTimer != nil {
			debounceTimer.Stop()
		}
		flush()
	}()

	var timerCh <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return true, newRev, madeProgress, nil
		case resp, ok := <-ch:
			if !ok {
				return false, newRev, madeProgress, nil
			}
			if err := resp.Err(); err != nil {
				return false, newRev, madeProgress, err
			}
			madeProgress = true
			if resp.Header.Revision > newRev {
				newRev = resp.Header.Revision
				p.stats.revision.Store(newRev)
			}
			for _, ev := range resp.Events {
				e := p.translateEvent(ev)
				if e == nil {
					continue
				}
				pending = append(pending, *e)
			}
			if p.settings.maxPendingEvents > 0 && len(pending) >= p.settings.maxPendingEvents {
				// Force-flush before the debounce window closes; a
				// stuck consumer or an event flood can't grow pending
				// unboundedly.
				flush()
				// Stop the debounce timer if it was running; it will
				// be re-armed when the next event arrives.
				if debounceTimer != nil && !debounceTimer.Stop() {
					select {
					case <-debounceTimer.C:
					default:
					}
				}
				timerCh = nil
				continue
			}
			if debounceWindow > 0 {
				debounceTimer, timerCh = armDebounce(debounceTimer, debounceWindow)
			} else {
				flush()
			}
		case <-timerCh:
			flush()
			timerCh = nil
		}
	}
}

// armDebounce ensures the debounce timer is running with a fresh window.
// On first call (timer is nil), creates a new timer and returns it plus
// its channel. On subsequent calls, drains the existing timer (if it
// already fired) and resets it to window.
func armDebounce(timer *time.Timer, window time.Duration) (*time.Timer, <-chan time.Time) {
	if timer == nil {
		t := time.NewTimer(window)
		return t, t.C
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(window)
	return timer, timer.C
}

// translateEvent converts a clientv3 event to our Event, applying filters
// and the configured key transform. Returns nil if the event is filtered
// out (server-side filter should already prevent this, but we double-guard).
func (p *Provider) translateEvent(ev *clientv3.Event) *Event {
	put := ev.Type == clientv3.EventTypePut
	if p.settings.filterSet {
		if put && !p.settings.wantPut {
			return nil
		}
		if !put && !p.settings.wantDelete {
			return nil
		}
	}
	out := &Event{
		Key:      p.settings.keyTransform(string(ev.Kv.Key)),
		Value:    ev.Kv.Value,
		Revision: ev.Kv.ModRevision,
	}
	if put {
		out.Type = EventPut
		p.stats.totalPuts.Add(1)
	} else {
		out.Type = EventDelete
		p.stats.totalDeletes.Add(1)
	}
	return out
}

// deliverBatch dispatches a batch to whichever callback is registered.
func (p *Provider) deliverBatch(batch []Event) {
	defer p.recoverWatchPanic("watch callback")
	p.stats.lastBatch.Store(int32(len(batch)))
	p.stats.totalEventsDelivered.Add(uint64(len(batch)))
	p.watchMu.Lock()
	cb := p.watchCb
	tcb := p.watchTypedCb
	p.watchMu.Unlock()
	if tcb != nil {
		tcb(batch, nil)
	}
	if cb != nil {
		cb(nil, nil)
	}
}

// deliverResync emits a single Resync event.
func (p *Provider) deliverResync(rev int64) {
	resync := []Event{{Type: EventResync, Revision: rev}}
	p.deliverBatch(resync)
}

// resync re-reads the full state after compaction, returning the new
// revision. Updates p.stats.revision. The watch ctx is threaded into
// the read path so a Close() during a slow resync exits promptly.
func (p *Provider) resync(ctx context.Context) (int64, error) {
	if p.settings.blob {
		if _, err := p.readBytesCtx(ctx); err != nil {
			return 0, err
		}
	} else {
		if _, err := p.readCtx(ctx); err != nil {
			return 0, err
		}
	}
	return p.stats.revision.Load(), nil
}

// watchKey returns the etcd key (or prefix) to watch.
func (p *Provider) watchKey() string {
	if p.settings.key != "" {
		return p.settings.key
	}
	return p.settings.prefix
}

// watchOpts builds the clientv3 watch options.
func (p *Provider) watchOpts(startRev int64) []clientv3.OpOption {
	opts := []clientv3.OpOption{}
	if p.settings.prefix != "" {
		opts = append(opts, clientv3.WithPrefix())
	}
	if startRev > 0 {
		opts = append(opts, clientv3.WithRev(startRev))
	}
	if p.settings.progressNotify {
		opts = append(opts, clientv3.WithProgressNotify())
	}
	if p.settings.createdNotify {
		opts = append(opts, clientv3.WithCreatedNotify())
	}
	if p.settings.filterSet {
		if p.settings.wantPut && !p.settings.wantDelete {
			opts = append(opts, clientv3.WithFilterDelete())
		}
		if p.settings.wantDelete && !p.settings.wantPut {
			opts = append(opts, clientv3.WithFilterPut())
		}
	}
	return opts
}

// backoff returns a duration in [min, exp] where exp doubles per attempt
// (capped at max). attempt is 1-indexed. Uses full-jitter style but
// guarantees never returning less than min — a flapping cluster must
// not be hammered sub-min between reconnect attempts.
func backoff(attempt int, min, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := min << (attempt - 1)
	if exp <= 0 || exp > max { // overflow or above ceiling
		exp = max
	}
	if exp <= min {
		return min
	}
	// rand.Int64N in math/rand/v2 is lock-free per goroutine, vs the
	// global mutex on math/rand v1. Range [min, exp].
	return min + time.Duration(mathrand.Int64N(int64(exp-min)+1))
}

// recoverWatchPanic catches a panic in the watch goroutine or in a
// user-supplied callback. The library is embedded in production
// processes; we must not let a buggy callback take down the host.
// Routes the panic via the OnWatchError hook with WatchErrorFatal class
// and updates stats so consumers can detect the event.
func (p *Provider) recoverWatchPanic(site string) {
	r := recover()
	if r == nil {
		return
	}
	err := fmt.Errorf("koanf-etcd: panic in %s: %v", site, r)
	p.recordWatchError()
	if p.settings.onWatchError != nil {
		// Best-effort notify — guard against panicking callback inside
		// the callback by NOT installing another recover here; if the
		// onWatchError handler panics, the host process gets it.
		func() {
			defer func() { _ = recover() }()
			p.settings.onWatchError(err, WatchErrorFatal)
		}()
	}
}

// isFatalRPCError reports whether the watch session error is permanent
// and a retry loop would just hammer the cluster forever. PermissionDenied
// and Unauthenticated typically mean RBAC misconfig or expired
// credentials — neither resolves on its own.
func isFatalRPCError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, rpctypes.ErrPermissionDenied) ||
		errors.Is(err, rpctypes.ErrUserNotFound) ||
		errors.Is(err, rpctypes.ErrAuthFailed) ||
		errors.Is(err, rpctypes.ErrInvalidAuthToken) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.PermissionDenied, codes.Unauthenticated:
			return true
		}
	}
	return false
}
