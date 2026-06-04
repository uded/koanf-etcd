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
	// LIFO order matters: recoverWatchPanic runs FIRST so the panic is
	// caught while watch state is still live, then clearWatchState resets
	// the slot so a future Watch() call can install a fresh callback.
	defer p.clearWatchState()
	defer p.recoverWatchPanic("watch loop")
	startRev := p.initialWatchRevision()
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}

		if p.settings.logger != nil && attempt > 0 {
			p.settings.logger.Info("koanf-etcd: watch reconnecting",
				"attempt", attempt,
				"start_revision", startRev,
			)
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
		if isFatalRPCError(fatalErr) {
			if p.settings.logger != nil {
				p.settings.logger.Error("koanf-etcd: fatal watch error; not retrying",
					"err", fatalErr.Error(),
				)
			}
			if p.settings.onWatchError != nil {
				p.settings.onWatchError(fmt.Errorf("koanf-etcd: fatal: %w", fatalErr))
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
func (p *Provider) tryHandleCompaction(ctx context.Context, fatalErr error) (int64, bool) {
	if fatalErr == nil {
		return 0, false
	}
	if p.settings.onWatchError != nil {
		p.settings.onWatchError(fatalErr)
	}
	if !errors.Is(fatalErr, rpctypes.ErrCompacted) {
		return 0, false
	}
	resyncRev, err := p.resync(ctx)
	if err != nil {
		if p.settings.onWatchError != nil {
			p.settings.onWatchError(fmt.Errorf("resync: %w", err))
		}
		return 0, false
	}
	p.stats.totalResyncs.Add(1)
	p.stats.lastResyncUnix.Store(time.Now().UnixNano())
	if p.settings.onResync != nil {
		p.settings.onResync("compaction", resyncRev)
	}
	if p.settings.logger != nil {
		p.settings.logger.Warn("koanf-etcd: compaction recovery — resynced",
			"new_revision", resyncRev,
		)
	}
	p.deliverResync(resyncRev)
	return resyncRev, true
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
	p.watchMu.Unlock()
}

// recordReconnect updates stats and fires the OnReconnect callback.
func (p *Provider) recordReconnect(attempt int, lastErr error) {
	p.stats.totalReconnects.Add(1)
	p.stats.lastReconnUnix.Store(time.Now().UnixNano())
	if p.settings.onReconnect != nil {
		p.settings.onReconnect(attempt, lastErr)
	}
	if p.settings.logger != nil {
		p.settings.logger.Warn("koanf-etcd: scheduling reconnect",
			"attempt", attempt,
			"last_err", errString(lastErr),
		)
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
// Routes the panic via the logger, the OnWatchError hook, and updates
// stats so consumers can detect the event.
func (p *Provider) recoverWatchPanic(site string) {
	r := recover()
	if r == nil {
		return
	}
	err := fmt.Errorf("koanf-etcd: panic in %s: %v", site, r)
	if p.settings.logger != nil {
		p.settings.logger.Error("koanf-etcd: panic recovered in watch path",
			"site", site,
			"panic", fmt.Sprint(r),
		)
	}
	if p.settings.onWatchError != nil {
		// Best-effort notify — guard against panicking callback inside
		// the callback by NOT installing another recover here; if the
		// onWatchError handler panics, the host process gets it.
		func() {
			defer func() { _ = recover() }()
			p.settings.onWatchError(err)
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

// errString gives a safe string form of an error, including nil.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
