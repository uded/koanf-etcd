package etcd

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// watchLoop runs in its own goroutine. It owns the watch lifecycle:
// establish watch -> dispatch events to cb -> on chan close, reconnect
// with backoff -> on compaction, resync and resume.
func (p *Provider) watchLoop(ctx context.Context) {
	startRev := p.stats.revision.Load()
	if !p.settings.resumeFromRevision {
		startRev = 0
	} else if startRev > 0 {
		startRev++
	}

	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		opts := p.watchOpts(startRev)
		ch := p.client.Watch(ctx, p.watchKey(), opts...)
		attempt = 0
		if p.settings.onReconnect != nil && p.stats.totalReconnects.Load() > 0 {
			p.settings.onReconnect(int(p.stats.totalReconnects.Load()), nil)
		}

		drained, newRev, fatalErr := p.consumeWatch(ctx, ch)
		if fatalErr != nil {
			if p.settings.onWatchError != nil {
				p.settings.onWatchError(fatalErr)
			}
			if errors.Is(fatalErr, rpctypes.ErrCompacted) {
				if resyncRev, err := p.resync(ctx); err == nil {
					startRev = resyncRev + 1
					p.stats.totalResyncs.Add(1)
					p.stats.lastResyncUnix.Store(time.Now().UnixNano())
					if p.settings.onResync != nil {
						p.settings.onResync("compaction", resyncRev)
					}
					p.deliverResync(resyncRev)
					continue
				} else {
					if p.settings.onWatchError != nil {
						p.settings.onWatchError(fmt.Errorf("resync: %w", err))
					}
				}
			}
		}

		if drained || ctx.Err() != nil {
			return
		}

		if newRev > 0 {
			startRev = newRev + 1
		}

		attempt++
		p.stats.totalReconnects.Add(1)
		p.stats.lastReconnUnix.Store(time.Now().UnixNano())
		if p.settings.onReconnect != nil {
			p.settings.onReconnect(attempt, fatalErr)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff(attempt, p.settings.reconnectMin, p.settings.reconnectMax)):
		}
	}
}

// consumeWatch reads from a watch channel until it closes or the context
// is cancelled. Returns:
//
//   - drained: true if ctx was cancelled (caller should exit).
//   - newRev:  last observed revision (for resume on reconnect).
//   - fatalErr: a non-nil terminating error from the channel (compaction
//     or otherwise).
func (p *Provider) consumeWatch(ctx context.Context, ch clientv3.WatchChan) (drained bool, newRev int64, fatalErr error) {
	debounceWindow := p.settings.debounce
	var debounceTimer *time.Timer
	pending := []Event{}
	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch := pending
		pending = pending[:0]
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
			return true, newRev, nil
		case resp, ok := <-ch:
			if !ok {
				return false, newRev, nil
			}
			if err := resp.Err(); err != nil {
				return false, newRev, err
			}
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
			if debounceWindow > 0 {
				if debounceTimer == nil {
					debounceTimer = time.NewTimer(debounceWindow)
					timerCh = debounceTimer.C
				} else {
					if !debounceTimer.Stop() {
						select {
						case <-debounceTimer.C:
						default:
						}
					}
					debounceTimer.Reset(debounceWindow)
				}
			} else {
				flush()
			}
		case <-timerCh:
			flush()
			timerCh = nil
		}
	}
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
// revision. Updates p.stats.revision.
func (p *Provider) resync(ctx context.Context) (int64, error) {
	if p.settings.blob {
		if _, err := p.ReadBytes(); err != nil {
			return 0, err
		}
	} else {
		if _, err := p.Read(); err != nil {
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

// backoff returns an exponential-with-full-jitter duration for the given
// attempt (1-indexed), bounded by min..max.
func backoff(attempt int, min, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := min << (attempt - 1)
	if exp <= 0 || exp > max {
		exp = max
	}
	return time.Duration(rand.Int63n(int64(exp) + 1))
}
