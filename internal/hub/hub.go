// Package hub is the downstream side shared by aggregators and controllers:
// it accepts Sync streams, collects UsageReports and fans Directives out.
package hub

import (
	"sync"
	"time"

	rlv1 "ratelimit/gen/ratelimit/v1"
)

// Usage is offered/dropped counts summed over one collection window.
type Usage struct {
	Offered uint64
	Dropped uint64
}

// Hub implements rlv1.RateLimitControlServer.
type Hub struct {
	rlv1.UnimplementedRateLimitControlServer

	staleAfter time.Duration
	now        func() time.Time

	mu      sync.Mutex
	pending []*rlv1.UsageReport
	subs    map[chan *rlv1.Directive]struct{}
	last    *rlv1.Directive // replayed to new subscribers
}

// New returns a Hub that ignores reports whose window ended more than
// staleAfter ago, so a lagging zone is dropped rather than summed stale.
func New(staleAfter time.Duration) *Hub {
	return &Hub{staleAfter: staleAfter, now: time.Now, subs: map[chan *rlv1.Directive]struct{}{}}
}

// Sync serves one downstream peer: reports in, directives out.
func (h *Hub) Sync(stream rlv1.RateLimitControl_SyncServer) error {
	ch := make(chan *rlv1.Directive, 1)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	if h.last != nil {
		ch <- h.last
	}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.subs, ch)
		h.mu.Unlock()
	}()

	errc := make(chan error, 1)
	go func() {
		for {
			r, err := stream.Recv()
			if err != nil {
				errc <- err
				return
			}
			h.mu.Lock()
			h.pending = append(h.pending, r)
			h.mu.Unlock()
		}
	}()

	for {
		select {
		case d := <-ch:
			if err := stream.Send(d); err != nil {
				return err
			}
		case err := <-errc:
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// Collect sums and clears every report received since the previous call,
// skipping stale ones.
func (h *Hub) Collect() map[uint32]Usage {
	h.mu.Lock()
	reports := h.pending
	h.pending = nil
	h.mu.Unlock()

	cutoff := h.now().Add(-h.staleAfter).UnixMilli()
	out := map[uint32]Usage{}
	for _, r := range reports {
		if r.GetWindowEndUnixMs() < cutoff {
			continue
		}
		for _, b := range r.GetBuckets() {
			u := out[b.GetBucketId()]
			u.Offered += b.GetOffered()
			u.Dropped += b.GetDropped()
			out[b.GetBucketId()] = u
		}
	}
	return out
}

// Broadcast sends d to every connected peer. A slow peer gets only the latest
// directive: an older pending one is replaced, never queued.
func (h *Hub) Broadcast(d *rlv1.Directive) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.last = d
	for ch := range h.subs {
		select {
		case <-ch:
		default:
		}
		ch <- d
	}
}

// Peers returns the number of connected downstream streams.
func (h *Hub) Peers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}
