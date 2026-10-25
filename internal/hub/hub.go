// Package hub serves Sync streams for aggregators and controllers.
package hub

import (
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

// Key identifies one usage series.
type Key struct {
	Bucket   uint32
	Priority rlv1.Priority
	Zone     string
}

// Rate is usage in requests per second.
type Rate struct {
	Offered float64
	Dropped float64
}

// Builder returns the directive for peers in a zone ("" if not known yet).
type Builder func(zone string) *rlv1.Directive

type nodeReport struct {
	zone       string
	start, end int64
	buckets    []*rlv1.BucketUsage
}

type peer struct {
	zone string
	ch   chan *rlv1.Directive
	kick chan struct{}
}

// Hub implements rlv1.RateLimitControlServer.
type Hub struct {
	rlv1.UnimplementedRateLimitControlServer

	staleAfter time.Duration
	now        func() time.Time

	mu      sync.Mutex
	nodes   map[string]*nodeReport
	peers   map[*peer]struct{}
	build   Builder
	serving bool
}

// New returns a serving Hub that ignores reports older than staleAfter.
func New(staleAfter time.Duration) *Hub {
	return &Hub{
		staleAfter: staleAfter,
		now:        time.Now,
		nodes:      map[string]*nodeReport{},
		peers:      map[*peer]struct{}{},
		serving:    true,
	}
}

// SetServing turns the hub on or off; off ends open streams with Unavailable.
func (h *Hub) SetServing(on bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.serving == on {
		return
	}
	h.serving = on
	if !on {
		for p := range h.peers {
			close(p.kick)
			delete(h.peers, p)
		}
		h.nodes = map[string]*nodeReport{}
		h.build = nil
	}
}

var errNotServing = status.Error(codes.Unavailable, "not serving (not the leader)")

// Sync serves one downstream peer: reports in, directives out.
func (h *Hub) Sync(stream rlv1.RateLimitControl_SyncServer) error {
	p := &peer{ch: make(chan *rlv1.Directive, 1), kick: make(chan struct{})}
	h.mu.Lock()
	if !h.serving {
		h.mu.Unlock()
		return errNotServing
	}
	h.peers[p] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.peers, p)
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
			h.record(p, r)
		}
	}()

	for {
		select {
		case d := <-p.ch:
			if err := stream.Send(d); err != nil {
				return err
			}
		case err := <-errc:
			return err
		case <-p.kick:
			return errNotServing
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (h *Hub) record(p *peer, r *rlv1.UsageReport) {
	start, end := r.GetWindowStartUnixMs(), r.GetWindowEndUnixMs()
	h.mu.Lock()
	defer h.mu.Unlock()
	if p.zone != r.GetZone() {
		p.zone = r.GetZone()
		if h.build != nil {
			offer(p.ch, h.build(p.zone))
		}
	}
	if end <= start {
		return
	}
	if old, ok := h.nodes[r.GetNodeId()]; ok && old.end >= end {
		return
	}
	h.nodes[r.GetNodeId()] = &nodeReport{zone: r.GetZone(), start: start, end: end, buckets: r.GetBuckets()}
}

// Rates sums the latest fresh report of every node, per second.
func (h *Hub) Rates() map[Key]Rate {
	cutoff := h.now().Add(-h.staleAfter).UnixMilli()
	out := map[Key]Rate{}
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, n := range h.nodes {
		if n.end < cutoff {
			delete(h.nodes, id)
			continue
		}
		secs := float64(n.end-n.start) / 1000
		for _, b := range n.buckets {
			k := Key{Bucket: b.GetBucketId(), Priority: b.GetPriority(), Zone: n.zone}
			r := out[k]
			r.Offered += float64(b.GetOffered()) / secs
			r.Dropped += float64(b.GetDropped()) / secs
			out[k] = r
		}
	}
	return out
}

// Broadcast sends every peer the directive for its zone.
func (h *Hub) Broadcast(build Builder) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.build = build
	byZone := map[string]*rlv1.Directive{}
	for p := range h.peers {
		d, ok := byZone[p.zone]
		if !ok {
			d = build(p.zone)
			byZone[p.zone] = d
		}
		offer(p.ch, d)
	}
}

// BroadcastAll sends the same directive to every peer.
func (h *Hub) BroadcastAll(d *rlv1.Directive) {
	h.Broadcast(func(string) *rlv1.Directive { return d })
}

func offer(ch chan *rlv1.Directive, d *rlv1.Directive) {
	select {
	case <-ch:
	default:
	}
	ch <- d
}

// Peers returns the number of connected downstream streams.
func (h *Hub) Peers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.peers)
}

// Report builds an upstream report from rates; nil keep keeps every bucket.
func Report(nodeID, zone string, rates map[Key]Rate, start, end time.Time, keep func(bucket uint32) bool) *rlv1.UsageReport {
	type bp struct {
		bucket uint32
		prio   rlv1.Priority
	}
	sum := map[bp]Rate{}
	for k, r := range rates {
		if keep != nil && !keep(k.Bucket) {
			continue
		}
		s := sum[bp{k.Bucket, k.Priority}]
		s.Offered += r.Offered
		s.Dropped += r.Dropped
		sum[bp{k.Bucket, k.Priority}] = s
	}
	secs := end.Sub(start).Seconds()
	rep := &rlv1.UsageReport{
		NodeId:            nodeID,
		Zone:              zone,
		WindowStartUnixMs: start.UnixMilli(),
		WindowEndUnixMs:   end.UnixMilli(),
	}
	for k, r := range sum {
		rep.Buckets = append(rep.Buckets, &rlv1.BucketUsage{
			BucketId: k.bucket,
			Priority: k.prio,
			Offered:  uint64(r.Offered*secs + 0.5),
			Dropped:  uint64(r.Dropped*secs + 0.5),
		})
	}
	return rep
}
