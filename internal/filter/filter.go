// Package filter is the sidecar hot path: match, count, and drop by ratio.
// It never blocks on the network; directives arrive asynchronously.
package filter

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aupv9/valve/internal/counter"
	"github.com/aupv9/valve/internal/policy"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

type Verdict int

const (
	Allow Verdict = iota
	Reject429
)

type bucketState struct {
	offered counter.Sharded // offered load: includes requests about to be dropped
	dropped counter.Sharded
	dropPPM atomic.Uint32
	shadow  atomic.Bool
	expires atomic.Int64 // unix nanos; directive is ignored after this (fail-open)
}

type compiled struct {
	table  *policy.Table
	states map[uint32]*bucketState
}

// Filter makes per-request decisions. Safe for concurrent use.
type Filter struct {
	cur     atomic.Pointer[compiled]
	killAll atomic.Bool // global kill switch: every bucket behaves as shadow
	mu      sync.Mutex  // serializes config swaps
}

func New() *Filter {
	f := &Filter{}
	f.cur.Store(&compiled{table: policy.Compile(&policy.File{}, 0), states: map[uint32]*bucketState{}})
	return f
}

// Decide is the per-request path: no locks, no network, integer math only.
func (f *Filter) Decide(req policy.Request) Verdict {
	c := f.cur.Load()
	b := c.table.Match(req)
	if b == nil {
		return Allow
	}
	s := c.states[b.ID]
	s.offered.Inc()
	p := s.dropPPM.Load()
	if p == 0 || rand.Uint32N(1_000_000) >= p {
		return Allow
	}
	s.dropped.Inc()
	if s.shadow.Load() || f.killAll.Load() {
		return Allow
	}
	return Reject429
}

// SetTable installs a new config version. Counters and ratios of buckets that
// survive the swap are carried over so reporting stays continuous.
func (f *Filter) SetTable(t *policy.Table) {
	f.mu.Lock()
	defer f.mu.Unlock()
	old := f.cur.Load()
	next := &compiled{table: t, states: make(map[uint32]*bucketState)}
	for _, b := range t.Buckets() {
		s, ok := old.states[b.ID]
		if !ok {
			s = &bucketState{}
		}
		s.shadow.Store(b.Mode == policy.ModeShadow)
		next.states[b.ID] = s
	}
	f.cur.Store(next)
}

// SetKillSwitch forces every bucket to shadow behaviour when on.
func (f *Filter) SetKillSwitch(on bool) { f.killAll.Store(on) }

// Apply installs drop ratios from a directive, valid until now+TTL.
func (f *Filter) Apply(d *rlv1.Directive, now time.Time) {
	c := f.cur.Load()
	exp := now.Add(time.Duration(d.GetTtlMs()) * time.Millisecond).UnixNano()
	for _, bd := range d.GetBuckets() {
		if s, ok := c.states[bd.GetBucketId()]; ok {
			s.dropPPM.Store(min(bd.GetDropPpm(), 1_000_000))
			s.expires.Store(exp)
		}
	}
}

// ExpireStale resets ratios whose directive TTL has passed (fail-open).
// Called from the sync loop, not the hot path. Returns how many were reset.
func (f *Filter) ExpireStale(now time.Time) int {
	n := 0
	for _, s := range f.cur.Load().states {
		if s.dropPPM.Load() != 0 && now.UnixNano() > s.expires.Load() {
			s.dropPPM.Store(0)
			n++
		}
	}
	return n
}

// Snapshot returns per-bucket usage since the previous Snapshot call.
// Must be called from a single goroutine.
func (f *Filter) Snapshot(nodeID, zone string, now time.Time) *rlv1.UsageReport {
	c := f.cur.Load()
	r := &rlv1.UsageReport{NodeId: nodeID, Zone: zone, WindowEndUnixMs: now.UnixMilli()}
	for id, s := range c.states {
		off, drop := s.offered.Delta(), s.dropped.Delta()
		if off == 0 && drop == 0 {
			continue
		}
		r.Buckets = append(r.Buckets, &rlv1.BucketUsage{BucketId: id, Offered: off, Dropped: drop})
	}
	return r
}
