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

// tierState holds one priority tier of a bucket.
type tierState struct {
	offered counter.Sharded // offered load: includes requests about to be dropped
	dropped counter.Sharded // sampled for dropping, whether enforced or not
	shadow  counter.Sharded // sampled but let through (shadow mode or kill switch)
	dropPPM atomic.Uint32
}

type bucketState struct {
	bucket  *policy.Bucket
	tiers   [4]tierState // indexed by rlv1.Priority; 0 is unused
	shadow  atomic.Bool
	expires atomic.Int64 // unix nanos; directive is ignored after this (fail-open)
}

type compiled struct {
	table  *policy.Table
	states map[uint32]*bucketState
}

// Filter makes per-request decisions. Safe for concurrent use.
type Filter struct {
	cur        atomic.Pointer[compiled]
	killLocal  atomic.Bool  // from the config channel (xDS)
	killRemote atomic.Bool  // from the last directive
	directive  atomic.Int64 // unix nanos of the last directive
	mu         sync.Mutex   // serializes config swaps and snapshots
	lastSnap   time.Time
}

func New() *Filter {
	f := &Filter{lastSnap: time.Now()}
	f.cur.Store(&compiled{table: policy.Compile(&policy.File{}, 0), states: map[uint32]*bucketState{}})
	return f
}

// Decide is the per-request path: no locks, no network, integer math only.
// req.Priority picks the tier; empty means the bucket's default.
func (f *Filter) Decide(req policy.Request) Verdict {
	c := f.cur.Load()
	b := c.table.Match(req)
	if b == nil {
		return Allow
	}
	s := c.states[b.ID]
	prio := req.Priority
	if prio == "" {
		prio = b.Priority
	}
	t := &s.tiers[prio.Proto()]
	t.offered.Inc()
	p := t.dropPPM.Load()
	if p == 0 || rand.Uint32N(1_000_000) >= p {
		return Allow
	}
	t.dropped.Inc()
	if s.shadow.Load() || f.killLocal.Load() || f.killRemote.Load() {
		t.shadow.Inc()
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
		s.bucket = b
		s.shadow.Store(b.Mode == policy.ModeShadow)
		next.states[b.ID] = s
	}
	f.cur.Store(next)
}

// Version returns the config version in use.
func (f *Filter) Version() uint64 { return f.cur.Load().table.Version }

// SetKillSwitch sets the kill switch received on the config channel. While
// either it or the directive's kill switch is on, every bucket is shadow.
func (f *Filter) SetKillSwitch(on bool) { f.killLocal.Store(on) }

// KillSwitch reports whether the kill switch is in effect.
func (f *Filter) KillSwitch() bool { return f.killLocal.Load() || f.killRemote.Load() }

// Apply installs drop ratios from a directive, valid until now+TTL.
func (f *Filter) Apply(d *rlv1.Directive, now time.Time) {
	c := f.cur.Load()
	exp := now.Add(time.Duration(d.GetTtlMs()) * time.Millisecond).UnixNano()
	f.killRemote.Store(d.GetKillSwitch())
	f.directive.Store(now.UnixNano())
	for _, bd := range d.GetBuckets() {
		s, ok := c.states[bd.GetBucketId()]
		if !ok {
			continue
		}
		var ppm [4]uint32
		for i := range ppm {
			ppm[i] = bd.GetDropPpm()
		}
		for _, td := range bd.GetTiers() {
			if p := td.GetPriority(); p > 0 && int(p) < len(ppm) {
				ppm[p] = td.GetDropPpm()
			}
		}
		for i := 1; i < len(ppm); i++ {
			s.tiers[i].dropPPM.Store(min(ppm[i], 1_000_000))
		}
		s.expires.Store(exp)
	}
}

// LastDirective returns when the last directive was applied (zero if never).
func (f *Filter) LastDirective() time.Time {
	if n := f.directive.Load(); n != 0 {
		return time.Unix(0, n)
	}
	return time.Time{}
}

// ExpireStale resets ratios whose directive TTL has passed (fail-open).
// Called from the sync loop, not the hot path. Returns how many buckets were
// reset.
func (f *Filter) ExpireStale(now time.Time) int {
	n := 0
	for _, s := range f.cur.Load().states {
		if now.UnixNano() <= s.expires.Load() {
			continue
		}
		reset := false
		for i := range s.tiers {
			if s.tiers[i].dropPPM.Swap(0) != 0 {
				reset = true
			}
		}
		if reset {
			n++
		}
	}
	if n > 0 {
		f.killRemote.Store(false)
	}
	return n
}

// TierUsage is one bucket tier's usage over a snapshot window.
type TierUsage struct {
	Bucket   *policy.Bucket
	Priority rlv1.Priority
	Offered  uint64
	Dropped  uint64 // sampled for dropping, enforced or not
	Shadow   uint64 // sampled but let through
	DropPPM  uint32 // ratio in effect at snapshot time
}

// Snapshot returns usage since the previous Snapshot as a report, plus the
// per-tier numbers for metrics.
func (f *Filter) Snapshot(nodeID, zone string, now time.Time) (*rlv1.UsageReport, []TierUsage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.cur.Load()
	r := &rlv1.UsageReport{
		NodeId:            nodeID,
		Zone:              zone,
		WindowStartUnixMs: f.lastSnap.UnixMilli(),
		WindowEndUnixMs:   now.UnixMilli(),
	}
	f.lastSnap = now
	var detail []TierUsage
	for id, s := range c.states {
		for i := 1; i < len(s.tiers); i++ {
			t := &s.tiers[i]
			u := TierUsage{
				Bucket:   s.bucket,
				Priority: rlv1.Priority(i),
				Offered:  t.offered.Delta(),
				Dropped:  t.dropped.Delta(),
				Shadow:   t.shadow.Delta(),
				DropPPM:  t.dropPPM.Load(),
			}
			detail = append(detail, u)
			if u.Offered == 0 && u.Dropped == 0 {
				continue
			}
			r.Buckets = append(r.Buckets, &rlv1.BucketUsage{
				BucketId: id, Priority: u.Priority, Offered: u.Offered, Dropped: u.Dropped,
			})
		}
	}
	return r, detail
}
