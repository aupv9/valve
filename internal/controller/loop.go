package controller

import (
	"strconv"
	"sync"
	"time"

	"github.com/aupv9/valve/internal/hub"
	"github.com/aupv9/valve/internal/policy"
	"github.com/aupv9/valve/internal/upstream"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

// Tier is where a controller sits in the hierarchy.
type Tier string

const (
	// TierRegional computes a region's buckets; global ones come from Global
	// when set, otherwise they are treated as regional.
	TierRegional Tier = "regional"
	// TierGlobal computes global buckets from regional controllers' usage.
	TierGlobal Tier = "global"
)

// Loop turns usage into directives once per cycle.
type Loop struct {
	Hub      *hub.Hub
	Params   Params
	Interval time.Duration
	TTL      time.Duration
	Tier     Tier

	// Owns selects this shard's buckets; nil means every bucket.
	Owns func(bucketID uint32) bool
	// Global holds the global tier's directives; nil means no global tier.
	Global *upstream.Merger

	mu     sync.Mutex
	table  *policy.Table
	states map[stateKey]*BucketState
	shares map[uint32]float64
	kill   bool
	rates  map[hub.Key]hub.Rate
}

type stateKey struct {
	bucket uint32
	zone   string
}

// SetTable installs a new policy version. State of surviving buckets is kept.
func (l *Loop) SetTable(t *policy.Table) {
	l.mu.Lock()
	defer l.mu.Unlock()
	next := make(map[stateKey]*BucketState)
	for k, s := range l.states {
		if b := t.Bucket(k.bucket); b != nil && (k.zone == "") == (b.Scope != policy.ScopeZonal) {
			next[k] = s
		}
	}
	l.table, l.states = t, next
	if l.shares == nil {
		l.shares = map[uint32]float64{}
	}
}

// SetKillSwitch makes every directive carry the kill switch.
func (l *Loop) SetKillSwitch(on bool) {
	l.mu.Lock()
	l.kill = on
	l.mu.Unlock()
}

// Result is what one Tick computed.
type Result struct {
	Shared  []*rlv1.BucketDirective
	ByZone  map[string][]*rlv1.BucketDirective
	Kill    bool
	Version uint64
}

// Directive returns the directive for peers in zone.
func (r *Result) Directive(zone string, ttl time.Duration) *rlv1.Directive {
	d := &rlv1.Directive{ConfigVersion: r.Version, TtlMs: ttl.Milliseconds(), KillSwitch: r.Kill}
	d.Buckets = append(d.Buckets, r.Shared...)
	d.Buckets = append(d.Buckets, r.ByZone[zone]...)
	return d
}

// Tick runs one control cycle and broadcasts the resulting directives.
func (l *Loop) Tick() *Result {
	rates := l.Hub.Rates()

	l.mu.Lock()
	l.rates = rates
	res := l.compute(rates)
	l.mu.Unlock()

	l.Hub.Broadcast(func(zone string) *rlv1.Directive { return res.Directive(zone, l.TTL) })
	return res
}

func (l *Loop) compute(rates map[hub.Key]hub.Rate) *Result {
	res := &Result{ByZone: map[string][]*rlv1.BucketDirective{}, Kill: l.kill}
	if l.Global != nil {
		if g := l.Global.Merged(); g != nil && g.GetKillSwitch() {
			res.Kill = true
		}
	}
	if l.table == nil {
		return res
	}
	res.Version = l.table.Version

	type zoneTiers map[string]*Tiers
	usage := map[uint32]zoneTiers{}
	for k, r := range rates {
		b := l.table.Bucket(k.Bucket)
		if b == nil {
			continue
		}
		prio := k.Priority
		if prio == rlv1.Priority_PRIORITY_UNSPECIFIED {
			prio = b.Priority.Proto()
		}
		zt := usage[k.Bucket]
		if zt == nil {
			zt = zoneTiers{}
			usage[k.Bucket] = zt
		}
		t := zt[k.Zone]
		if t == nil {
			t = &Tiers{}
			zt[k.Zone] = t
		}
		t[prio] += r.Offered
	}
	total := func(id uint32) Tiers {
		var sum Tiers
		for _, t := range usage[id] {
			for i := range sum {
				sum[i] += t[i]
			}
		}
		return sum
	}

	for _, b := range l.table.Buckets() {
		if l.Owns != nil && !l.Owns(b.ID) {
			continue
		}
		limit := float64(b.LimitRPS)
		switch {
		case l.Tier == TierGlobal:
			if b.Scope == policy.ScopeGlobal {
				res.Shared = append(res.Shared, l.step(stateKey{bucket: b.ID}, total(b.ID), limit))
			}

		case b.Scope == policy.ScopeZonal:
			zones := map[string]bool{}
			for z := range usage[b.ID] {
				zones[z] = true
			}
			for k := range l.states {
				if k.bucket == b.ID && k.zone != "" {
					zones[k.zone] = true
				}
			}
			for z := range zones {
				var t Tiers
				if zt := usage[b.ID][z]; zt != nil {
					t = *zt
				}
				res.ByZone[z] = append(res.ByZone[z], l.step(stateKey{bucket: b.ID, zone: z}, t, limit))
			}

		case b.Scope == policy.ScopeGlobal && l.Global != nil:
			res.Shared = append(res.Shared, l.fromGlobal(b, total(b.ID)))

		default:
			res.Shared = append(res.Shared, l.step(stateKey{bucket: b.ID}, total(b.ID), limit))
		}
	}
	return res
}

func (l *Loop) step(k stateKey, t Tiers, limit float64) *rlv1.BucketDirective {
	s, ok := l.states[k]
	if !ok {
		s = &BucketState{}
		l.states[k] = s
	}
	p, tiers := s.StepTiered(t, limit, l.Params)
	return directive(k.bucket, p, tiers, s.Smoothed())
}

func directive(id uint32, p float64, tiers Tiers, scopeRPS float64) *rlv1.BucketDirective {
	d := &rlv1.BucketDirective{BucketId: id, DropPpm: ToPPM(p), ScopeOfferedRps: scopeRPS}
	for prio := rlv1.Priority_PRIORITY_LOW; prio <= rlv1.Priority_PRIORITY_HIGH; prio++ {
		if p > 0 {
			d.Tiers = append(d.Tiers, &rlv1.TierDirective{Priority: prio, DropPpm: ToPPM(tiers[prio])})
		}
	}
	return d
}

func (l *Loop) fromGlobal(b *policy.Bucket, local Tiers) *rlv1.BucketDirective {
	var localRPS float64
	for _, r := range local[1:] {
		localRPS += r
	}
	if g, ok := l.Global.Bucket(b.ID); ok {
		if g.GetScopeOfferedRps() > 0 {
			l.shares[b.ID] = min(localRPS/g.GetScopeOfferedRps(), 1)
		}

		l.states[stateKey{bucket: b.ID}] = seeded(localRPS, float64(g.GetDropPpm())/1e6)
		return g
	}
	share, ok := l.shares[b.ID]
	if !ok || share == 0 {
		return &rlv1.BucketDirective{BucketId: b.ID}
	}
	return l.step(stateKey{bucket: b.ID}, local, float64(b.LimitRPS)*share)
}

// GlobalReport returns the region's usage of global buckets for the global tier.
func (l *Loop) GlobalReport(nodeID, region string, start, end time.Time) *rlv1.UsageReport {
	l.mu.Lock()
	rates, table := l.rates, l.table
	l.mu.Unlock()
	if table == nil {
		return hub.Report(nodeID, region, nil, start, end, nil)
	}
	return hub.Report(nodeID, region, rates, start, end, func(id uint32) bool {
		b := table.Bucket(id)
		return b != nil && b.Scope == policy.ScopeGlobal
	})
}

// BucketName returns a bucket's name for metrics, or its id if unknown.
func (l *Loop) BucketName(id uint32) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.table != nil {
		if b := l.table.Bucket(id); b != nil {
			return b.Name
		}
	}
	return strconv.FormatUint(uint64(id), 16)
}
