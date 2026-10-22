package controller

import (
	"sync"
	"time"

	"ratelimit/internal/hub"
	"ratelimit/internal/policy"

	rlv1 "ratelimit/gen/ratelimit/v1"
)

// Loop turns collected usage into directives once per cycle.
//
// TODO: leader election (etcd / Kubernetes lease), hash-sharding buckets
// across instances, and the regional -> global hop for scope=global buckets.
type Loop struct {
	Hub      *hub.Hub
	Params   Params
	Interval time.Duration
	TTL      time.Duration // directive lifetime downstream; > a few intervals

	mu     sync.Mutex
	table  *policy.Table
	states map[uint32]*BucketState
}

// SetTable installs a new policy version. State of surviving buckets is kept.
func (l *Loop) SetTable(t *policy.Table) {
	l.mu.Lock()
	defer l.mu.Unlock()
	next := make(map[uint32]*BucketState)
	for _, b := range t.Buckets() {
		if s, ok := l.states[b.ID]; ok {
			next[b.ID] = s
		} else {
			next[b.ID] = &BucketState{}
		}
	}
	l.table, l.states = t, next
}

// Tick runs one control cycle and broadcasts the resulting directive.
func (l *Loop) Tick() *rlv1.Directive {
	usage := l.Hub.Collect()
	secs := l.Interval.Seconds()

	l.mu.Lock()
	d := &rlv1.Directive{TtlMs: l.TTL.Milliseconds()}
	if l.table != nil {
		d.ConfigVersion = l.table.Version
		for id, s := range l.states {
			b := l.table.Bucket(id)
			p := s.Step(float64(usage[id].Offered)/secs, float64(b.LimitRPS), l.Params)
			d.Buckets = append(d.Buckets, &rlv1.BucketDirective{BucketId: id, DropPpm: ToPPM(p)})
		}
	}
	l.mu.Unlock()

	l.Hub.Broadcast(d)
	return d
}
