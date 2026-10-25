package upstream

import (
	"sync"
	"time"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

// Merger combines directives from controller shards, each live until its own TTL.
type Merger struct {
	now func() time.Time

	mu   sync.Mutex
	last map[int]received
}

type received struct {
	d  *rlv1.Directive
	at time.Time
}

func NewMerger() *Merger {
	return &Merger{now: time.Now, last: map[int]received{}}
}

// Set records the latest directive from source i.
func (m *Merger) Set(i int, d *rlv1.Directive) {
	m.mu.Lock()
	m.last[i] = received{d: d, at: m.now()}
	m.mu.Unlock()
}

// Merged returns the union of all live directives, or nil if none is live.
func (m *Merger) Merged() *rlv1.Directive {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var out *rlv1.Directive
	for _, r := range m.last {
		remaining := r.at.Add(time.Duration(r.d.GetTtlMs()) * time.Millisecond).Sub(now)
		if remaining <= 0 {
			continue
		}
		if out == nil {
			out = &rlv1.Directive{TtlMs: remaining.Milliseconds()}
		}
		out.TtlMs = min(out.TtlMs, remaining.Milliseconds())
		out.ConfigVersion = max(out.ConfigVersion, r.d.GetConfigVersion())
		out.KillSwitch = out.KillSwitch || r.d.GetKillSwitch()
		out.Buckets = append(out.Buckets, r.d.GetBuckets()...)
	}
	return out
}

// Bucket returns the live directive for one bucket, if any source has it.
func (m *Merger) Bucket(id uint32) (*rlv1.BucketDirective, bool) {
	d := m.Merged()
	for _, b := range d.GetBuckets() {
		if b.GetBucketId() == id {
			return b, true
		}
	}
	return nil, false
}
