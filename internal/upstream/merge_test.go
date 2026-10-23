package upstream

import (
	"testing"
	"time"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

func TestMergerUnionAndExpiry(t *testing.T) {
	now := time.Now()
	m := NewMerger()
	m.now = func() time.Time { return now }

	m.Set(0, &rlv1.Directive{ConfigVersion: 1, TtlMs: 5000, Buckets: []*rlv1.BucketDirective{{BucketId: 1, DropPpm: 10}}})
	now = now.Add(3 * time.Second)
	m.Set(1, &rlv1.Directive{ConfigVersion: 2, TtlMs: 5000, KillSwitch: true, Buckets: []*rlv1.BucketDirective{{BucketId: 2, DropPpm: 20}}})

	d := m.Merged()
	if len(d.Buckets) != 2 || !d.KillSwitch || d.ConfigVersion != 2 {
		t.Fatalf("merged = %v", d)
	}
	if d.TtlMs != 2000 { // shard 0 has 2s left
		t.Fatalf("ttl = %d, want 2000", d.TtlMs)
	}

	// Shard 0 expires: its bucket disappears instead of being refreshed.
	now = now.Add(3 * time.Second)
	if _, ok := m.Bucket(1); ok {
		t.Fatal("expired shard's bucket still merged")
	}
	if b, ok := m.Bucket(2); !ok || b.DropPpm != 20 {
		t.Fatal("live shard's bucket missing")
	}

	now = now.Add(10 * time.Second)
	if m.Merged() != nil {
		t.Fatal("all expired, want nil")
	}
}
