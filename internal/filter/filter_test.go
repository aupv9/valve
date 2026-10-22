package filter

import (
	"strings"
	"testing"
	"time"

	"github.com/aupv9/valve/internal/policy"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

const testPolicy = `
buckets:
  - name: checkout-from-web
    match: {caller: web-gateway, callee: checkout}
    limit_rps: 1000
    mode: enforce
  - name: checkout-shadow
    match: {callee: checkout}
    limit_rps: 1000
`

func setup(t *testing.T) (*Filter, *policy.Table) {
	t.Helper()
	pf, err := policy.Parse(strings.NewReader(testPolicy))
	if err != nil {
		t.Fatal(err)
	}
	tbl := policy.Compile(pf, 1)
	f := New()
	f.SetTable(tbl)
	return f, tbl
}

func directive(id uint32, ppm uint32) *rlv1.Directive {
	return &rlv1.Directive{TtlMs: 3000, Buckets: []*rlv1.BucketDirective{{BucketId: id, DropPpm: ppm}}}
}

var webReq = policy.Request{Caller: "web-gateway", Callee: "checkout", Endpoint: "CreateOrder"}

func TestDropRatioApproximate(t *testing.T) {
	f, tbl := setup(t)
	id := tbl.Match(webReq).ID
	now := time.Now()
	f.Apply(directive(id, 333_333), now)

	const n = 200_000
	rejected := 0
	for range n {
		if f.Decide(webReq) == Reject429 {
			rejected++
		}
	}
	if got := float64(rejected) / n; got < 0.32 || got > 0.345 {
		t.Fatalf("drop fraction = %.3f, want ~0.333", got)
	}

	rep := f.Snapshot("n1", "z1", now)
	if len(rep.Buckets) != 1 || rep.Buckets[0].Offered != n || rep.Buckets[0].Dropped != uint64(rejected) {
		t.Fatalf("snapshot = %v, want offered=%d dropped=%d", rep.Buckets, n, rejected)
	}
	if rep = f.Snapshot("n1", "z1", now); len(rep.Buckets) != 0 {
		t.Fatalf("second snapshot should be empty, got %v", rep.Buckets)
	}
}

func TestShadowAndKillSwitchNeverReject(t *testing.T) {
	f, tbl := setup(t)
	now := time.Now()
	shadowReq := policy.Request{Caller: "mobile", Callee: "checkout"}
	f.Apply(directive(tbl.Match(shadowReq).ID, 1_000_000), now)
	f.Apply(directive(tbl.Match(webReq).ID, 1_000_000), now)

	if f.Decide(shadowReq) != Allow {
		t.Fatal("shadow bucket rejected a request")
	}
	if f.Decide(webReq) != Reject429 {
		t.Fatal("enforce bucket at ratio 1 allowed a request")
	}
	f.SetKillSwitch(true)
	if f.Decide(webReq) != Allow {
		t.Fatal("kill switch did not force shadow")
	}
}

func TestTTLFailsOpen(t *testing.T) {
	f, tbl := setup(t)
	now := time.Now()
	f.Apply(directive(tbl.Match(webReq).ID, 1_000_000), now)
	if n := f.ExpireStale(now.Add(time.Second)); n != 0 {
		t.Fatalf("expired %d before TTL", n)
	}
	if n := f.ExpireStale(now.Add(4 * time.Second)); n != 1 {
		t.Fatalf("expired %d after TTL, want 1", n)
	}
	if f.Decide(webReq) != Allow {
		t.Fatal("request rejected after directive expired")
	}
}

func TestUnmatchedAllowed(t *testing.T) {
	f, _ := setup(t)
	if f.Decide(policy.Request{Callee: "payments"}) != Allow {
		t.Fatal("unmatched request rejected")
	}
}

func BenchmarkDecide(b *testing.B) {
	pf, _ := policy.Parse(strings.NewReader(testPolicy))
	tbl := policy.Compile(pf, 1)
	f := New()
	f.SetTable(tbl)
	f.Apply(directive(tbl.Match(webReq).ID, 100_000), time.Now())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f.Decide(webReq)
		}
	})
}
