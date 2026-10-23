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

func setup(t testing.TB) (*Filter, *policy.Table) {
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

func reject(f *Filter, req policy.Request, n int) int {
	rejected := 0
	for range n {
		if f.Decide(req) == Reject429 {
			rejected++
		}
	}
	return rejected
}

func TestDropRatioApproximate(t *testing.T) {
	f, tbl := setup(t)
	id := tbl.Match(webReq).ID
	now := time.Now()
	f.Apply(directive(id, 333_333), now)

	const n = 200_000
	rejected := reject(f, webReq, n)
	if got := float64(rejected) / n; got < 0.32 || got > 0.345 {
		t.Fatalf("drop fraction = %.3f, want ~0.333", got)
	}

	rep, _ := f.Snapshot("n1", "z1", now.Add(time.Second))
	if len(rep.Buckets) != 1 || rep.Buckets[0].Offered != n || rep.Buckets[0].Dropped != uint64(rejected) {
		t.Fatalf("snapshot = %v, want offered=%d dropped=%d", rep.Buckets, n, rejected)
	}
	if rep.Buckets[0].Priority != rlv1.Priority_PRIORITY_MEDIUM {
		t.Fatalf("priority = %v, want bucket default medium", rep.Buckets[0].Priority)
	}
	if rep.WindowEndUnixMs <= rep.WindowStartUnixMs {
		t.Fatalf("bad window %d..%d", rep.WindowStartUnixMs, rep.WindowEndUnixMs)
	}
	if rep, _ = f.Snapshot("n1", "z1", now.Add(2*time.Second)); len(rep.Buckets) != 0 {
		t.Fatalf("second snapshot should be empty, got %v", rep.Buckets)
	}
}

func TestTierRatios(t *testing.T) {
	f, tbl := setup(t)
	id := tbl.Match(webReq).ID
	f.Apply(&rlv1.Directive{TtlMs: 3000, Buckets: []*rlv1.BucketDirective{{
		BucketId: id,
		DropPpm:  500_000,
		Tiers: []*rlv1.TierDirective{
			{Priority: rlv1.Priority_PRIORITY_LOW, DropPpm: 1_000_000},
			{Priority: rlv1.Priority_PRIORITY_HIGH, DropPpm: 0},
		},
	}}}, time.Now())

	low, high, med := webReq, webReq, webReq
	low.Priority, high.Priority = policy.PriorityLow, policy.PriorityHigh
	if got := reject(f, low, 1000); got != 1000 {
		t.Fatalf("low tier rejected %d/1000, want all", got)
	}
	if got := reject(f, high, 1000); got != 0 {
		t.Fatalf("high tier rejected %d/1000, want none", got)
	}
	// Medium is not listed, so it falls back to drop_ppm.
	if got := reject(f, med, 10_000); got < 4500 || got > 5500 {
		t.Fatalf("medium tier rejected %d/10000, want ~5000", got)
	}

	_, detail := f.Snapshot("n1", "z1", time.Now())
	byPrio := map[rlv1.Priority]TierUsage{}
	for _, u := range detail {
		if u.Bucket.ID == id {
			byPrio[u.Priority] = u
		}
	}
	if byPrio[rlv1.Priority_PRIORITY_LOW].Offered != 1000 || byPrio[rlv1.Priority_PRIORITY_HIGH].Offered != 1000 {
		t.Fatalf("per-tier offered not tracked: %+v", byPrio)
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
		t.Fatal("local kill switch did not force shadow")
	}
	f.SetKillSwitch(false)

	d := directive(tbl.Match(webReq).ID, 1_000_000)
	d.KillSwitch = true
	f.Apply(d, now)
	if f.Decide(webReq) != Allow || !f.KillSwitch() {
		t.Fatal("directive kill switch did not force shadow")
	}

	_, detail := f.Snapshot("n1", "z1", now)
	var shadow uint64
	for _, u := range detail {
		shadow += u.Shadow
	}
	if shadow != 3 { // shadow bucket, local kill switch, directive kill switch
		t.Fatalf("shadow would-drop count = %d, want 3", shadow)
	}
}

func TestTTLFailsOpen(t *testing.T) {
	f, tbl := setup(t)
	now := time.Now()
	d := directive(tbl.Match(webReq).ID, 1_000_000)
	d.KillSwitch = true
	f.Apply(d, now)
	if n := f.ExpireStale(now.Add(time.Second)); n != 0 {
		t.Fatalf("expired %d before TTL", n)
	}
	if n := f.ExpireStale(now.Add(4 * time.Second)); n != 1 {
		t.Fatalf("expired %d after TTL, want 1", n)
	}
	if f.Decide(webReq) != Allow {
		t.Fatal("request rejected after directive expired")
	}
	if f.KillSwitch() {
		t.Fatal("expired directive's kill switch still in effect")
	}
}

func TestUnmatchedAllowed(t *testing.T) {
	f, _ := setup(t)
	if f.Decide(policy.Request{Callee: "payments"}) != Allow {
		t.Fatal("unmatched request rejected")
	}
}

func BenchmarkDecide(b *testing.B) {
	f, tbl := setup(b)
	f.Apply(directive(tbl.Match(webReq).ID, 100_000), time.Now())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f.Decide(webReq)
		}
	})
}
