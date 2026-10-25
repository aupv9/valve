package controller

import (
	"strings"
	"testing"
	"time"

	"github.com/aupv9/valve/internal/hub"
	"github.com/aupv9/valve/internal/policy"
	"github.com/aupv9/valve/internal/upstream"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

const loopPolicy = `
buckets:
  - name: zonal
    match: {callee: a}
    limit_rps: 100
    scope: zonal
  - name: regional
    match: {callee: b}
    limit_rps: 100
    scope: regional
  - name: global
    match: {callee: c}
    limit_rps: 100
    scope: global
`

func newLoop(t *testing.T) (*Loop, *policy.Table, func(node, zone string, bucket string, rps uint64)) {
	t.Helper()
	pf, err := policy.Parse(strings.NewReader(loopPolicy))
	if err != nil {
		t.Fatal(err)
	}
	tbl := policy.Compile(pf, 42)
	h := hub.New(time.Hour)
	l := &Loop{Hub: h, Params: DefaultParams, Interval: time.Second, TTL: 5 * time.Second, Tier: TierRegional}
	l.SetTable(tbl)
	feed := func(node, zone, bucket string, rps uint64) {
		end := time.Now()
		hub.Inject(h, &rlv1.UsageReport{
			NodeId: node + "/" + bucket, Zone: zone,
			WindowStartUnixMs: end.Add(-time.Second).UnixMilli(), WindowEndUnixMs: end.UnixMilli(),
			Buckets: []*rlv1.BucketUsage{{BucketId: bucketID(tbl, bucket), Offered: rps}},
		})
	}
	return l, tbl, feed
}

func bucketID(tbl *policy.Table, name string) uint32 {
	for _, b := range tbl.Buckets() {
		if b.Name == name {
			return b.ID
		}
	}
	panic(name)
}

func ppm(res *Result, zone string, id uint32) uint32 {
	for _, b := range res.Directive(zone, time.Second).Buckets {
		if b.BucketId == id {
			return b.DropPpm
		}
	}
	return 0
}

func run(l *Loop, n int) *Result {
	var r *Result
	for range n {
		r = l.Tick()
	}
	return r
}

func TestScopes(t *testing.T) {
	l, tbl, feed := newLoop(t)

	for _, b := range []string{"zonal", "regional", "global"} {
		feed("n1", "z1", b, 150)
		feed("n2", "z2", b, 50)
	}
	res := run(l, 30)

	if got := ppm(res, "z1", bucketID(tbl, "zonal")); got < 330_000 || got > 337_000 {
		t.Fatalf("zonal z1 ppm = %d, want ~333333", got)
	}
	if got := ppm(res, "z2", bucketID(tbl, "zonal")); got != 0 {
		t.Fatalf("zonal z2 ppm = %d, want 0", got)
	}

	for _, name := range []string{"regional", "global"} {
		for _, z := range []string{"z1", "z2"} {
			if got := ppm(res, z, bucketID(tbl, name)); got < 495_000 || got > 505_000 {
				t.Fatalf("%s %s ppm = %d, want ~500000", name, z, got)
			}
		}
	}
	if res.Version != 42 {
		t.Fatalf("version = %d", res.Version)
	}
}

func TestSharding(t *testing.T) {
	l, tbl, feed := newLoop(t)
	keep := bucketID(tbl, "regional")
	l.Owns = func(id uint32) bool { return id == keep }
	feed("n1", "z1", "regional", 500)
	feed("n1", "z1", "global", 500)
	res := run(l, 5)
	if len(res.Shared) != 1 || res.Shared[0].BucketId != keep {
		t.Fatalf("shard computed %v, want only bucket %d", res.Shared, keep)
	}
}

func TestGlobalRelayAndFallback(t *testing.T) {
	l, tbl, feed := newLoop(t)
	g := upstream.NewMerger()
	l.Global = g
	id := bucketID(tbl, "global")
	feed("n1", "z1", "global", 300)

	g.Set(0, &rlv1.Directive{TtlMs: 60_000, Buckets: []*rlv1.BucketDirective{{BucketId: id, DropPpm: 400_000, ScopeOfferedRps: 1000}}})
	if got := ppm(run(l, 1), "z1", id); got != 400_000 {
		t.Fatalf("relayed ppm = %d, want 400000", got)
	}

	g2 := upstream.NewMerger()
	l.Global = g2
	if got := ppm(run(l, 40), "z1", id); got != uint32(DefaultParams.MaxDrop*1e6) {
		t.Fatalf("fallback ppm = %d, want clamp at MaxDrop (target 0.9)", got)
	}

	l2, tbl2, feed2 := newLoop(t)
	l2.Global = upstream.NewMerger()
	feed2("n1", "z1", "global", 300)
	if got := ppm(run(l2, 10), "z1", bucketID(tbl2, "global")); got != 0 {
		t.Fatalf("no share known: ppm = %d, want fail-open 0", got)
	}
}

func TestGlobalTierAndReport(t *testing.T) {
	l, tbl, feed := newLoop(t)
	l.Tier = TierGlobal
	feed("eu", "eu", "global", 120)
	feed("us", "us", "global", 80)
	feed("eu", "eu", "regional", 1000)
	res := run(l, 30)
	if len(res.Shared) != 1 || res.Shared[0].BucketId != bucketID(tbl, "global") {
		t.Fatalf("global tier computed %v", res.Shared)
	}
	if d := res.Shared[0]; d.DropPpm < 495_000 || d.ScopeOfferedRps < 199 || d.ScopeOfferedRps > 201 {
		t.Fatalf("global directive = %v, want ~50%% and scope ~200 rps", d)
	}

	r, _, feedR := newLoop(t)
	feedR("n1", "z1", "global", 10)
	feedR("n1", "z1", "regional", 10)
	r.Tick()
	end := time.Now()
	rep := r.GlobalReport("eu-ctl", "eu", end.Add(-time.Second), end)
	if len(rep.Buckets) != 1 || rep.Zone != "eu" {
		t.Fatalf("global report = %v", rep)
	}
}

func TestKillSwitch(t *testing.T) {
	l, _, _ := newLoop(t)
	l.SetKillSwitch(true)
	if !l.Tick().Kill {
		t.Fatal("local kill switch not in result")
	}
	l.SetKillSwitch(false)
	g := upstream.NewMerger()
	g.Set(0, &rlv1.Directive{TtlMs: 60_000, KillSwitch: true})
	l.Global = g
	if !l.Tick().Kill {
		t.Fatal("global tier kill switch not relayed")
	}
}
