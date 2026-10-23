package envoy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	runtimev3 "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"

	"github.com/aupv9/valve/internal/policy"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

func parse(t *testing.T, src string) *policy.File {
	t.Helper()
	pf, err := policy.Parse(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	return pf
}

func TestConfigOrderAndExclusion(t *testing.T) {
	pf := parse(t, `
buckets:
  - {name: any, match: {callee: checkout}, limit_rps: 10}
  - {name: web, match: {caller: web, callee: checkout}, limit_rps: 10}
  - {name: pay, match: {callee: checkout, endpoint: /pay}, limit_rps: 10}
  - {name: other, match: {callee: payments}, limit_rps: 10}
`)
	frag, err := Config(pf, "checkout", CallerMatch{})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, vc := range frag.VirtualClusters {
		names = append(names, vc.GetName())
	}
	// Most specific first; the other service's bucket is left out.
	if got := strings.Join(names, ","); got != "valve_web,valve_pay,valve_any" {
		t.Fatalf("virtual clusters = %s", got)
	}
	// "any" excludes both narrower buckets with one inverted matcher each.
	anyVC := frag.VirtualClusters[2]
	if len(anyVC.GetHeaders()) != 2 || !anyVC.GetHeaders()[0].GetInvertMatch() || !anyVC.GetHeaders()[1].GetInvertMatch() {
		t.Fatalf("catch-all headers = %v", anyVC.GetHeaders())
	}
	if len(frag.Warnings) != 0 {
		t.Fatalf("unexpected warnings %v", frag.Warnings)
	}
	if p := frag.VirtualClusters[1].GetHeaders()[0].GetStringMatch().GetPrefix(); p != "/pay" {
		t.Fatalf("HTTP endpoint should match a path prefix, got %q", p)
	}
	if _, err := frag.YAML(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigWarnsOnTwoDimensionOverlap(t *testing.T) {
	pf := parse(t, `
buckets:
  - {name: any, match: {callee: checkout}, limit_rps: 10}
  - {name: web-create, match: {caller: web, callee: checkout, endpoint: CreateOrder}, limit_rps: 10}
`)
	frag, err := Config(pf, "checkout", CallerMatch{SPIFFE: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(frag.Warnings) != 1 {
		t.Fatalf("warnings = %v, want 1", frag.Warnings)
	}
	h := frag.VirtualClusters[0].GetHeaders()
	if h[0].GetName() != "x-forwarded-client-cert" || !strings.Contains(h[0].GetStringMatch().GetSafeRegex().GetRegex(), "/sa/web") {
		t.Fatalf("SPIFFE caller matcher = %v", h[0])
	}
	if h[1].GetStringMatch().GetSuffix() != "/CreateOrder" {
		t.Fatalf("gRPC endpoint should match a method suffix, got %v", h[1])
	}
}

// fakeEnvoy serves /stats with a settable counter.
type fakeEnvoy struct {
	mu    sync.Mutex
	total float64
}

func (f *fakeEnvoy) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fmt.Fprintf(w, `{"stats":[{"name":"vhost.checkout.vcluster.valve_web.upstream_rq_total","value":%v},
		{"name":"vhost.checkout.vcluster.valve_web.upstream_rq_time","histograms":{}}]}`, f.total)
}

func (f *fakeEnvoy) add(n float64) {
	f.mu.Lock()
	f.total += n
	f.mu.Unlock()
}

// runtimeLayer returns the runtime layer currently published over RTDS.
func runtimeLayer(t *testing.T, d *DataPlane) map[string]float64 {
	t.Helper()
	res, ok := d.rtds.GetResources()[RuntimeLayer]
	if !ok {
		t.Fatal("no runtime layer published")
	}
	rt := res.(*runtimev3.Runtime)
	if rt.GetName() != RuntimeLayer {
		t.Fatalf("layer name %q", rt.GetName())
	}
	out := map[string]float64{}
	for k, v := range rt.GetLayer().GetFields() {
		fp := v.GetStructValue().GetFields()
		if fp["denominator"].GetStringValue() != "MILLION" {
			t.Fatalf("%s: denominator %v, want MILLION", k, fp["denominator"])
		}
		out[k] = fp["numerator"].GetNumberValue()
	}
	return out
}

func TestDataPlane(t *testing.T) {
	fe := &fakeEnvoy{}
	srv := httptest.NewServer(fe)
	defer srv.Close()

	d := NewDataPlane(context.Background(), srv.URL, "checkout", slog.New(slog.DiscardHandler))
	pf := parse(t, `
buckets:
  - {name: web, match: {caller: web, callee: checkout}, limit_rps: 10, mode: enforce}
  - {name: elsewhere, match: {callee: payments}, limit_rps: 10, mode: enforce}
`)
	tbl := policy.Compile(pf, 1)
	d.SetTable(tbl)
	web := tbl.Match(policy.Request{Caller: "web", Callee: "checkout"})

	now := time.Now()
	d.Snapshot("n", "z", now) // baseline

	d.Apply(&rlv1.Directive{TtlMs: 5000, Buckets: []*rlv1.BucketDirective{{BucketId: web.ID, DropPpm: 250_000}}}, now)
	layer := runtimeLayer(t, d)
	if layer[RuntimeKey("web")] != 250_000 || len(layer) != 1 {
		t.Fatalf("runtime layer = %v, want only web at 250000", layer)
	}

	// Envoy let 750 through at 25% abort: offered was ~1000.
	fe.add(750)
	r, detail := d.Snapshot("n", "z", now.Add(time.Second))
	if len(r.Buckets) != 1 || r.Buckets[0].Offered != 1000 || r.Buckets[0].Dropped != 250 {
		t.Fatalf("report = %v, want offered 1000 dropped 250", r.Buckets)
	}
	if detail[0].Shadow != 0 {
		t.Fatalf("enforced bucket reported shadow drops: %+v", detail[0])
	}

	// Kill switch: Envoy enforces nothing, would-drop is reported as shadow.
	d.SetKillSwitch(true)
	if runtimeLayer(t, d)[RuntimeKey("web")] != 0 {
		t.Fatal("kill switch did not zero the runtime key")
	}
	d.Snapshot("n", "z", now.Add(2*time.Second)) // window that still had 25%
	fe.add(1000)
	_, detail = d.Snapshot("n", "z", now.Add(3*time.Second))
	if detail[0].Offered != 1000 || detail[0].Shadow != 250 {
		t.Fatalf("kill switch window = %+v, want offered 1000 shadow 250", detail[0])
	}
	d.SetKillSwitch(false)

	// Directive TTL expiry fails open.
	if n := d.ExpireStale(now.Add(time.Minute)); n != 1 {
		t.Fatalf("expired %d buckets, want 1", n)
	}
	if runtimeLayer(t, d)[RuntimeKey("web")] != 0 {
		t.Fatal("expired directive still enforced")
	}
}
