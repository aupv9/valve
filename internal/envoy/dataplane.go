package envoy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"regexp"
	"sync"
	"time"

	runtimev3 "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/aupv9/valve/internal/filter"
	"github.com/aupv9/valve/internal/policy"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

// DataPlane drives an Envoy sidecar. It has the same surface as the
// in-process filter, so the agent's sync loop works with either.
//
// Envoy only counts requests that reach the router, i.e. the ones let
// through. Offered load is estimated as allowed / (1 − p), where p is the
// abort ratio Envoy applied during the window. p never exceeds the
// controller's clamp (0.9 by default), so the estimate stays bounded.
type DataPlane struct {
	admin   string // Envoy admin base URL
	service string // only buckets whose callee is this service are driven
	client  *http.Client
	rtds    *cache.LinearCache
	xds     server.Server
	log     *slog.Logger

	mu            sync.Mutex
	table         *policy.Table
	buckets       map[string]*policy.Bucket // by name, local service only
	target        map[uint32]uint32         // controller's ratio (ppm)
	expires       map[uint32]time.Time
	applied       map[string]uint32 // what Envoy has, by bucket name
	published     bool
	killLocal     bool
	killRemote    bool
	lastDirective time.Time
	lastTotals    map[string]float64 // virtual cluster counters at last snapshot
	windowPPM     map[string]uint32  // ratio applied during the current window
	lastSnap      time.Time
}

// NewDataPlane returns a data plane for the Envoy whose admin API is at
// adminURL (for example http://127.0.0.1:9901).
func NewDataPlane(ctx context.Context, adminURL, service string, log *slog.Logger) *DataPlane {
	d := &DataPlane{
		admin:      adminURL,
		service:    service,
		client:     &http.Client{Timeout: 2 * time.Second},
		rtds:       cache.NewLinearCache(resource.RuntimeType),
		log:        log,
		table:      policy.Compile(&policy.File{}, 0),
		buckets:    map[string]*policy.Bucket{},
		target:     map[uint32]uint32{},
		expires:    map[uint32]time.Time{},
		applied:    map[string]uint32{},
		lastTotals: map[string]float64{},
		windowPPM:  map[string]uint32{},
		lastSnap:   time.Now(),
	}
	d.xds = server.NewServer(ctx, d.rtds, server.CallbackFuncs{})
	d.mu.Lock()
	d.pushLocked()
	d.mu.Unlock()
	return d
}

// Register adds the RTDS service Envoy subscribes to.
func (d *DataPlane) Register(g *grpc.Server) {
	runtimev3.RegisterRuntimeDiscoveryServiceServer(g, d.xds)
}

func (d *DataPlane) SetTable(t *policy.Table) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.table = t
	d.buckets = map[string]*policy.Bucket{}
	for _, b := range t.Buckets() {
		if b.Match.Callee == d.service {
			d.buckets[b.Name] = b
		}
	}
	d.pushLocked()
}

func (d *DataPlane) Version() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.table.Version
}

func (d *DataPlane) SetKillSwitch(on bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.killLocal = on
	d.pushLocked()
}

func (d *DataPlane) KillSwitch() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.killLocal || d.killRemote
}

// Apply records a directive and pushes the resulting abort ratios to Envoy.
// Envoy has no priority tiers here, so the bucket's overall ratio is used.
func (d *DataPlane) Apply(dir *rlv1.Directive, now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.killRemote = dir.GetKillSwitch()
	d.lastDirective = now
	exp := now.Add(time.Duration(dir.GetTtlMs()) * time.Millisecond)
	for _, bd := range dir.GetBuckets() {
		d.target[bd.GetBucketId()] = min(bd.GetDropPpm(), 1_000_000)
		d.expires[bd.GetBucketId()] = exp
	}
	d.pushLocked()
}

func (d *DataPlane) LastDirective() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastDirective
}

// ExpireStale fails open: ratios whose directive expired go back to zero.
func (d *DataPlane) ExpireStale(now time.Time) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for id, exp := range d.expires {
		if now.After(exp) && d.target[id] != 0 {
			d.target[id] = 0
			n++
		}
	}
	if n > 0 {
		d.killRemote = false
		d.pushLocked()
	}
	return n
}

// pushLocked computes what Envoy should enforce and publishes it over RTDS
// if it changed. Shadow buckets and the kill switch enforce nothing.
func (d *DataPlane) pushLocked() {
	want := map[string]uint32{}
	for name, b := range d.buckets {
		p := d.target[b.ID]
		if d.killLocal || d.killRemote || b.Mode == policy.ModeShadow {
			p = 0
		}
		want[name] = p
	}
	// Always publish once: Envoy waits for every RTDS layer before it
	// finishes initializing.
	if d.published && maps.Equal(want, d.applied) {
		return
	}
	// A plain integer runtime value is read by Envoy as a percent out of 100
	// (anything above 100 means always), so ratios go in as a
	// FractionalPercent struct with a MILLION denominator.
	fields := map[string]any{}
	for name, p := range want {
		fields[RuntimeKey(name)] = map[string]any{"numerator": float64(p), "denominator": "MILLION"}
	}
	layer, err := structpb.NewStruct(fields)
	if err != nil {
		d.log.Error("build runtime layer", "err", err)
		return
	}
	if err := d.rtds.UpdateResource(RuntimeLayer, &runtimev3.Runtime{Name: RuntimeLayer, Layer: layer}); err != nil {
		d.log.Error("publish runtime layer", "err", err)
		return
	}
	// A ratio raised mid-window counts for the rest of it; take the higher
	// one so the estimate does not under-count offered load.
	for name, p := range want {
		d.windowPPM[name] = max(d.windowPPM[name], p)
	}
	d.applied = want
	d.published = true
}

var vclusterStat = regexp.MustCompile(`^vhost\.[^.]+\.vcluster\.` + VirtualClusterPrefix + `([A-Za-z0-9_-]+)\.upstream_rq_total$`)

type statsJSON struct {
	Stats []struct {
		Name  string          `json:"name"`
		Value json.RawMessage `json:"value"`
	} `json:"stats"`
}

func (d *DataPlane) fetchTotals() (map[string]float64, error) {
	resp, err := d.client.Get(d.admin + "/stats?format=json&usedonly&filter=" + VirtualClusterPrefix)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("envoy admin: %s", resp.Status)
	}
	var s statsJSON
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	totals := map[string]float64{}
	for _, st := range s.Stats {
		m := vclusterStat.FindStringSubmatch(st.Name)
		if m == nil {
			continue
		}
		var v float64
		if err := json.Unmarshal(st.Value, &v); err != nil {
			continue // histograms and the like
		}
		totals[m[1]] += v // the same bucket may appear under several vhosts
	}
	return totals, nil
}

// Snapshot reads Envoy's counters and turns the window's allowed requests
// into offered/dropped estimates.
func (d *DataPlane) Snapshot(nodeID, zone string, now time.Time) (*rlv1.UsageReport, []filter.TierUsage) {
	totals, err := d.fetchTotals()

	d.mu.Lock()
	defer d.mu.Unlock()
	r := &rlv1.UsageReport{
		NodeId: nodeID, Zone: zone,
		WindowStartUnixMs: d.lastSnap.UnixMilli(), WindowEndUnixMs: now.UnixMilli(),
	}
	if err != nil {
		// Report nothing for this window; the next one covers the gap.
		d.log.Warn("read envoy stats", "err", err)
		return &rlv1.UsageReport{NodeId: nodeID, Zone: zone}, nil
	}
	d.lastSnap = now

	var detail []filter.TierUsage
	for name, b := range d.buckets {
		total := totals[name]
		allowed := total - d.lastTotals[name]
		if allowed < 0 { // Envoy restarted
			allowed = total
		}
		d.lastTotals[name] = total

		p := float64(d.windowPPM[name]) / 1e6
		offered := allowed
		if p > 0 && p < 1 {
			offered = allowed / (1 - p)
		}
		enforced := offered - allowed
		// Shadow and kill switch: Envoy enforced nothing; report what the
		// controller's ratio would have dropped.
		var shadow float64
		if target := d.target[b.ID]; d.applied[name] == 0 && target > 0 {
			shadow = offered * float64(target) / 1e6
		}
		u := filter.TierUsage{
			Bucket:   b,
			Priority: b.Priority.Proto(),
			Offered:  uint64(math.Round(offered)),
			Dropped:  uint64(math.Round(enforced + shadow)),
			Shadow:   uint64(math.Round(shadow)),
			DropPPM:  d.target[b.ID],
		}
		detail = append(detail, u)
		if u.Offered > 0 {
			r.Buckets = append(r.Buckets, &rlv1.BucketUsage{
				BucketId: b.ID, Priority: u.Priority, Offered: u.Offered, Dropped: u.Dropped,
			})
		}
	}
	// The next window starts with whatever Envoy enforces now.
	d.windowPPM = maps.Clone(d.applied)
	return r, detail
}
