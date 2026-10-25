// Package metrics defines the Prometheus metrics of every Valve component.
//
// Request rates are exported as counters, per Prometheus convention; take
// rate() in queries.
// Per-bucket gauges are published as a full snapshot each cycle, so a bucket
// removed from the policy disappears instead of freezing at its last value.
package metrics

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aupv9/valve/internal/filter"
	"github.com/aupv9/valve/internal/policy"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

const ns = "valve"

var bucketLabels = []string{"bucket", "caller", "callee", "priority"}

func prio(p rlv1.Priority) string { return string(policy.PriorityFromProto(p)) }

// Snapshot is a collector of gauges replaced as a whole on every Set.
type Snapshot struct {
	mu      sync.Mutex
	metrics []prometheus.Metric
	descs   []*prometheus.Desc
}

func NewSnapshot(descs ...*prometheus.Desc) *Snapshot { return &Snapshot{descs: descs} }

func (s *Snapshot) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range s.descs {
		ch <- d
	}
}

func (s *Snapshot) Collect(ch chan<- prometheus.Metric) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.metrics {
		ch <- m
	}
}

// Set replaces every metric in the snapshot.
func (s *Snapshot) Set(ms []prometheus.Metric) {
	s.mu.Lock()
	s.metrics = ms
	s.mu.Unlock()
}

func gauge(d *prometheus.Desc, v float64, labels ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
}

// Common holds the metrics every data and control plane process exports.
type Common struct {
	KillSwitch prometheus.Gauge
	config     *Snapshot
	configDesc *prometheus.Desc
}

func newCommon(reg prometheus.Registerer) *Common {
	c := &Common{
		KillSwitch: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "kill_switch", Help: "1 while the global kill switch is in effect.",
		}),
		configDesc: prometheus.NewDesc(ns+"_config_info", "Policy version in use (always 1; compare the version label across hosts).", []string{"version"}, nil),
	}
	c.config = NewSnapshot(c.configDesc)
	reg.MustRegister(c.KillSwitch, c.config)
	return c
}

// SetConfigVersion publishes the policy version in use.
func (c *Common) SetConfigVersion(v uint64) {
	c.config.Set([]prometheus.Metric{gauge(c.configDesc, 1, strconv.FormatUint(v, 16))})
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Agent holds the agent's metrics.
type Agent struct {
	*Common
	Offered    *prometheus.CounterVec
	Rejected   *prometheus.CounterVec
	ShadowDrop *prometheus.CounterVec
	FailOpen   prometheus.Counter
	LoopLag    prometheus.Gauge
	Connected  prometheus.Gauge
	ratios     *Snapshot
	ratioDesc  *prometheus.Desc
}

func NewAgent(reg prometheus.Registerer) *Agent {
	counter := func(name, help string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help}, bucketLabels)
	}
	a := &Agent{
		Common:     newCommon(reg),
		Offered:    counter("requests_offered_total", "Requests matched to a bucket, including dropped ones (offered load)."),
		Rejected:   counter("requests_rejected_total", "Requests rejected with 429."),
		ShadowDrop: counter("requests_shadow_would_drop_total", "Requests sampled for dropping but let through (shadow mode or kill switch)."),
		FailOpen: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "fail_open_total", Help: "Buckets whose directive expired, so they stopped dropping.",
		}),
		LoopLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "control_loop_lag_seconds", Help: "Age of the newest directive applied on this host.",
		}),
		Connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "upstream_connected", Help: "1 while the stream to the aggregator is up.",
		}),
		ratioDesc: prometheus.NewDesc(ns+"_drop_ratio", "Drop ratio in effect for a bucket tier.", bucketLabels, nil),
	}
	a.ratios = NewSnapshot(a.ratioDesc)
	reg.MustRegister(a.Offered, a.Rejected, a.ShadowDrop, a.FailOpen, a.LoopLag, a.Connected, a.ratios)
	return a
}

// Observe records one reporting cycle of the filter.
func (a *Agent) Observe(detail []filter.TierUsage, lastDirective, now time.Time, killSwitch bool, version uint64) {
	ratios := make([]prometheus.Metric, 0, len(detail))
	for _, u := range detail {
		l := []string{u.Bucket.Name, u.Bucket.Match.Caller, u.Bucket.Match.Callee, prio(u.Priority)}
		if u.Offered > 0 {
			a.Offered.WithLabelValues(l...).Add(float64(u.Offered))
		}
		if rej := u.Dropped - u.Shadow; rej > 0 {
			a.Rejected.WithLabelValues(l...).Add(float64(rej))
		}
		if u.Shadow > 0 {
			a.ShadowDrop.WithLabelValues(l...).Add(float64(u.Shadow))
		}
		ratios = append(ratios, gauge(a.ratioDesc, float64(u.DropPPM)/1e6, l...))
	}
	a.ratios.Set(ratios)
	if !lastDirective.IsZero() {
		a.LoopLag.Set(now.Sub(lastDirective).Seconds())
	}
	a.KillSwitch.Set(boolGauge(killSwitch))
	a.SetConfigVersion(version)
}

// Controller holds the controller's metrics.
type Controller struct {
	*Common
	Leader     prometheus.Gauge
	Peers      prometheus.Gauge
	Tick       prometheus.Histogram
	buckets    *Snapshot
	offered    *prometheus.Desc
	ratio      *prometheus.Desc
	tierRatio  *prometheus.Desc
	GlobalConn *prometheus.GaugeVec
}

func NewController(reg prometheus.Registerer) *Controller {
	c := &Controller{
		Common: newCommon(reg),
		Leader: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "controller", Name: "is_leader", Help: "1 while this replica leads its shard."}),
		Peers:  prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "controller", Name: "peers", Help: "Connected downstream streams."}),
		Tick: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: "controller", Name: "tick_seconds", Help: "Time to compute one control cycle.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 4, 8),
		}),
		GlobalConn: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: "controller", Name: "global_connected", Help: "1 while the stream to a global tier shard is up.",
		}, []string{"target"}),
		offered:   prometheus.NewDesc(ns+"_controller_offered_rps", "Smoothed offered RPS over the bucket's scope.", []string{"bucket", "zone"}, nil),
		ratio:     prometheus.NewDesc(ns+"_controller_drop_ratio", "Drop ratio sent downstream for a bucket.", []string{"bucket", "zone"}, nil),
		tierRatio: prometheus.NewDesc(ns+"_controller_tier_drop_ratio", "Per-tier drop ratio sent downstream.", []string{"bucket", "zone", "priority"}, nil),
	}
	c.buckets = NewSnapshot(c.offered, c.ratio, c.tierRatio)
	reg.MustRegister(c.Leader, c.Peers, c.Tick, c.GlobalConn, c.buckets)
	return c
}

// ObserveBuckets publishes the directives of one cycle. name maps bucket ids
// to names; zone is "" for directives shared by every zone.
func (c *Controller) ObserveBuckets(shared []*rlv1.BucketDirective, byZone map[string][]*rlv1.BucketDirective, name func(uint32) string) {
	var ms []prometheus.Metric
	add := func(zone string, ds []*rlv1.BucketDirective) {
		for _, d := range ds {
			n := name(d.GetBucketId())
			ms = append(ms,
				gauge(c.offered, d.GetScopeOfferedRps(), n, zone),
				gauge(c.ratio, float64(d.GetDropPpm())/1e6, n, zone))
			for _, t := range d.GetTiers() {
				ms = append(ms, gauge(c.tierRatio, float64(t.GetDropPpm())/1e6, n, zone, prio(t.GetPriority())))
			}
		}
	}
	add("", shared)
	for z, ds := range byZone {
		add(z, ds)
	}
	c.buckets.Set(ms)
}

// Aggregator holds the aggregator's metrics.
type Aggregator struct {
	Peers     prometheus.Gauge
	Connected *prometheus.GaugeVec
}

func NewAggregator(reg prometheus.Registerer) *Aggregator {
	a := &Aggregator{
		Peers: prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "aggregator", Name: "peers", Help: "Connected sidecar streams."}),
		Connected: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: "aggregator", Name: "upstream_connected", Help: "1 while the stream to a controller shard is up.",
		}, []string{"target"}),
	}
	reg.MustRegister(a.Peers, a.Connected)
	return a
}

// Configurator holds the configurator's metrics.
type Configurator struct {
	*Common
	ReloadErrors prometheus.Counter
	Published    prometheus.Counter
}

func NewConfigurator(reg prometheus.Registerer) *Configurator {
	c := &Configurator{
		Common: newCommon(reg),
		ReloadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: "configurator", Name: "reload_errors_total", Help: "Policy changes rejected by validation.",
		}),
		Published: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: "configurator", Name: "published_total", Help: "Policy versions published.",
		}),
	}
	reg.MustRegister(c.ReloadErrors, c.Published)
	return c
}

// Bool converts a flag to a gauge value.
func Bool(b bool) float64 { return boolGauge(b) }
