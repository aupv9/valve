// Command agent is the per-host sidecar agent: it keeps a stream to the zone
// aggregator, reports usage and applies drop ratios to a data plane.
//
//	-mode envoy      drive an Envoy sidecar: serve abort ratios over RTDS and
//	                 read counters from its admin API (phase 1 of the rollout)
//	-mode inprocess  decide in the agent's own filter; with -load-rps it
//	                 generates synthetic traffic to exercise the loop locally
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/aupv9/valve/internal/admin"
	"github.com/aupv9/valve/internal/envoy"
	"github.com/aupv9/valve/internal/filter"
	"github.com/aupv9/valve/internal/metrics"
	"github.com/aupv9/valve/internal/policy"
	"github.com/aupv9/valve/internal/tlsutil"
	"github.com/aupv9/valve/internal/upstream"
	"github.com/aupv9/valve/internal/xds"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

// dataPlane is what the agent drives: the in-process filter or Envoy.
type dataPlane interface {
	SetTable(*policy.Table)
	Version() uint64
	SetKillSwitch(bool)
	KillSwitch() bool
	Apply(*rlv1.Directive, time.Time)
	LastDirective() time.Time
	ExpireStale(time.Time) int
	Snapshot(nodeID, zone string, now time.Time) (*rlv1.UsageReport, []filter.TierUsage)
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("agent failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	mode := flag.String("mode", "inprocess", "envoy | inprocess")
	aggAddr := flag.String("aggregator", "localhost:9090", "zone aggregator address")
	policyPath := flag.String("policy", "policies", "policy file or directory (ignored with -xds)")
	xdsTarget := flag.String("xds", "", "configurator xDS target; policies and the kill switch come from it")
	nodeID := flag.String("node-id", hostname(), "this host's id")
	zone := flag.String("zone", "local", "zone name")
	interval := flag.Duration("interval", time.Second, "report cycle")
	adminAddr := flag.String("admin", ":9102", "admin HTTP address for /metrics, /healthz, /readyz (empty = off)")
	envoyAdmin := flag.String("envoy-admin", "http://127.0.0.1:9901", "Envoy admin API (-mode envoy)")
	rtdsListen := flag.String("rtds-listen", "127.0.0.1:18001", "RTDS gRPC address Envoy subscribes to (-mode envoy)")
	service := flag.String("service", "", "the callee the Envoy sidecar fronts (-mode envoy, required)")
	loadRPS := flag.Int("load-rps", 0, "synthetic requests/second (-mode inprocess; 0 = off)")
	loadReq := flag.String("load-request", "", "synthetic request as caller/callee/endpoint[/priority] (default: the first bucket's match)")
	tlsCfg := tlsutil.RegisterFlags(flag.CommandLine)
	flag.Parse()

	clientCreds, err := tlsCfg.ClientCredentials()
	if err != nil {
		return err
	}
	reg := admin.NewRegistry()
	m := metrics.NewAgent(reg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var dp dataPlane
	var inproc *filter.Filter
	switch *mode {
	case "inprocess":
		inproc = filter.New() // allows everything until a policy arrives
		dp = inproc
	case "envoy":
		if *service == "" {
			return fmt.Errorf("-service is required with -mode envoy")
		}
		e := envoy.NewDataPlane(ctx, *envoyAdmin, *service, log)
		// RTDS is plaintext on loopback: Envoy and the agent share the pod.
		lis, err := net.Listen("tcp", *rtdsListen)
		if err != nil {
			return fmt.Errorf("rtds listen: %w", err)
		}
		g := grpc.NewServer()
		e.Register(g)
		go g.Serve(lis) //nolint:errcheck // stopped on exit
		defer g.Stop()
		dp = e
	default:
		return fmt.Errorf("unknown -mode %q", *mode)
	}

	pf := &policy.File{}
	if *xdsTarget == "" {
		if pf, err = policy.LoadPaths(*policyPath); err != nil {
			return fmt.Errorf("load policy: %w", err)
		}
		dp.SetTable(policy.Compile(pf, policy.Version(pf.ToProto())))
	} else {
		xconn, err := upstream.Dial(*xdsTarget, clientCreds)
		if err != nil {
			return fmt.Errorf("dial xds: %w", err)
		}
		defer xconn.Close()
		xds.Watch(ctx, xconn, *nodeID, log.With("xds", *xdsTarget),
			func(p *policy.File, version uint64) error {
				dp.SetTable(policy.Compile(p, version))
				return nil
			},
			func(ks *rlv1.KillSwitch) { dp.SetKillSwitch(ks.GetEnabled()) })
	}

	if *loadRPS > 0 {
		if inproc == nil {
			return fmt.Errorf("-load-rps needs -mode inprocess")
		}
		req, err := loadRequest(*loadReq, pf)
		if err != nil {
			return fmt.Errorf("synthetic load: %w", err)
		}
		go generateLoad(ctx, inproc, req, *loadRPS, log)
	}

	conn, err := upstream.Dial(*aggAddr, clientCreds)
	if err != nil {
		return fmt.Errorf("dial aggregator: %w", err)
	}
	defer conn.Close()

	admin.Serve(ctx, *adminAddr, admin.Mux(reg, nil), log)
	log.Info("agent started", "mode", *mode, "aggregator", *aggAddr, "xds", *xdsTarget, "service", *service, "mtls", tlsCfg.Enabled())
	upstream.Run(ctx, conn, upstream.Config{
		Interval: *interval,
		Report: func() *rlv1.UsageReport {
			now := time.Now()
			r, detail := dp.Snapshot(*nodeID, *zone, now)
			m.Observe(detail, dp.LastDirective(), now, dp.KillSwitch(), dp.Version())
			return r
		},
		OnDirective: func(d *rlv1.Directive) { dp.Apply(d, time.Now()) },
		OnTick: func(now time.Time) {
			if n := dp.ExpireStale(now); n > 0 {
				m.FailOpen.Add(float64(n))
				log.Warn("directive TTL expired, failing open", "buckets", n)
			}
		},
		OnState: func(up bool) { m.Connected.Set(metrics.Bool(up)) },
		Log:     log,
	})
	return nil
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
