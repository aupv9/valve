// Command agent is the per-host sidecar agent: it keeps a stream to the zone
// aggregator, reports usage and applies drop ratios.
//
// TODO(phase 1): read offered/dropped counters from Envoy admin stats and
// push drop ratios to Envoy via RTDS so the fault filter aborts with 429.
// Until then it runs the in-process filter with a synthetic load generator,
// which is enough to exercise the full control loop locally.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aupv9/valve/internal/admin"
	"github.com/aupv9/valve/internal/filter"
	"github.com/aupv9/valve/internal/metrics"
	"github.com/aupv9/valve/internal/policy"
	"github.com/aupv9/valve/internal/tlsutil"
	"github.com/aupv9/valve/internal/upstream"
	"github.com/aupv9/valve/internal/xds"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("agent failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	aggAddr := flag.String("aggregator", "localhost:9090", "zone aggregator address")
	policyPath := flag.String("policy", "policies", "policy file or directory (ignored with -xds)")
	xdsTarget := flag.String("xds", "", "configurator xDS target; policies and the kill switch come from it")
	nodeID := flag.String("node-id", hostname(), "this host's id")
	zone := flag.String("zone", "local", "zone name")
	interval := flag.Duration("interval", time.Second, "report cycle")
	loadRPS := flag.Int("load-rps", 0, "synthetic requests/second (0 = off)")
	loadReq := flag.String("load-request", "", "synthetic request as caller/callee/endpoint[/priority] (default: the first bucket's match)")
	adminAddr := flag.String("admin", ":9102", "admin HTTP address for /metrics, /healthz, /readyz (empty = off)")
	tlsCfg := tlsutil.RegisterFlags(flag.CommandLine)
	flag.Parse()

	clientCreds, err := tlsCfg.ClientCredentials()
	if err != nil {
		return err
	}
	reg := admin.NewRegistry()
	m := metrics.NewAgent(reg)

	f := filter.New() // allows everything until a policy arrives
	pf := &policy.File{}
	if *xdsTarget == "" {
		if pf, err = policy.LoadPaths(*policyPath); err != nil {
			return fmt.Errorf("load policy: %w", err)
		}
		f.SetTable(policy.Compile(pf, policy.Version(pf.ToProto())))
	}

	conn, err := upstream.Dial(*aggAddr, clientCreds)
	if err != nil {
		return fmt.Errorf("dial aggregator: %w", err)
	}
	defer conn.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *xdsTarget != "" {
		xconn, err := upstream.Dial(*xdsTarget, clientCreds)
		if err != nil {
			return fmt.Errorf("dial xds: %w", err)
		}
		defer xconn.Close()
		xds.Watch(ctx, xconn, *nodeID, log.With("xds", *xdsTarget),
			func(p *policy.File, version uint64) error {
				f.SetTable(policy.Compile(p, version))
				return nil
			},
			func(ks *rlv1.KillSwitch) { f.SetKillSwitch(ks.GetEnabled()) })
	}

	if *loadRPS > 0 {
		req, err := loadRequest(*loadReq, pf)
		if err != nil {
			return fmt.Errorf("synthetic load: %w", err)
		}
		go generateLoad(ctx, f, req, *loadRPS, log)
	}

	admin.Serve(ctx, *adminAddr, admin.Mux(reg, nil), log)

	log.Info("agent started", "aggregator", *aggAddr, "buckets", len(pf.Buckets), "xds", *xdsTarget, "mtls", tlsCfg.Enabled())
	upstream.Run(ctx, conn, upstream.Config{
		Interval: *interval,
		Report: func() *rlv1.UsageReport {
			now := time.Now()
			r, detail := f.Snapshot(*nodeID, *zone, now)
			m.Observe(detail, f.LastDirective(), now, f.KillSwitch(), f.Version())
			return r
		},
		OnDirective: func(d *rlv1.Directive) { f.Apply(d, time.Now()) },
		OnTick: func(now time.Time) {
			if n := f.ExpireStale(now); n > 0 {
				m.FailOpen.Add(float64(n))
				log.Warn("directive TTL expired, failing open", "buckets", n)
			}
		},
		OnState: func(up bool) { m.Connected.Set(metrics.Bool(up)) },
		Log:     log,
	})
	return nil
}

func loadRequest(spec string, pf *policy.File) (policy.Request, error) {
	if spec == "" {
		if len(pf.Buckets) == 0 {
			return policy.Request{}, fmt.Errorf("-load-request is required when policies come from xDS")
		}
		m := pf.Buckets[0].Match
		return policy.Request{Caller: m.Caller, Callee: m.Callee, Endpoint: m.Endpoint}, nil
	}
	parts := strings.Split(spec, "/")
	if len(parts) < 3 || len(parts) > 4 {
		return policy.Request{}, fmt.Errorf("-load-request %q: want caller/callee/endpoint[/priority]", spec)
	}
	req := policy.Request{Caller: parts[0], Callee: parts[1], Endpoint: parts[2]}
	if len(parts) == 4 {
		if req.Priority = policy.ParsePriority(parts[3]); req.Priority == "" {
			return policy.Request{}, fmt.Errorf("-load-request: unknown priority %q", parts[3])
		}
	}
	return req, nil
}

func generateLoad(ctx context.Context, f *filter.Filter, req policy.Request, rps int, log *slog.Logger) {
	const batches = 100 // spread each second over 10ms batches
	t := time.NewTicker(time.Second / batches)
	defer t.Stop()
	var allowed, rejected, tick int
	for {
		select {
		case <-t.C:
			for range rps / batches {
				if f.Decide(req) == filter.Allow {
					allowed++
				} else {
					rejected++
				}
			}
			if tick++; tick%batches == 0 {
				log.Info("synthetic load", "allowed", allowed, "rejected", rejected)
				allowed, rejected = 0, 0
			}
		case <-ctx.Done():
			return
		}
	}
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
