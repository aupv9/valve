// Command agent is the per-host sidecar agent (phase 1, "direction 2" in the
// design doc): it keeps a stream to the zone aggregator, reports usage and
// applies drop ratios.
//
// TODO(phase 1): read offered/dropped counters from Envoy admin stats and
// push drop ratios to Envoy via RTDS so the fault filter aborts with 429.
// Until then it runs the in-process filter with a synthetic load generator,
// which is enough to exercise the full control loop locally.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aupv9/valve/internal/filter"
	"github.com/aupv9/valve/internal/policy"
	"github.com/aupv9/valve/internal/upstream"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

func main() {
	aggAddr := flag.String("aggregator", "localhost:9090", "zone aggregator address")
	policyPath := flag.String("policy", "policies", "policy file or directory (later: delivered over xDS)")
	nodeID := flag.String("node-id", hostname(), "this host's id")
	zone := flag.String("zone", "local", "zone name")
	interval := flag.Duration("interval", time.Second, "report cycle")
	loadRPS := flag.Int("load-rps", 0, "synthetic requests/second against the first bucket (0 = off)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	pf, err := policy.LoadPaths(*policyPath)
	if err != nil {
		log.Error("load policy", "err", err)
		os.Exit(1)
	}
	f := filter.New()
	f.SetTable(policy.Compile(pf, policy.Version(pf.ToProto())))

	conn, err := grpc.NewClient(*aggAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("dial aggregator", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *loadRPS > 0 && len(pf.Buckets) > 0 {
		go generateLoad(ctx, f, policy.Request{Caller: pf.Buckets[0].Match.Caller, Callee: pf.Buckets[0].Match.Callee, Endpoint: pf.Buckets[0].Match.Endpoint}, *loadRPS, log)
	}

	log.Info("agent started", "aggregator", *aggAddr, "buckets", len(pf.Buckets))
	upstream.Run(ctx, conn, upstream.Config{
		Interval: *interval,
		Report: func() *rlv1.UsageReport {
			r, _ := f.Snapshot(*nodeID, *zone, time.Now())
			return r
		},
		OnDirective: func(d *rlv1.Directive) { f.Apply(d, time.Now()) },
		OnTick: func(now time.Time) {
			if n := f.ExpireStale(now); n > 0 {
				log.Warn("directive TTL expired, failing open", "buckets", n)
			}
		},
		Log: log,
	})
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
