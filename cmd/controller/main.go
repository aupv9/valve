// Command controller computes drop ratios from aggregated usage and pushes
// them down. Run one per region (-tier regional) and optionally a global tier
// (-tier global) that regional controllers report to with -global.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aupv9/valve/internal/controller"
	"github.com/aupv9/valve/internal/hub"
	"github.com/aupv9/valve/internal/leader"
	"github.com/aupv9/valve/internal/policy"
	"github.com/aupv9/valve/internal/upstream"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("controller failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	listen := flag.String("listen", ":9091", "gRPC listen address for aggregators (or regional controllers)")
	policyPath := flag.String("policy", "policies", "policy file or directory")
	interval := flag.Duration("interval", time.Second, "control cycle")
	ttl := flag.Duration("ttl", 5*time.Second, "directive TTL downstream")
	tier := flag.String("tier", "regional", "regional | global")
	region := flag.String("region", "local", "region name (reported to the global tier)")
	global := flag.String("global", "", "comma-separated global controller targets, one per shard (regional tier only)")
	shard := flag.Int("shard", 0, "this instance's shard index")
	shards := flag.Int("shards", 1, "number of shards; buckets are split by id % shards")
	elect := flag.Bool("leader-elect", false, "elect a leader with a Kubernetes Lease (in-cluster)")
	leaseNS := flag.String("lease-namespace", envOr("POD_NAMESPACE", "default"), "namespace of the Lease")
	leaseName := flag.String("lease-name", "", "Lease name (default valve-controller-<tier>-<shard>)")
	identity := flag.String("identity", envOr("POD_NAME", hostname()), "this replica's identity for leader election")
	flag.Parse()

	t := controller.Tier(*tier)
	if t != controller.TierRegional && t != controller.TierGlobal {
		return fmt.Errorf("unknown -tier %q", *tier)
	}
	if *shards < 1 || *shard < 0 || *shard >= *shards {
		return fmt.Errorf("bad sharding -shard %d -shards %d", *shard, *shards)
	}

	pf, err := policy.LoadPaths(*policyPath)
	if err != nil {
		return fmt.Errorf("load policy: %w", err)
	}

	h := hub.New(3 * *interval)
	loop := &controller.Loop{Hub: h, Params: controller.DefaultParams, Interval: *interval, TTL: *ttl, Tier: t}
	loop.SetTable(policy.Compile(pf, policy.Version(pf.ToProto())))
	if *shards > 1 {
		n, i := uint32(*shards), uint32(*shard)
		loop.Owns = func(id uint32) bool { return id%n == i }
	}

	var globals []string
	if *global != "" {
		if t == controller.TierGlobal {
			return fmt.Errorf("-global is for the regional tier")
		}
		globals = strings.Split(*global, ",")
		loop.Global = upstream.NewMerger()
	}

	var elector leader.Elector = leader.Always{}
	if *elect {
		name := *leaseName
		if name == "" {
			name = fmt.Sprintf("valve-controller-%s-%d", t, *shard)
		}
		le, err := leader.InCluster(*leaseNS, name, *identity, log)
		if err != nil {
			return fmt.Errorf("leader election: %w", err)
		}
		elector = le
		h.SetServing(false) // standby until elected
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := grpc.NewServer()
	rlv1.RegisterRateLimitControlServer(srv, h)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go elector.Run(ctx, func(ctx context.Context) {
		log.Info("leading", "tier", t, "shard", *shard, "identity", *identity)
		h.SetServing(true)
		defer h.SetServing(false)
		// Only the leader reports to the global tier: a standby would report
		// zeros under the same node id and overwrite the leader's numbers.
		for i, target := range globals {
			conn, err := upstream.Dial(target, insecure.NewCredentials())
			if err != nil {
				log.Error("dial global controller", "target", target, "err", err)
				continue
			}
			defer conn.Close()
			prev := time.Now()
			go upstream.Run(ctx, conn, upstream.Config{
				Interval: *interval,
				Report: func() *rlv1.UsageReport {
					now := time.Now()
					r := loop.GlobalReport(*region, *region, prev, now)
					prev = now
					return r
				},
				OnDirective: func(d *rlv1.Directive) { loop.Global.Set(i, d) },
				Log:         log.With("upstream", target),
			})
		}
		tick := time.NewTicker(*interval)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				res := loop.Tick()
				log.Debug("tick", "peers", h.Peers(), "buckets", len(res.Shared))
			case <-ctx.Done():
				return
			}
		}
	})
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	log.Info("controller listening", "addr", lis.Addr().String(), "tier", t, "shard", *shard, "shards", *shards,
		"buckets", len(pf.Buckets), "leader_elect", *elect)
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
