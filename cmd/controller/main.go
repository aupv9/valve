// Command controller computes drop ratios, per region or for the global tier.
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
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/aupv9/valve/internal/admin"
	"github.com/aupv9/valve/internal/controller"
	"github.com/aupv9/valve/internal/hub"
	"github.com/aupv9/valve/internal/leader"
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
		log.Error("controller failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	listen := flag.String("listen", ":9091", "gRPC listen address for aggregators (or regional controllers)")
	policyPath := flag.String("policy", "policies", "policy file or directory (ignored with -xds)")
	xdsTarget := flag.String("xds", "", "configurator xDS target; policies and the kill switch come from it")
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
	adminAddr := flag.String("admin", ":9104", "admin HTTP address for /metrics, /healthz, /readyz (empty = off)")
	tlsCfg := tlsutil.RegisterFlags(flag.CommandLine)
	flag.Parse()

	serverCreds, err := tlsCfg.ServerCredentials()
	if err != nil {
		return err
	}
	clientCreds, err := tlsCfg.ClientCredentials()
	if err != nil {
		return err
	}
	reg := admin.NewRegistry()
	m := metrics.NewController(reg)
	var leading atomic.Bool

	t := controller.Tier(*tier)
	if t != controller.TierRegional && t != controller.TierGlobal {
		return fmt.Errorf("unknown -tier %q", *tier)
	}
	if *shards < 1 || *shard < 0 || *shard >= *shards {
		return fmt.Errorf("bad sharding -shard %d -shards %d", *shard, *shards)
	}

	h := hub.New(3 * *interval)
	loop := &controller.Loop{Hub: h, Params: controller.DefaultParams, Interval: *interval, TTL: *ttl, Tier: t}
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
		h.SetServing(false)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := grpc.NewServer(grpc.Creds(serverCreds))
	rlv1.RegisterRateLimitControlServer(srv, h)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *xdsTarget == "" {
		pf, err := policy.LoadPaths(*policyPath)
		if err != nil {
			return fmt.Errorf("load policy: %w", err)
		}
		loop.SetTable(policy.Compile(pf, policy.Version(pf.ToProto())))
	} else {
		conn, err := upstream.Dial(*xdsTarget, clientCreds)
		if err != nil {
			return fmt.Errorf("dial xds: %w", err)
		}
		defer conn.Close()
		xds.Watch(ctx, conn, *identity, log.With("xds", *xdsTarget),
			func(pf *policy.File, version uint64) error {
				loop.SetTable(policy.Compile(pf, version))
				return nil
			},
			func(ks *rlv1.KillSwitch) { loop.SetKillSwitch(ks.GetEnabled()) })
	}

	go elector.Run(ctx, func(ctx context.Context) {
		log.Info("leading", "tier", t, "shard", *shard, "identity", *identity)
		h.SetServing(true)
		leading.Store(true)
		m.Leader.Set(1)
		defer func() {
			h.SetServing(false)
			leading.Store(false)
			m.Leader.Set(0)
		}()

		for i, target := range globals {
			conn, err := upstream.Dial(target, clientCreds)
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
				OnState:     func(up bool) { m.GlobalConn.WithLabelValues(target).Set(metrics.Bool(up)) },
				Log:         log.With("upstream", target),
			})
		}
		tick := time.NewTicker(*interval)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				start := time.Now()
				res := loop.Tick()
				m.Tick.Observe(time.Since(start).Seconds())
				m.Peers.Set(float64(h.Peers()))
				m.KillSwitch.Set(metrics.Bool(res.Kill))
				m.SetConfigVersion(res.Version)
				m.ObserveBuckets(res.Shared, res.ByZone, loop.BucketName)
			case <-ctx.Done():
				return
			}
		}
	})
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	admin.Serve(ctx, *adminAddr, admin.Mux(reg, leading.Load), log)

	log.Info("controller listening", "addr", lis.Addr().String(), "tier", t, "shard", *shard, "shards", *shards,
		"leader_elect", *elect, "xds", *xdsTarget, "mtls", tlsCfg.Enabled())
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
