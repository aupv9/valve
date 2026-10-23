// Command controller computes drop ratios from aggregated usage and pushes
// them down to aggregators. Run one per region (and one global tier).
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

	"github.com/aupv9/valve/internal/controller"
	"github.com/aupv9/valve/internal/hub"
	"github.com/aupv9/valve/internal/policy"

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
	listen := flag.String("listen", ":9091", "gRPC listen address for aggregators")
	policyPath := flag.String("policy", "policies/example.yaml", "policy file")
	interval := flag.Duration("interval", time.Second, "control cycle")
	ttl := flag.Duration("ttl", 5*time.Second, "directive TTL downstream")
	flag.Parse()

	pf, err := policy.Load(*policyPath)
	if err != nil {
		return fmt.Errorf("load policy: %w", err)
	}

	h := hub.New(3 * *interval)
	loop := &controller.Loop{Hub: h, Params: controller.DefaultParams, Interval: *interval, TTL: *ttl}
	loop.SetTable(policy.Compile(pf, 1))

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := grpc.NewServer()
	rlv1.RegisterRateLimitControlServer(srv, h)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		t := time.NewTicker(*interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				d := loop.Tick()
				log.Debug("tick", "peers", h.Peers(), "buckets", len(d.Buckets))
			case <-ctx.Done():
				srv.GracefulStop()
				return
			}
		}
	}()

	log.Info("controller listening", "addr", lis.Addr().String(), "buckets", len(pf.Buckets))
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
