// Command aggregator runs per zone: it sums usage from sidecars, forwards it
// to the regional controller shards, and relays their merged directives back
// down. Stateless; run several replicas per zone.
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

	"github.com/aupv9/valve/internal/hub"
	"github.com/aupv9/valve/internal/upstream"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("aggregator failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	listen := flag.String("listen", ":9090", "gRPC listen address for sidecars")
	controllers := flag.String("controller", "localhost:9091", "comma-separated regional controller targets, one per shard")
	nodeID := flag.String("node-id", hostname(), "this aggregator's id")
	zone := flag.String("zone", "local", "zone name")
	interval := flag.Duration("interval", time.Second, "report cycle")
	flag.Parse()

	h := hub.New(3 * *interval)
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := grpc.NewServer()
	rlv1.RegisterRateLimitControlServer(srv, h)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	merger := upstream.NewMerger()
	for i, target := range strings.Split(*controllers, ",") {
		conn, err := upstream.Dial(target, insecure.NewCredentials())
		if err != nil {
			return fmt.Errorf("dial controller %s: %w", target, err)
		}
		defer conn.Close()
		prev := time.Now()
		go upstream.Run(ctx, conn, upstream.Config{
			Interval: *interval,
			Report: func() *rlv1.UsageReport {
				now := time.Now()
				r := hub.Report(*nodeID, *zone, h.Rates(), prev, now, nil)
				prev = now
				return r
			},
			OnDirective: func(d *rlv1.Directive) {
				merger.Set(i, d)
				if m := merger.Merged(); m != nil {
					h.BroadcastAll(m)
				}
			},
			Log: log.With("upstream", target),
		})
	}
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	log.Info("aggregator listening", "addr", lis.Addr().String(), "controllers", *controllers, "zone", *zone)
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
