// Command aggregator runs per zone: it sums usage from sidecars, forwards it
// to the regional controller, and relays directives back down. Stateless.
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
	controllerAddr := flag.String("controller", "localhost:9091", "regional controller address")
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

	// TODO: mTLS between tiers.
	conn, err := grpc.NewClient(*controllerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer conn.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go upstream.Run(ctx, conn, upstream.Config{
		Interval: *interval,
		Report: func() *rlv1.UsageReport {
			r := &rlv1.UsageReport{NodeId: *nodeID, Zone: *zone, WindowEndUnixMs: time.Now().UnixMilli()}
			for id, u := range h.Collect() {
				r.Buckets = append(r.Buckets, &rlv1.BucketUsage{BucketId: id, Offered: u.Offered, Dropped: u.Dropped})
			}
			return r
		},
		OnDirective: h.Broadcast,
		Log:         log,
	})
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	log.Info("aggregator listening", "addr", lis.Addr().String(), "controller", *controllerAddr)
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
