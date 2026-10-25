// Command aggregator sums a zone's usage and relays directives back down.
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

	"github.com/aupv9/valve/internal/admin"
	"github.com/aupv9/valve/internal/hub"
	"github.com/aupv9/valve/internal/metrics"
	"github.com/aupv9/valve/internal/tlsutil"
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
	adminAddr := flag.String("admin", ":9103", "admin HTTP address for /metrics, /healthz, /readyz (empty = off)")
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
	m := metrics.NewAggregator(reg)

	h := hub.New(3 * *interval)
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	srv := grpc.NewServer(grpc.Creds(serverCreds))
	rlv1.RegisterRateLimitControlServer(srv, h)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	merger := upstream.NewMerger()
	for i, target := range strings.Split(*controllers, ",") {
		conn, err := upstream.Dial(target, clientCreds)
		if err != nil {
			return fmt.Errorf("dial controller %s: %w", target, err)
		}
		defer conn.Close()
		prev := time.Now()
		go upstream.Run(ctx, conn, upstream.Config{
			Interval: *interval,
			Report: func() *rlv1.UsageReport {
				now := time.Now()
				m.Peers.Set(float64(h.Peers()))
				r := hub.Report(*nodeID, *zone, h.Rates(), prev, now, nil)
				prev = now
				return r
			},
			OnDirective: func(d *rlv1.Directive) {
				merger.Set(i, d)
				if md := merger.Merged(); md != nil {
					h.BroadcastAll(md)
				}
			},
			OnState: func(up bool) { m.Connected.WithLabelValues(target).Set(metrics.Bool(up)) },
			Log:     log.With("upstream", target),
		})
	}
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	admin.Serve(ctx, *adminAddr, admin.Mux(reg, nil), log)

	log.Info("aggregator listening", "addr", lis.Addr().String(), "controllers", *controllers, "zone", *zone, "mtls", tlsCfg.Enabled())
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
