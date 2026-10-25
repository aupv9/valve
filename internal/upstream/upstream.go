// Package upstream runs the client side of the Sync stream.
package upstream

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

type Config struct {
	Interval    time.Duration
	Report      func() *rlv1.UsageReport
	OnDirective func(*rlv1.Directive)
	OnTick      func(now time.Time)
	OnState     func(connected bool)
	Log         *slog.Logger
}

// Dial creates a round-robin client for one upstream target.
func Dial(target string, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
	)
}

// Run keeps a Sync stream open until ctx is done; OnTick runs even while disconnected.
func Run(ctx context.Context, conn grpc.ClientConnInterface, cfg Config) {
	if cfg.OnTick != nil {
		go func() {
			t := time.NewTicker(cfg.Interval)
			defer t.Stop()
			for {
				select {
				case now := <-t.C:
					cfg.OnTick(now)
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	client := rlv1.NewRateLimitControlClient(conn)
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		start := time.Now()
		err := runOnce(ctx, client, cfg)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > 10*cfg.Interval {
			backoff = 100 * time.Millisecond
		}
		cfg.Log.Warn("upstream stream ended, reconnecting", "err", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func runOnce(ctx context.Context, client rlv1.RateLimitControlClient, cfg Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Sync(ctx)
	if err != nil {
		return err
	}

	if err := stream.Send(cfg.Report()); err != nil {
		return err
	}
	if cfg.OnState != nil {
		cfg.OnState(true)
		defer cfg.OnState(false)
	}

	errc := make(chan error, 1)
	go func() {
		for {
			d, err := stream.Recv()
			if err != nil {
				errc <- err
				return
			}
			cfg.OnDirective(d)
		}
	}()

	t := time.NewTicker(cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := stream.Send(cfg.Report()); err != nil {
				return err
			}
		case err := <-errc:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
