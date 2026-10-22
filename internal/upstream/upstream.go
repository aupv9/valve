// Package upstream runs the client side of the Sync stream: push a usage
// report every cycle, apply directives as they arrive, reconnect on failure.
package upstream

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

type Config struct {
	Interval    time.Duration            // report cycle, ~1s
	Report      func() *rlv1.UsageReport // called once per cycle
	OnDirective func(*rlv1.Directive)    // called for each directive received
	OnTick      func(now time.Time)      // optional, e.g. TTL expiry
	Log         *slog.Logger
}

// Run keeps a Sync stream open to conn until ctx is done. OnTick runs on its
// own ticker, independent of the stream, so TTLs still expire and fail open
// while the upstream is unreachable.
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
			backoff = 100 * time.Millisecond // was healthy for a while
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
