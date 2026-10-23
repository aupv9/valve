package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aupv9/valve/internal/filter"
	"github.com/aupv9/valve/internal/policy"
)

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
