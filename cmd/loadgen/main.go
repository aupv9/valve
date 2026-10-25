// Command loadgen sends HTTP requests at a fixed rate and prints status codes
// every second.
//
//	loadgen -url http://checkout:10000/checkout.Orders/CreateOrder -rps 300 -H x-valve-caller:web-gateway
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type headers []string

func (h *headers) String() string     { return strings.Join(*h, ",") }
func (h *headers) Set(v string) error { *h = append(*h, v); return nil }

func main() {
	url := flag.String("url", "http://localhost:10000/", "target URL")
	rps := flag.Int("rps", 100, "requests per second")
	duration := flag.Duration("duration", 0, "stop after this long (0 = until interrupted)")
	label := flag.String("label", "", "name printed at the start of each line")
	var hs headers
	flag.Var(&hs, "H", "header as name:value (repeatable)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: 256, MaxConnsPerHost: 256},
	}
	var (
		mu     sync.Mutex
		counts = map[string]int{}
		wg     sync.WaitGroup
		active atomic.Int64
	)
	send := func() {
		defer wg.Done()
		defer active.Add(-1)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, *url, nil)
		for _, h := range hs {
			if k, v, ok := strings.Cut(h, ":"); ok {
				req.Header.Set(strings.TrimSpace(k), strings.TrimSpace(v))
			}
		}
		code := "error"
		if resp, err := client.Do(req); err == nil {
			io.Copy(io.Discard, resp.Body) //nolint:errcheck // draining for reuse
			resp.Body.Close()
			code = fmt.Sprint(resp.StatusCode)
		} else if ctx.Err() != nil {
			return
		}
		mu.Lock()
		counts[code]++
		mu.Unlock()
	}

	const slots = 50
	tick := time.NewTicker(time.Second / slots)
	defer tick.Stop()
	report := time.NewTicker(time.Second)
	defer report.Stop()
	carry := 0.0
	for {
		select {
		case <-tick.C:
			carry += float64(*rps) / slots
			for ; carry >= 1; carry-- {
				if active.Load() >= 1024 {
					continue
				}
				active.Add(1)
				wg.Add(1)
				go send()
			}
		case <-report.C:
			mu.Lock()
			snapshot := counts
			counts = map[string]int{}
			mu.Unlock()
			keys := make([]string, 0, len(snapshot))
			for k := range snapshot {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			parts := make([]string, 0, len(keys))
			for _, k := range keys {
				parts = append(parts, fmt.Sprintf("%s=%d", k, snapshot[k]))
			}
			fmt.Printf("%-8s %s %s\n", *label, time.Now().Format("15:04:05"), strings.Join(parts, " "))
		case <-ctx.Done():
			wg.Wait()
			return
		}
	}
}
