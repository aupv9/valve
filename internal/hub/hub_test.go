package hub

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

func serve(t *testing.T, h *Hub) rlv1.RateLimitControlClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	rlv1.RegisterRateLimitControlServer(srv, h)
	go srv.Serve(lis) //nolint:errcheck // ends with Stop
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return rlv1.NewRateLimitControlClient(conn)
}

func report(node, zone string, end time.Time, offered uint64) *rlv1.UsageReport {
	return &rlv1.UsageReport{
		NodeId: node, Zone: zone,
		WindowStartUnixMs: end.Add(-time.Second).UnixMilli(),
		WindowEndUnixMs:   end.UnixMilli(),
		Buckets:           []*rlv1.BucketUsage{{BucketId: 7, Offered: offered, Priority: rlv1.Priority_PRIORITY_LOW}},
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRatesLatestPerNodeAndStale(t *testing.T) {
	h := New(3 * time.Second)
	now := time.Now()
	h.now = func() time.Time { return now }
	c := serve(t, h)

	s, err := c.Sync(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range []*rlv1.UsageReport{
		report("a", "z1", now.Add(-time.Second), 999),
		report("a", "z1", now, 100),
		report("b", "z2", now.Add(-10*time.Second), 500),
		report("c", "z2", now, 300),
	} {
		if err := s.Send(r); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { h.mu.Lock(); defer h.mu.Unlock(); return len(h.nodes) == 3 })

	rates := h.Rates()
	want := map[Key]Rate{
		{Bucket: 7, Priority: rlv1.Priority_PRIORITY_LOW, Zone: "z1"}: {Offered: 100},
		{Bucket: 7, Priority: rlv1.Priority_PRIORITY_LOW, Zone: "z2"}: {Offered: 300},
	}
	if len(rates) != len(want) {
		t.Fatalf("rates = %v, want %v", rates, want)
	}
	for k, w := range want {
		if rates[k] != w {
			t.Fatalf("rates[%v] = %v, want %v", k, rates[k], w)
		}
	}
}

func TestBroadcastPerZoneAndReplay(t *testing.T) {
	h := New(3 * time.Second)
	c := serve(t, h)
	h.Broadcast(func(zone string) *rlv1.Directive { return &rlv1.Directive{ConfigVersion: uint64(len(zone))} })

	s, err := c.Sync(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Send(report("a", "zone-ab", time.Now(), 1)); err != nil {
		t.Fatal(err)
	}
	for {
		d, err := s.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if d.GetConfigVersion() == uint64(len("zone-ab")) {
			break
		}
	}
	waitFor(t, func() bool { return h.Peers() == 1 })
}

func TestSetServingFalseEndsStreams(t *testing.T) {
	h := New(3 * time.Second)
	c := serve(t, h)
	s, err := c.Sync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(report("a", "z", time.Now(), 1)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.Peers() == 1 })

	h.SetServing(false)
	if _, err := s.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("Recv err = %v, want Unavailable", err)
	}
	s2, err := c.Sync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("new stream err = %v, want Unavailable", err)
	}
}

func TestReport(t *testing.T) {
	rates := map[Key]Rate{
		{Bucket: 1, Priority: 1, Zone: "z1"}: {Offered: 100, Dropped: 10},
		{Bucket: 1, Priority: 1, Zone: "z2"}: {Offered: 50},
		{Bucket: 2, Priority: 1, Zone: "z1"}: {Offered: 7},
	}
	end := time.Now()
	r := Report("agg", "zone", rates, end.Add(-2*time.Second), end, func(id uint32) bool { return id == 1 })
	if len(r.Buckets) != 1 || r.Buckets[0].Offered != 300 || r.Buckets[0].Dropped != 20 {
		t.Fatalf("report = %v, want bucket 1 offered=300 dropped=20 over 2s", r.Buckets)
	}
}
