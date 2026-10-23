package xds

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

func dial(t *testing.T, s *Server) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(ServerOptions()...)
	s.Register(g)
	go g.Serve(lis) //nolint:errcheck // ends with Stop
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

type recorder struct {
	mu   sync.Mutex
	seen []string
}

func (r *recorder) add(s string) {
	r.mu.Lock()
	r.seen = append(r.seen, s)
	r.mu.Unlock()
}

func (r *recorder) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.seen) == 0 {
		return ""
	}
	return r.seen[len(r.seen)-1]
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPolicyDeliveryAckNack(t *testing.T) {
	ctx := t.Context()
	s := NewServer(ctx)
	conn := dial(t, s)

	var got recorder
	go Subscribe(ctx, conn, Subscription{
		TypeURL: PolicySetType, Name: PolicySetName, NodeID: "n1", Log: slog.New(slog.DiscardHandler),
		Apply: PolicySetHandler(func(ps *rlv1.PolicySet) error {
			if ps.GetVersion() == "bad" {
				return errors.New("invalid")
			}
			got.add(ps.GetVersion())
			return nil
		}),
	})

	for _, v := range []string{"v1", "bad", "v2"} {
		if err := s.SetPolicies(&rlv1.PolicySet{Version: v}); err != nil {
			t.Fatal(err)
		}
		if v != "bad" {
			eventually(t, func() bool { return got.last() == v })
		}
	}
	for _, v := range got.seen {
		if v == "bad" {
			t.Fatal("rejected version was applied")
		}
	}
}

func TestKillSwitchOwnStream(t *testing.T) {
	ctx := t.Context()
	s := NewServer(ctx)
	conn := dial(t, s)

	var on recorder
	go Subscribe(ctx, conn, Subscription{
		TypeURL: KillSwitchType, Name: KillSwitchName, NodeID: "n1", Log: slog.New(slog.DiscardHandler),
		Apply: KillSwitchHandler(func(ks *rlv1.KillSwitch) error {
			if ks.GetEnabled() {
				on.add("on")
			} else {
				on.add("off")
			}
			return nil
		}),
	})
	eventually(t, func() bool { return on.last() == "off" }) // initial state

	start := time.Now()
	if err := s.SetKillSwitch(&rlv1.KillSwitch{Enabled: true, Reason: "drill"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return on.last() == "on" })
	if d := time.Since(start); d > time.Second {
		t.Fatalf("kill switch took %v to arrive", d)
	}
}
