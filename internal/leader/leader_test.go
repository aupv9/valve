package leader

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func TestLeaseOneLeaderAndFailover(t *testing.T) {
	cs := fake.NewClientset()
	log := slog.New(slog.DiscardHandler)
	var leaders atomic.Int32
	var aLed atomic.Bool

	newLease := func(id string) *Lease {
		return &Lease{
			Client: cs, Namespace: "valve", Name: "valve-controller-0", Identity: id, Log: log,
			LeaseDuration: 2 * time.Second, RenewDeadline: 1500 * time.Millisecond, RetryPeriod: 200 * time.Millisecond,
		}
	}
	lead := func(id string) func(ctx context.Context) {
		return func(ctx context.Context) {
			if n := leaders.Add(1); n > 1 {
				t.Errorf("%s: %d leaders at once", id, n)
			}
			if id == "a" {
				aLed.Store(true)
			}
			<-ctx.Done()
			leaders.Add(-1)
		}
	}

	ctxA, cancelA := context.WithCancel(t.Context())
	doneA := make(chan struct{})
	go func() { newLease("a").Run(ctxA, lead("a")); close(doneA) }()
	waitFor(t, aLed.Load)

	var bLed atomic.Bool
	go newLease("b").Run(t.Context(), func(ctx context.Context) { bLed.Store(true); lead("b")(ctx) })
	time.Sleep(time.Second)
	if bLed.Load() {
		t.Fatal("b became leader while a holds the lease")
	}

	cancelA() // a releases the lease on cancel
	<-doneA
	waitFor(t, bLed.Load)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
