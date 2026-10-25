// Package leader runs a function only while this replica holds leadership.
package leader

import (
	"context"
	"log/slog"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Elector runs lead while this replica is the leader, until ctx is done.
type Elector interface {
	Run(ctx context.Context, lead func(ctx context.Context))
}

// Always is the elector for single-replica and local deployments.
type Always struct{}

func (Always) Run(ctx context.Context, lead func(ctx context.Context)) { lead(ctx) }

// Lease elects a leader with a Kubernetes coordination.k8s.io Lease.
type Lease struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string
	Identity  string
	Log       *slog.Logger

	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// InCluster returns a Lease elector using the pod's service account.
func InCluster(namespace, name, identity string, log *slog.Logger) (*Lease, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Lease{Client: cs, Namespace: namespace, Name: name, Identity: identity, Log: log}, nil
}

func (l *Lease) Run(ctx context.Context, lead func(ctx context.Context)) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metaFor(l.Namespace, l.Name),
		Client:     l.Client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: l.Identity},
	}
	cfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   or(l.LeaseDuration, 15*time.Second),
		RenewDeadline:   or(l.RenewDeadline, 10*time.Second),
		RetryPeriod:     or(l.RetryPeriod, 2*time.Second),
		ReleaseOnCancel: true,
		Name:            l.Name,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: lead,
			OnStoppedLeading: func() { l.Log.Info("leadership lost", "lease", l.Name, "identity", l.Identity) },
			OnNewLeader: func(id string) {
				l.Log.Info("leader is", "lease", l.Name, "leader", id, "self", id == l.Identity)
			},
		},
	}

	for ctx.Err() == nil {
		leaderelection.RunOrDie(ctx, cfg)
	}
}

func or(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}
