package xds

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"

	"github.com/aupv9/valve/internal/policy"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

// Watch subscribes to policies and the kill switch and returns immediately.
func Watch(ctx context.Context, conn grpc.ClientConnInterface, nodeID string, log *slog.Logger,
	onPolicy func(pf *policy.File, version uint64) error, onKill func(ks *rlv1.KillSwitch)) {
	go Subscribe(ctx, conn, Subscription{
		TypeURL: PolicySetType, Name: PolicySetName, NodeID: nodeID, Log: log,
		Apply: PolicySetHandler(func(ps *rlv1.PolicySet) error {
			pf, version, err := policy.FromProto(ps)
			if err != nil {
				return err
			}
			if err := onPolicy(pf, version); err != nil {
				return err
			}
			log.Info("policy update applied", "version", ps.GetVersion(), "buckets", len(pf.Buckets))
			return nil
		}),
	})
	go Subscribe(ctx, conn, Subscription{
		TypeURL: KillSwitchType, Name: KillSwitchName, NodeID: nodeID, Log: log,
		Apply: KillSwitchHandler(func(ks *rlv1.KillSwitch) error {
			onKill(ks)
			return nil
		}),
	})
}
