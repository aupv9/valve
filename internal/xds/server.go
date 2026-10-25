// Package xds serves and subscribes to Valve's policies and kill switch over ADS.
package xds

import (
	"context"
	"fmt"

	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

// Type URLs and resource names of Valve's xDS resources.
const (
	PolicySetType    = "type.googleapis.com/ratelimit.v1.PolicySet"
	KillSwitchType   = "type.googleapis.com/ratelimit.v1.KillSwitch"
	PolicySetName    = "policies"
	KillSwitchName   = "kill-switch"
	maxResourceBytes = 64 << 20
)

// Server serves PolicySet and KillSwitch resources over ADS.
type Server struct {
	policies *cache.LinearCache
	kill     *cache.LinearCache
	xds      server.Server
}

// NewServer returns a server with no policies and the kill switch off.
func NewServer(ctx context.Context) *Server {
	s := &Server{
		policies: cache.NewLinearCache(PolicySetType),
		kill:     cache.NewLinearCache(KillSwitchType),
	}
	mux := &cache.MuxCache{
		Classify:      func(r *cache.Request) string { return r.GetTypeUrl() },
		ClassifyDelta: func(r *cache.DeltaRequest) string { return r.GetTypeUrl() },
		Caches:        map[string]cache.Cache{PolicySetType: s.policies, KillSwitchType: s.kill},
	}
	s.xds = server.NewServer(ctx, mux, server.CallbackFuncs{})
	if err := s.SetKillSwitch(&rlv1.KillSwitch{}); err != nil {
		panic(err)
	}
	return s
}

// Register adds the ADS service to a gRPC server.
func (s *Server) Register(g *grpc.Server) {
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(g, s.xds)
}

// SetPolicies publishes a new policy set to every subscriber.
func (s *Server) SetPolicies(ps *rlv1.PolicySet) error {
	return update(s.policies, PolicySetName, ps)
}

// SetKillSwitch publishes the kill switch state to every subscriber.
func (s *Server) SetKillSwitch(ks *rlv1.KillSwitch) error {
	return update(s.kill, KillSwitchName, ks)
}

func update(c *cache.LinearCache, name string, res types.Resource) error {
	if err := c.UpdateResource(name, res); err != nil {
		return fmt.Errorf("publish %s: %w", name, err)
	}
	return nil
}

// ServerOptions raise gRPC's message size limit for large policy sets.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.MaxRecvMsgSize(maxResourceBytes), grpc.MaxSendMsgSize(maxResourceBytes)}
}
