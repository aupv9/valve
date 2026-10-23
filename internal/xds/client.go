package xds

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

// Subscription watches one named resource.
type Subscription struct {
	TypeURL string
	Name    string
	NodeID  string
	// Apply validates and installs the resource. Returning an error NACKs
	// the update and the caller keeps its last good config.
	Apply func(*anypb.Any) error
	Log   *slog.Logger
}

// Subscribe keeps an ADS stream open until ctx is done, reconnecting with
// backoff. Updates are applied in order; each is ACKed or NACKed.
func Subscribe(ctx context.Context, conn grpc.ClientConnInterface, s Subscription) {
	client := discoverygrpc.NewAggregatedDiscoveryServiceClient(conn)
	backoff := 100 * time.Millisecond
	acked := "" // last version we accepted, re-sent on reconnect
	for ctx.Err() == nil {
		start := time.Now()
		err := s.stream(ctx, client, &acked)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute {
			backoff = 100 * time.Millisecond
		}
		s.Log.Warn("xds stream ended, reconnecting", "type", s.TypeURL, "err", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (s Subscription) stream(ctx context.Context, client discoverygrpc.AggregatedDiscoveryServiceClient, acked *string) error {
	st, err := client.StreamAggregatedResources(ctx, grpc.MaxCallRecvMsgSize(maxResourceBytes))
	if err != nil {
		return err
	}
	req := &discoverygrpc.DiscoveryRequest{
		Node:          &corev3.Node{Id: s.NodeID},
		TypeUrl:       s.TypeURL,
		ResourceNames: []string{s.Name},
		VersionInfo:   *acked,
	}
	if err := st.Send(req); err != nil {
		return err
	}
	for {
		resp, err := st.Recv()
		if err != nil {
			return err
		}
		next := &discoverygrpc.DiscoveryRequest{
			Node:          req.Node,
			TypeUrl:       s.TypeURL,
			ResourceNames: req.ResourceNames,
			ResponseNonce: resp.GetNonce(),
			VersionInfo:   *acked,
		}
		if err := s.apply(resp); err != nil {
			s.Log.Error("rejected xds update, keeping last good config", "type", s.TypeURL, "version", resp.GetVersionInfo(), "err", err)
			next.ErrorDetail = &statuspb.Status{Code: int32(codes.InvalidArgument), Message: err.Error()}
		} else {
			*acked = resp.GetVersionInfo()
			next.VersionInfo = *acked
		}
		if err := st.Send(next); err != nil {
			return err
		}
	}
}

func (s Subscription) apply(resp *discoverygrpc.DiscoveryResponse) error {
	if resp.GetTypeUrl() != s.TypeURL {
		return fmt.Errorf("unexpected type %q", resp.GetTypeUrl())
	}
	for _, r := range resp.GetResources() {
		if r.GetTypeUrl() == s.TypeURL {
			return s.Apply(r)
		}
	}
	return nil // resource not published yet
}

// PolicySetHandler adapts a typed handler to Subscription.Apply.
func PolicySetHandler(fn func(*rlv1.PolicySet) error) func(*anypb.Any) error {
	return func(a *anypb.Any) error {
		ps := &rlv1.PolicySet{}
		if err := a.UnmarshalTo(ps); err != nil {
			return err
		}
		return fn(ps)
	}
}

// KillSwitchHandler adapts a typed handler to Subscription.Apply.
func KillSwitchHandler(fn func(*rlv1.KillSwitch) error) func(*anypb.Any) error {
	return func(a *anypb.Any) error {
		ks := &rlv1.KillSwitch{}
		if err := a.UnmarshalTo(ks); err != nil {
			return err
		}
		return fn(ks)
	}
}
