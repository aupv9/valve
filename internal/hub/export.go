package hub

import rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"

// Inject records a report as if it arrived on a stream. It lets tests of
// code built on the hub skip the network.
func Inject(h *Hub, r *rlv1.UsageReport) {
	h.record(&peer{ch: make(chan *rlv1.Directive, 1)}, r)
}
