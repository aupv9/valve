// Package controller computes per-bucket drop ratios from aggregated offered load.
package controller

import "math"

// Params tune the feedback loop. The defaults are starting points; tune them
// with load tests.
type Params struct {
	Alpha      float64 // EWMA smoothing factor
	Hysteresis float64 // start dropping only when R > L*(1+Hysteresis)
	MaxSlew    float64 // max change in p per cycle
	MaxDrop    float64 // hard clamp on p, limits blast radius of a bad computation
}

var DefaultParams = Params{
	Alpha:      0.5,
	Hysteresis: 0.05,
	MaxSlew:    0.2,
	MaxDrop:    0.9,
}

// BucketState is the controller's small, rebuildable state for one bucket.
type BucketState struct {
	smoothed float64 // R̂, smoothed offered RPS
	p        float64 // current drop ratio
	primed   bool
}

// Step feeds one cycle of offered RPS for a bucket with limit L and returns
// the new drop ratio:
//
//	R̂_t = α·R_t + (1−α)·R̂_{t−1}
//	p_t = max(0, (R̂_t − L) / R̂_t)
//
// with hysteresis on activation, a slew limit, and a clamp at MaxDrop.
func (s *BucketState) Step(offeredRPS, limit float64, prm Params) float64 {
	if !s.primed {
		s.smoothed, s.primed = offeredRPS, true
	} else {
		s.smoothed = prm.Alpha*offeredRPS + (1-prm.Alpha)*s.smoothed
	}

	target := 0.0
	r := s.smoothed
	// Hysteresis: once dropping, keep the plain formula (it reaches 0 at L);
	// when idle, require a margin above L before engaging.
	if r > limit && (s.p > 0 || r > limit*(1+prm.Hysteresis)) {
		target = (r - limit) / r
	}

	delta := target - s.p
	if math.Abs(delta) > prm.MaxSlew {
		delta = math.Copysign(prm.MaxSlew, delta)
	}
	s.p = min(max(s.p+delta, 0), prm.MaxDrop)
	return s.p
}

// seeded returns a state as if the bucket had been running at offered RPS
// with drop ratio p.
func seeded(offeredRPS, p float64) *BucketState {
	return &BucketState{smoothed: offeredRPS, p: p, primed: true}
}

// Ratio returns the current drop ratio without advancing the state.
func (s *BucketState) Ratio() float64 { return s.p }

// ToPPM converts a ratio to parts per million for the wire format.
func ToPPM(p float64) uint32 {
	return uint32(math.Round(min(max(p, 0), 1) * 1_000_000))
}

// AllocateByPriority splits the load to shed, S = R − L, across priority tiers
// from lowest to highest. tierRPS is ordered lowest priority first. Each tier
// gets p = min(1, remaining / R_tier), so a higher tier is touched only after
// every lower tier is fully dropped.
func AllocateByPriority(tierRPS []float64, limit float64) []float64 {
	var total float64
	for _, r := range tierRPS {
		total += r
	}
	out := make([]float64, len(tierRPS))
	remaining := total - limit
	for i, r := range tierRPS {
		if remaining <= 0 {
			break
		}
		if r <= 0 {
			continue
		}
		out[i] = min(1, remaining/r)
		remaining -= out[i] * r
	}
	return out
}

// Smoothed returns R̂, the smoothed offered RPS.
func (s *BucketState) Smoothed() float64 { return s.smoothed }

// Tiers holds one value per priority, indexed by rlv1.Priority
// (index 0, unspecified, is unused).
type Tiers [4]float64

// StepTiered advances the bucket with its per-tier offered RPS and returns
// the overall drop ratio plus a per-tier split of the same shed load. The
// overall ratio keeps every stability guard of Step; the split only decides
// who absorbs the shed, lowest priority first.
func (s *BucketState) StepTiered(tierRPS Tiers, limit float64, prm Params) (float64, Tiers) {
	var total float64
	for _, r := range tierRPS[1:] {
		total += r
	}
	p := s.Step(total, limit, prm)
	var out Tiers
	if p == 0 || total == 0 {
		return p, out
	}
	// Scale the raw tier mix to the smoothed rate, then shed p·R̂ from the
	// bottom up.
	scale := s.smoothed / total
	scaled := make([]float64, 3)
	for i := range scaled {
		scaled[i] = tierRPS[i+1] * scale
	}
	alloc := AllocateByPriority(scaled, s.smoothed*(1-p))
	copy(out[1:], alloc)
	return p, out
}
