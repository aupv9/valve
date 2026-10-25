// Package controller computes per-bucket drop ratios from aggregated offered load.
package controller

import "math"

// Params tune the feedback loop.
type Params struct {
	Alpha      float64
	Hysteresis float64
	MaxSlew    float64
	MaxDrop    float64
}

var DefaultParams = Params{
	Alpha:      0.5,
	Hysteresis: 0.05,
	MaxSlew:    0.2,
	MaxDrop:    0.9,
}

// BucketState is the controller's small, rebuildable state for one bucket.
type BucketState struct {
	smoothed float64
	p        float64
	primed   bool
}

// Step feeds one cycle of offered RPS against limit and returns the new drop
// ratio: p = max(0, (R̂ − L) / R̂) over the EWMA R̂, with hysteresis, a slew
// limit and a clamp at MaxDrop.
func (s *BucketState) Step(offeredRPS, limit float64, prm Params) float64 {
	if !s.primed {
		s.smoothed, s.primed = offeredRPS, true
	} else {
		s.smoothed = prm.Alpha*offeredRPS + (1-prm.Alpha)*s.smoothed
	}

	target := 0.0
	r := s.smoothed

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

func seeded(offeredRPS, p float64) *BucketState {
	return &BucketState{smoothed: offeredRPS, p: p, primed: true}
}

// Ratio returns the current drop ratio without advancing the state.
func (s *BucketState) Ratio() float64 { return s.p }

// ToPPM converts a ratio to parts per million for the wire format.
func ToPPM(p float64) uint32 {
	return uint32(math.Round(min(max(p, 0), 1) * 1_000_000))
}

// AllocateByPriority sheds R − L from tierRPS, lowest priority first, and
// returns each tier's drop ratio.
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

// Tiers holds one value per priority, indexed by rlv1.Priority.
type Tiers [4]float64

// StepTiered is Step on the tiers' total, plus a per-tier split of the shed
// load, lowest priority first.
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

	scale := s.smoothed / total
	scaled := make([]float64, 3)
	for i := range scaled {
		scaled[i] = tierRPS[i+1] * scale
	}
	alloc := AllocateByPriority(scaled, s.smoothed*(1-p))
	copy(out[1:], alloc)
	return p, out
}
