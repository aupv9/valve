package controller

import (
	"math"
	"testing"
)

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestStepConvergesToDocExample(t *testing.T) {

	var s BucketState
	var p float64
	for range 20 {
		p = s.Step(1500, 1000, DefaultParams)
	}
	if !approx(p, 1.0/3) {
		t.Fatalf("p = %v, want 1/3", p)
	}
}

func TestStepSlewLimited(t *testing.T) {
	var s BucketState
	p := s.Step(10_000, 1000, DefaultParams)
	if !approx(p, 0.2) {
		t.Fatalf("first step p = %v, want slew-limited 0.2", p)
	}
}

func TestStepHysteresis(t *testing.T) {
	var s BucketState

	for range 10 {
		if p := s.Step(1030, 1000, DefaultParams); p != 0 {
			t.Fatalf("engaged inside hysteresis band: p = %v", p)
		}
	}

	for range 10 {
		s.Step(1200, 1000, DefaultParams)
	}
	for range 10 {
		s.Step(1030, 1000, DefaultParams)
	}
	if p := s.Ratio(); p <= 0 {
		t.Fatalf("released inside band while active: p = %v", p)
	}
}

func TestStepClamp(t *testing.T) {
	var s BucketState
	for range 50 {
		s.Step(1e9, 1, DefaultParams)
	}
	if p := s.Ratio(); p != DefaultParams.MaxDrop {
		t.Fatalf("p = %v, want clamp %v", p, DefaultParams.MaxDrop)
	}
}

func TestStepReleasesBelowLimit(t *testing.T) {
	var s BucketState
	for range 20 {
		s.Step(2000, 1000, DefaultParams)
	}
	for range 30 {
		s.Step(500, 1000, DefaultParams)
	}
	if p := s.Ratio(); p != 0 {
		t.Fatalf("p = %v, want 0 after load drops below limit", p)
	}
}

func TestAllocateByPriority(t *testing.T) {
	tests := []struct {
		name  string
		tiers []float64
		limit float64
		want  []float64
	}{
		{"under limit", []float64{100, 100}, 500, []float64{0, 0}},
		{"low tier absorbs", []float64{400, 600}, 800, []float64{0.5, 0}},
		{"spills to high", []float64{200, 800}, 600, []float64{1, 0.25}},
		{"empty low tier", []float64{0, 1000}, 800, []float64{0, 0.2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AllocateByPriority(tt.tiers, tt.limit)
			for i := range got {
				if !approx(got[i], tt.want[i]) {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestStepTieredShedsLowFirst(t *testing.T) {
	var s BucketState

	var p float64
	var tiers Tiers
	for range 30 {
		p, tiers = s.StepTiered(Tiers{0, 600, 0, 900}, 1000, DefaultParams)
	}
	if !approx(p, 1.0/3) {
		t.Fatalf("overall p = %v, want 1/3", p)
	}
	if !approx(tiers[1], 500.0/600) || tiers[3] != 0 {
		t.Fatalf("tiers = %v, want low=%v high=0", tiers, 500.0/600)
	}
}

func TestStepTieredSpillsToHigh(t *testing.T) {
	var s BucketState
	var tiers Tiers
	for range 30 {
		_, tiers = s.StepTiered(Tiers{0, 200, 0, 1300}, 1000, DefaultParams)
	}

	if tiers[1] != 1 || !approx(tiers[3], 300.0/1300) {
		t.Fatalf("tiers = %v", tiers)
	}
}
