// Package policy loads, validates and compiles rate-limit bucket policies.
//
// Policies live in Git as YAML (see policies/). The configurator compiles them
// into a Table that the sidecar uses to match requests without re-parsing.
package policy

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

type Scope string

const (
	ScopeGlobal   Scope = "global"
	ScopeRegional Scope = "regional"
	ScopeZonal    Scope = "zonal"
)

type Mode string

const (
	ModeShadow  Mode = "shadow"
	ModeEnforce Mode = "enforce"
)

type Priority string

const (
	PriorityLow    Priority = "low"
	PriorityMedium Priority = "medium"
	PriorityHigh   Priority = "high"
)

// Rank orders priorities for tiered shedding: lower ranks are shed first.
func (p Priority) Rank() int {
	switch p {
	case PriorityLow:
		return 0
	case PriorityMedium:
		return 1
	case PriorityHigh:
		return 2
	}
	return -1
}

type Match struct {
	Caller   string `yaml:"caller"`   // from mesh mTLS identity; empty = any caller
	Callee   string `yaml:"callee"`   // required
	Endpoint string `yaml:"endpoint"` // empty = every endpoint
}

type Bucket struct {
	Name     string   `yaml:"name"`
	Match    Match    `yaml:"match"`
	LimitRPS uint64   `yaml:"limit_rps"`
	Scope    Scope    `yaml:"scope"`
	Priority Priority `yaml:"priority"`
	Mode     Mode     `yaml:"mode"`

	ID uint32 `yaml:"-"` // stable hash of Name, filled in by Validate
}

type File struct {
	Buckets []Bucket `yaml:"buckets"`
}

// Load parses and validates a policy file.
func Load(path string) (*File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f)
}

// Parse decodes YAML strictly (unknown fields are errors) and validates it.
func Parse(r io.Reader) (*File, error) {
	pf, err := decode(r)
	if err != nil {
		return nil, err
	}
	if err := pf.Validate(); err != nil {
		return nil, err
	}
	return pf, nil
}

func decode(r io.Reader) (*File, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	var pf File
	if err := dec.Decode(&pf); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode policy: %w", err)
	}
	return &pf, nil
}

// Validate checks every bucket, applies defaults and assigns IDs.
// New buckets default to shadow mode, per the rollout rules.
func (pf *File) Validate() error {
	var errs []error
	names := map[string]bool{}
	ids := map[uint32]string{}
	matches := map[Match]string{}
	for i := range pf.Buckets {
		b := &pf.Buckets[i]
		if b.Mode == "" {
			b.Mode = ModeShadow
		}
		if b.Scope == "" {
			b.Scope = ScopeGlobal
		}
		if b.Priority == "" {
			b.Priority = PriorityMedium
		}
		bad := func(format string, a ...any) {
			errs = append(errs, fmt.Errorf("bucket %d (%q): %s", i, b.Name, fmt.Sprintf(format, a...)))
		}
		if b.Name == "" {
			bad("name is required")
		} else if names[b.Name] {
			bad("duplicate name")
		}
		names[b.Name] = true
		if b.Match.Callee == "" {
			bad("match.callee is required")
		}
		if b.LimitRPS == 0 {
			bad("limit_rps must be > 0")
		}
		switch b.Scope {
		case ScopeGlobal, ScopeRegional, ScopeZonal:
		default:
			bad("unknown scope %q", b.Scope)
		}
		switch b.Mode {
		case ModeShadow, ModeEnforce:
		default:
			bad("unknown mode %q", b.Mode)
		}
		if b.Priority.Rank() < 0 {
			bad("unknown priority %q", b.Priority)
		}
		if other, ok := matches[b.Match]; ok {
			bad("same match as bucket %q", other)
		}
		matches[b.Match] = b.Name

		b.ID = hashName(b.Name)
		if other, ok := ids[b.ID]; ok && other != b.Name {
			bad("id collision with bucket %q; rename one of them", other)
		}
		ids[b.ID] = b.Name
	}
	return errors.Join(errs...)
}

func hashName(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return h.Sum32()
}
