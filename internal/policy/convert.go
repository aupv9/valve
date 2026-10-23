package policy

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	rlv1 "github.com/aupv9/valve/gen/ratelimit/v1"
)

// LoadPaths loads and merges every policy file in paths. A directory
// contributes its *.yaml and *.yml files. The merged set is validated as one,
// so names and matches must be unique across files.
func LoadPaths(paths ...string) (*File, error) {
	var files []string
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if !st.IsDir() {
			files = append(files, p)
			continue
		}
		for _, pat := range []string{"*.yaml", "*.yml"} {
			m, _ := filepath.Glob(filepath.Join(p, pat))
			files = append(files, m...)
		}
	}
	slices.Sort(files)

	merged := &File{}
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		pf, err := decode(f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		merged.Buckets = append(merged.Buckets, pf.Buckets...)
	}
	if err := merged.Validate(); err != nil {
		return nil, err
	}
	return merged, nil
}

var priorityToProto = map[Priority]rlv1.Priority{
	PriorityLow:    rlv1.Priority_PRIORITY_LOW,
	PriorityMedium: rlv1.Priority_PRIORITY_MEDIUM,
	PriorityHigh:   rlv1.Priority_PRIORITY_HIGH,
}

// PriorityFromProto maps a wire priority to a policy priority ("" if unspecified).
func PriorityFromProto(p rlv1.Priority) Priority {
	for k, v := range priorityToProto {
		if v == p {
			return k
		}
	}
	return ""
}

// Proto returns the wire priority.
func (p Priority) Proto() rlv1.Priority { return priorityToProto[p] }

// ParsePriority accepts low|medium|high, case-insensitive; "" if unknown.
func ParsePriority(s string) Priority {
	p := Priority(strings.ToLower(strings.TrimSpace(s)))
	if p.Rank() < 0 {
		return ""
	}
	return p
}

var scopeToProto = map[Scope]rlv1.Scope{
	ScopeGlobal:   rlv1.Scope_SCOPE_GLOBAL,
	ScopeRegional: rlv1.Scope_SCOPE_REGIONAL,
	ScopeZonal:    rlv1.Scope_SCOPE_ZONAL,
}

var modeToProto = map[Mode]rlv1.Mode{
	ModeShadow:  rlv1.Mode_MODE_SHADOW,
	ModeEnforce: rlv1.Mode_MODE_ENFORCE,
}

func reverse[K comparable, V comparable](m map[K]V, v V) K {
	for k, mv := range m {
		if mv == v {
			return k
		}
	}
	var zero K
	return zero
}

// ToProto converts a validated file into a PolicySet whose version is a hash
// of its content, so identical policies always get the same version.
func (pf *File) ToProto() *rlv1.PolicySet {
	ps := &rlv1.PolicySet{}
	for _, b := range pf.Buckets {
		ps.Buckets = append(ps.Buckets, &rlv1.Bucket{
			Name:     b.Name,
			Caller:   b.Match.Caller,
			Callee:   b.Match.Callee,
			Endpoint: b.Match.Endpoint,
			LimitRps: b.LimitRPS,
			Scope:    scopeToProto[b.Scope],
			Priority: priorityToProto[b.Priority],
			Mode:     modeToProto[b.Mode],
		})
	}
	ps.Version = fmt.Sprintf("%016x", hashProto(ps))
	return ps
}

// FromProto converts and validates a PolicySet received over the wire.
func FromProto(ps *rlv1.PolicySet) (*File, uint64, error) {
	pf := &File{}
	for _, b := range ps.GetBuckets() {
		pf.Buckets = append(pf.Buckets, Bucket{
			Name:     b.GetName(),
			Match:    Match{Caller: b.GetCaller(), Callee: b.GetCallee(), Endpoint: b.GetEndpoint()},
			LimitRPS: b.GetLimitRps(),
			Scope:    reverse(scopeToProto, b.GetScope()),
			Priority: reverse(priorityToProto, b.GetPriority()),
			Mode:     reverse(modeToProto, b.GetMode()),
		})
	}
	if err := pf.Validate(); err != nil {
		return nil, 0, err
	}
	return pf, Version(ps), nil
}

// Version returns the numeric config version used in Directives.
func Version(ps *rlv1.PolicySet) uint64 {
	c := proto.CloneOf(ps)
	c.Version = ""
	return hashProto(c)
}

func hashProto(m proto.Message) uint64 {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic(err) // only plain generated messages are hashed
	}
	sum := sha256.Sum256(b)
	return binary.BigEndian.Uint64(sum[:8])
}
