// Package envoy integrates Valve with an unmodified Envoy sidecar
// ("direction 2" in the design doc):
//
//   - Config generates, per bucket of the local service, a fault filter that
//     aborts with 429 at a percentage read from the runtime key
//     valve.<bucket>.abort_ppm, and a virtual cluster that counts the
//     requests let through.
//   - DataPlane is the agent side: it serves those runtime keys to Envoy over
//     RTDS and reads the virtual cluster counters from the admin API.
//
// Envoy cannot express "most specific bucket wins" for every overlap. Where
// a less specific bucket can exclude a more specific one with one inverted
// header match it does; otherwise Config reports a warning and a request may
// be sampled by both buckets.
package envoy

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	faultv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/fault/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	matcherv3 "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"gopkg.in/yaml.v3"

	"github.com/aupv9/valve/internal/policy"
)

const (
	// VirtualClusterPrefix prefixes the virtual cluster of each bucket.
	VirtualClusterPrefix = "valve_"
	// RuntimeLayer is the RTDS layer name Envoy must subscribe to.
	RuntimeLayer = "valve"
)

// RuntimeKey is the runtime key holding a bucket's abort ratio in ppm.
func RuntimeKey(bucket string) string { return "valve." + bucket + ".abort_ppm" }

// CallerMatch says how Envoy identifies the caller of a request.
type CallerMatch struct {
	// Header holds the caller name verbatim (for example x-valve-caller set
	// by a trusted gateway). Used when SPIFFE is false.
	Header string
	// SPIFFE matches the service account in the client certificate URI that
	// Envoy forwards in x-forwarded-client-cert (…/sa/<caller>).
	SPIFFE bool
}

// Fragment is the Envoy configuration for one service's buckets.
type Fragment struct {
	HTTPFilters     []*hcmv3.HttpFilter       // insert before envoy.filters.http.router
	VirtualClusters []*routev3.VirtualCluster // add to the service's virtual host
	Buckets         []*policy.Bucket          // buckets covered, most specific first
	Warnings        []string
}

// Config builds the fragment for buckets whose callee is service.
func Config(pf *policy.File, service string, cm CallerMatch) (*Fragment, error) {
	if !cm.SPIFFE && cm.Header == "" {
		cm.Header = "x-valve-caller"
	}
	var bs []*policy.Bucket
	for i := range pf.Buckets {
		if pf.Buckets[i].Match.Callee == service {
			bs = append(bs, &pf.Buckets[i])
		}
	}
	// Most specific first: Envoy uses the first matching virtual cluster.
	slices.SortStableFunc(bs, func(a, b *policy.Bucket) int { return specificity(b) - specificity(a) })

	frag := &Fragment{Buckets: bs}
	for _, b := range bs {
		headers := matchers(b.Match, cm)
		for _, other := range bs {
			if other == b || !narrower(other.Match, b.Match) {
				continue
			}
			ex, ok := exclusion(other.Match, b.Match, cm)
			if !ok {
				frag.Warnings = append(frag.Warnings, fmt.Sprintf(
					"bucket %q overlaps the more specific %q in two dimensions; Envoy cannot exclude it, so their requests are sampled by both",
					b.Name, other.Name))
				continue
			}
			headers = append(headers, ex)
		}

		fault := &faultv3.HTTPFault{
			Abort: &faultv3.FaultAbort{
				ErrorType:  &faultv3.FaultAbort_HttpStatus{HttpStatus: 429},
				Percentage: &typev3.FractionalPercent{Numerator: 0, Denominator: typev3.FractionalPercent_MILLION},
			},
			AbortPercentRuntime: RuntimeKey(b.Name),
			Headers:             headers,
		}
		typed, err := anypb.New(fault)
		if err != nil {
			return nil, err
		}
		frag.HTTPFilters = append(frag.HTTPFilters, &hcmv3.HttpFilter{
			Name:       "valve.fault." + b.Name,
			ConfigType: &hcmv3.HttpFilter_TypedConfig{TypedConfig: typed},
		})
		frag.VirtualClusters = append(frag.VirtualClusters, &routev3.VirtualCluster{
			Name:    VirtualClusterPrefix + b.Name,
			Headers: headers,
		})
	}
	return frag, nil
}

func specificity(b *policy.Bucket) int {
	n := 0
	if b.Match.Caller != "" {
		n += 2
	}
	if b.Match.Endpoint != "" {
		n++
	}
	return n
}

// narrower reports whether every request matching a also matches b, and a
// is strictly more specific.
func narrower(a, b policy.Match) bool {
	if a == b {
		return false
	}
	return (b.Caller == "" || b.Caller == a.Caller) && (b.Endpoint == "" || b.Endpoint == a.Endpoint)
}

// exclusion returns the inverted matcher that removes a's requests from b,
// possible when a adds exactly one condition to b.
func exclusion(a, b policy.Match, cm CallerMatch) (*routev3.HeaderMatcher, bool) {
	extraCaller := a.Caller != "" && b.Caller == ""
	extraEndpoint := a.Endpoint != "" && b.Endpoint == ""
	switch {
	case extraCaller && !extraEndpoint:
		m := callerMatcher(a.Caller, cm)
		m.InvertMatch = true
		return m, true
	case extraEndpoint && !extraCaller:
		m := endpointMatcher(a.Endpoint)
		m.InvertMatch = true
		return m, true
	}
	return nil, false
}

func matchers(m policy.Match, cm CallerMatch) []*routev3.HeaderMatcher {
	var hs []*routev3.HeaderMatcher
	if m.Caller != "" {
		hs = append(hs, callerMatcher(m.Caller, cm))
	}
	if m.Endpoint != "" {
		hs = append(hs, endpointMatcher(m.Endpoint))
	}
	return hs
}

func callerMatcher(caller string, cm CallerMatch) *routev3.HeaderMatcher {
	if cm.SPIFFE {
		re := `.*URI=spiffe://[^;,]*/sa/` + regexpQuote(caller) + `(;.*|,.*|$)`
		return &routev3.HeaderMatcher{
			Name: "x-forwarded-client-cert",
			HeaderMatchSpecifier: &routev3.HeaderMatcher_StringMatch{StringMatch: &matcherv3.StringMatcher{
				MatchPattern: &matcherv3.StringMatcher_SafeRegex{SafeRegex: &matcherv3.RegexMatcher{Regex: re}},
			}},
		}
	}
	return &routev3.HeaderMatcher{
		Name: cm.Header,
		HeaderMatchSpecifier: &routev3.HeaderMatcher_StringMatch{StringMatch: &matcherv3.StringMatcher{
			MatchPattern: &matcherv3.StringMatcher_Exact{Exact: caller},
		}},
	}
}

// endpointMatcher matches a path prefix when the endpoint starts with "/"
// (HTTP routes), otherwise a gRPC method name at the end of the path.
func endpointMatcher(endpoint string) *routev3.HeaderMatcher {
	sm := &matcherv3.StringMatcher{MatchPattern: &matcherv3.StringMatcher_Suffix{Suffix: "/" + endpoint}}
	if strings.HasPrefix(endpoint, "/") {
		sm = &matcherv3.StringMatcher{MatchPattern: &matcherv3.StringMatcher_Prefix{Prefix: endpoint}}
	}
	return &routev3.HeaderMatcher{
		Name:                 ":path",
		HeaderMatchSpecifier: &routev3.HeaderMatcher_StringMatch{StringMatch: sm},
	}
}

func regexpQuote(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(`\.+*?()|[]{}^$`, r) {
			b.WriteRune('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// YAML renders the fragment as two YAML lists ready to paste into an Envoy
// config (or an Istio EnvoyFilter patch).
func (f *Fragment) YAML() ([]byte, error) {
	toAny := func(ms []proto.Message) ([]any, error) {
		out := make([]any, 0, len(ms))
		for _, m := range ms {
			b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
			if err != nil {
				return nil, err
			}
			var v any
			if err := json.Unmarshal(b, &v); err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	filters := make([]proto.Message, len(f.HTTPFilters))
	for i, x := range f.HTTPFilters {
		filters[i] = x
	}
	vcs := make([]proto.Message, len(f.VirtualClusters))
	for i, x := range f.VirtualClusters {
		vcs[i] = x
	}
	hf, err := toAny(filters)
	if err != nil {
		return nil, err
	}
	vc, err := toAny(vcs)
	if err != nil {
		return nil, err
	}
	return yaml.Marshal(map[string]any{"http_filters": hf, "virtual_clusters": vc})
}
