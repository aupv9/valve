package policy

import (
	"strings"
	"testing"
)

func TestParseDefaultsAndMatch(t *testing.T) {
	pf, err := Parse(strings.NewReader(`
buckets:
  - name: any-to-checkout
    match: {callee: checkout}
    limit_rps: 5000
  - name: web-create-order
    match: {caller: web, callee: checkout, endpoint: CreateOrder}
    limit_rps: 100
    mode: enforce
    priority: high
    scope: zonal
`))
	if err != nil {
		t.Fatal(err)
	}
	b := pf.Buckets[0]
	if b.Mode != ModeShadow || b.Scope != ScopeGlobal || b.Priority != PriorityMedium {
		t.Fatalf("defaults not applied: %+v", b)
	}

	tbl := Compile(pf, 7)
	tests := []struct {
		req  Request
		want string
	}{
		{Request{Caller: "web", Callee: "checkout", Endpoint: "CreateOrder"}, "web-create-order"},
		{Request{Caller: "web", Callee: "checkout", Endpoint: "GetOrder"}, "any-to-checkout"},
		{Request{Caller: "mobile", Callee: "checkout", Endpoint: "CreateOrder"}, "any-to-checkout"},
		{Request{Caller: "web", Callee: "payments", Endpoint: "Pay"}, ""},
	}
	for _, tt := range tests {
		got := ""
		if b := tbl.Match(tt.req); b != nil {
			got = b.Name
		}
		if got != tt.want {
			t.Errorf("Match(%+v) = %q, want %q", tt.req, got, tt.want)
		}
	}
}

func TestValidateRejects(t *testing.T) {
	for name, src := range map[string]string{
		"missing callee": "buckets: [{name: a, limit_rps: 1}]",
		"zero limit":     "buckets: [{name: a, match: {callee: x}}]",
		"bad mode":       "buckets: [{name: a, match: {callee: x}, limit_rps: 1, mode: block}]",
		"duplicate":      "buckets: [{name: a, match: {callee: x}, limit_rps: 1}, {name: a, match: {callee: y}, limit_rps: 1}]",
		"same match":     "buckets: [{name: a, match: {callee: x}, limit_rps: 1}, {name: b, match: {callee: x}, limit_rps: 1}]",
		"unknown field":  "buckets: [{name: a, match: {callee: x}, limit: 1}]",
		"bad name":       "buckets: [{name: a.b, match: {callee: x}, limit_rps: 1}]",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(strings.NewReader(src)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
