package policy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProtoRoundTripAndVersion(t *testing.T) {
	pf, err := Parse(strings.NewReader(`
buckets:
  - name: a
    match: {caller: web, callee: checkout, endpoint: Pay}
    limit_rps: 10
    scope: zonal
    priority: high
    mode: enforce
`))
	if err != nil {
		t.Fatal(err)
	}
	ps := pf.ToProto()
	back, version, err := FromProto(ps)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := back.Buckets[0], pf.Buckets[0]; got != want {
		t.Fatalf("round trip: got %+v, want %+v", got, want)
	}
	if version != Version(ps) || version == 0 {
		t.Fatalf("version = %d, want Version(ps) = %d", version, Version(ps))
	}
	if again := pf.ToProto(); again.GetVersion() != ps.GetVersion() {
		t.Fatal("same policy produced a different version")
	}
	pf.Buckets[0].LimitRPS = 11
	if pf.ToProto().GetVersion() == ps.GetVersion() {
		t.Fatal("changed policy kept the same version")
	}
}

func TestLoadPathsMergesAndValidatesAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a.yaml", "buckets: [{name: a, match: {callee: x}, limit_rps: 1}]")
	write("b.yml", "buckets: [{name: b, match: {callee: y}, limit_rps: 1}]")
	write("notes.txt", "ignored")

	pf, err := LoadPaths(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(pf.Buckets) != 2 {
		t.Fatalf("got %d buckets, want 2", len(pf.Buckets))
	}

	write("c.yaml", "buckets: [{name: a, match: {callee: z}, limit_rps: 1}]")
	if _, err := LoadPaths(dir); err == nil {
		t.Fatal("duplicate name across files accepted")
	}
}
