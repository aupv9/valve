// Command configurator validates and compiles policy files. CI runs it on
// every merge request.
//
// TODO: simulate impact against the last 7 days of traffic, and publish
// versioned config to sidecars over xDS (go-control-plane).
package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"ratelimit/internal/policy"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: configurator validate <policy.yaml>...\n")
	}
	flag.Parse()
	if flag.NArg() < 2 || flag.Arg(0) != "validate" {
		flag.Usage()
		os.Exit(2)
	}

	failed := false
	for _, path := range flag.Args()[1:] {
		pf, err := policy.Load(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
			failed = true
			continue
		}
		fmt.Printf("%s: %d buckets OK\n", path, len(pf.Buckets))
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "  ID\tNAME\tCALLER\tCALLEE\tENDPOINT\tLIMIT_RPS\tSCOPE\tPRIORITY\tMODE")
		for _, b := range pf.Buckets {
			fmt.Fprintf(w, "  %08x\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
				b.ID, b.Name, or(b.Match.Caller), b.Match.Callee, or(b.Match.Endpoint),
				b.LimitRPS, b.Scope, b.Priority, b.Mode)
		}
		w.Flush()
	}
	if failed {
		os.Exit(1)
	}
}

func or(s string) string {
	if s == "" {
		return "*"
	}
	return s
}
