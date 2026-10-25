// Command configurator owns Valve's policy lifecycle:
//
//	configurator validate <file|dir>...   check policies (CI runs this on every merge request)
//	configurator serve [flags]             serve policies and the kill switch over xDS
//	configurator envoy -service <name>     print Envoy config for one service's buckets
package main

import (
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"

	"github.com/aupv9/valve/internal/policy"
)

const usage = `usage:
  configurator validate <file|dir>...
  configurator serve [flags]      (configurator serve -h for flags)
  configurator envoy -service <name> [flags]
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	var err error
	switch os.Args[1] {
	case "validate":
		err = validate(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:], log)
	case "envoy":
		err = envoyConfig(os.Args[2:])
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		log.Error("configurator failed", "cmd", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func validate(paths []string) error {
	if len(paths) == 0 {
		return fmt.Errorf("validate needs at least one file or directory")
	}
	// Validate as one set: names and matches must be unique across files.
	pf, err := policy.LoadPaths(paths...)
	if err != nil {
		return err
	}
	fmt.Printf("%d buckets OK (version %s)\n", len(pf.Buckets), pf.ToProto().GetVersion())
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  ID\tNAME\tCALLER\tCALLEE\tENDPOINT\tLIMIT_RPS\tSCOPE\tPRIORITY\tMODE")
	for _, b := range pf.Buckets {
		fmt.Fprintf(w, "  %08x\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
			b.ID, b.Name, or(b.Match.Caller), b.Match.Callee, or(b.Match.Endpoint),
			b.LimitRPS, b.Scope, b.Priority, b.Mode)
	}
	return w.Flush()
}

func or(s string) string {
	if s == "" {
		return "*"
	}
	return s
}
