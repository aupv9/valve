package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aupv9/valve/internal/envoy"
	"github.com/aupv9/valve/internal/policy"
)

func envoyConfig(args []string) error {
	flags := flag.NewFlagSet("envoy", flag.ExitOnError)
	policyPath := flags.String("policy", "policies", "policy file or directory")
	service := flags.String("service", "", "the callee this Envoy sidecar fronts (required)")
	callerHeader := flags.String("caller-header", "x-valve-caller", "request header carrying the caller name")
	spiffe := flags.Bool("caller-spiffe", false, "identify the caller by the SPIFFE service account in x-forwarded-client-cert instead")
	retryAfter := flags.Int("retry-after", envoy.DefaultRetryAfter, "Retry-After seconds sent with 429 responses")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *service == "" {
		return fmt.Errorf("-service is required")
	}
	pf, err := policy.LoadPaths(*policyPath)
	if err != nil {
		return err
	}
	frag, err := envoy.Config(pf, *service, envoy.Options{
		Caller:     envoy.CallerMatch{Header: *callerHeader, SPIFFE: *spiffe},
		RetryAfter: *retryAfter,
	})
	if err != nil {
		return err
	}
	for _, w := range frag.Warnings {
		fmt.Fprintln(os.Stderr, "warning:", w)
	}
	out, err := frag.YAML()
	if err != nil {
		return err
	}
	fmt.Printf("# Valve buckets for service %q (policy version %s).\n", *service, pf.ToProto().GetVersion())
	fmt.Println("# http_filters go before envoy.filters.http.router; virtual_clusters go in the service's virtual host;")
	fmt.Println("# local_reply_config goes in the HTTP connection manager (it adds Retry-After to Valve's 429s).")
	fmt.Println("# Envoy must load the RTDS layer \"" + envoy.RuntimeLayer + "\" from the Valve agent.")
	_, err = os.Stdout.Write(out)
	return err
}
