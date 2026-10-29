// Command auditor collects the ledger root digest of every replica over gRPC
// (Verify("audit")) and reports the replicas that differ from the majority.
//
//	auditor --addrs 172.28.0.11:1990,172.28.0.12:1990,... --once
//
// The i-th address (1-based) is named node<i>; give the addresses in node
// order. Output on stdout, one line:
//
//	CONSISTENT                  every replica has the same root digest
//	DIVERGENT node3 [node5 ...] these replicas differ from the majority digest
//	                            (with no strict majority, e.g. 2-2, all of them)
//
// Exit status: 0 CONSISTENT, 1 DIVERGENT, 2 usage error or a replica could not
// be queried (the message on stderr names it). The periodic mode
// (--interval, --out) is not implemented yet.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"hybrid/veritas/audit"
)

// collectFunc collects the root digest of every node, keyed by node name.
type collectFunc func(ctx context.Context, addrs []string) (map[string][]byte, error)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, audit.CollectRoots))
}

// run is main with its inputs and outputs injected; it returns the exit status.
func run(args []string, stdout, stderr io.Writer, collect collectFunc) int {
	logf := func(format string, a ...interface{}) {
		fmt.Fprintf(stderr, "auditor: %s\n", fmt.Sprintf(format, a...))
	}
	fs := flag.NewFlagSet("auditor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addrs := fs.String("addrs", "", "comma-separated veritas node gRPC addresses, in node order (the i-th is node<i>)")
	once := fs.Bool("once", false, "collect once, print the verdict and exit (required)")
	timeout := fs.Duration("timeout", 30*time.Second, "overall deadline of the collection")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	list, err := audit.ParseAddrs(*addrs)
	if err != nil {
		logf("--addrs: %v", err)
		return 2
	}
	if !*once {
		logf("--once is required (periodic mode is not implemented yet)")
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	roots, err := collect(ctx, list)
	if err != nil {
		logf("%v", err)
		return 2
	}
	bad := audit.Divergent(roots)
	if len(bad) == 0 {
		fmt.Fprintln(stdout, "CONSISTENT")
		return 0
	}
	if !audit.HasMajority(roots) {
		logf("no strict majority digest among %d nodes: all nodes reported as divergent", len(roots))
	}
	fmt.Fprintln(stdout, "DIVERGENT "+strings.Join(bad, " "))
	return 1
}
