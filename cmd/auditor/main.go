// Command auditor collects the ledger root digest of every replica over gRPC
// (Verify("audit")) and reports the replicas that differ from the majority.
//
//	auditor --addrs 172.28.0.11:1990,172.28.0.12:1990,... --once
//	auditor --addrs ... --interval 1s --out /results/<EXP>/<tag>.audit
//
// The i-th address (1-based) is named node<i>; give the addresses in node
// order.
//
// --once: collect once, print on stdout one line
//
//	CONSISTENT                  every replica has the same root digest
//	DIVERGENT node3 [node5 ...] these replicas differ from the majority digest
//	                            (with no strict majority, e.g. 2-2, all of them)
//
// and exit: 0 CONSISTENT, 1 DIVERGENT, 2 usage error or a replica could not be
// queried (the message on stderr names it).
//
// --interval D (periodic mode, RQ3): collect every D (first tick at once)
// until SIGINT/SIGTERM, then exit 0. Every tick appends one line to --out
// (default stdout), unbuffered, so the file is complete up to the last tick
// even if the process is killed:
//
//	<unix_ts>,<nodes>
//
// unix_ts is the start of the tick in seconds with millisecond fraction
// (1730000000.123); <nodes> is empty when the tick found the replicas
// consistent, otherwise the tokens below joined with '+' (so the line is
// always a 2-field CSV):
//
//	node3        divergent: its digest differs from the majority digest of the
//	             replicas that answered (same rule as --once)
//	down:node4   unreachable: the node did not answer within --node-timeout
//
// Examples: "1730000001.002," consistent; "1730000002.001,node4" node4
// divergent; "1730000003.003,down:node4" node4 stopped (F1), the others agree;
// "1730000004.002,node3+down:node4". An unreachable node is NEVER counted as
// divergent: the majority is computed over the replicas that answered, so a
// stopped node (F1) shows up only as down:. If the answering replicas have no
// strict majority all of them are listed, as in --once. A tick whose whole
// collection failed for another reason marks every node down:. The auditor
// never exits because a node is unreachable. Ticks are sequential: while a
// node does not answer (a stopped container often does not even refuse the
// connection) a tick lasts up to --node-timeout (default 2s), so lines come
// that much less often than --interval; a tick cut short by SIGTERM writes
// no line.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
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
	once := fs.Bool("once", false, "collect once, print the verdict and exit (one of --once, --interval is required)")
	interval := fs.Duration("interval", 0, "periodic mode: collect every interval until SIGINT/SIGTERM and write one `unix_ts,divergent_nodes` line per tick")
	out := fs.String("out", "", "periodic mode: file the tick lines are appended to (default stdout)")
	count := fs.Int("count", 0, "periodic mode: stop after this many ticks (0 = run until SIGINT/SIGTERM)")
	nodeTimeout := fs.Duration("node-timeout", 2*time.Second, "periodic mode: how long a node may take to answer before it is reported as down:<node>")
	timeout := fs.Duration("timeout", 30*time.Second, "overall deadline of one collection")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	list, err := audit.ParseAddrs(*addrs)
	if err != nil {
		logf("--addrs: %v", err)
		return 2
	}
	periodic := *interval != 0
	switch {
	case *once && periodic:
		logf("--once and --interval are mutually exclusive")
		return 2
	case !*once && !periodic:
		logf("--once or --interval is required")
		return 2
	case periodic && *interval < 0:
		logf("--interval must be positive, got %v", *interval)
		return 2
	case periodic && *nodeTimeout <= 0:
		logf("--node-timeout must be positive, got %v", *nodeTimeout)
		return 2
	case periodic && *count < 0:
		logf("--count must be >= 0, got %d", *count)
		return 2
	case *once && (*out != "" || *count != 0):
		logf("--out and --count need --interval")
		return 2
	}

	if periodic {
		w := stdout
		if *out != "" {
			f, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				logf("--out: %v", err)
				return 2
			}
			defer f.Close()
			w = f // unbuffered: every tick line reaches the file at once
		}
		old := audit.PerNodeTimeout
		audit.PerNodeTimeout = *nodeTimeout
		defer func() { audit.PerNodeTimeout = old }()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(sig)
		go func() {
			select {
			case <-sig:
				cancel()
			case <-ctx.Done():
			}
		}()
		if err := runPeriodic(ctx, w, list, *interval, *timeout, *count, collect, time.Now); err != nil {
			logf("writing the audit lines: %v", err)
			return 2
		}
		return 0
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

// runPeriodic collects every interval (the first tick at once) and writes one
// formatTick line per tick to w, until ctx is cancelled or count ticks are
// done (count 0 = no limit). The only error it returns is a failed write. A
// tick interrupted by the cancellation writes nothing. Ticks are sequential:
// one slower than interval delays the next, it never overlaps it.
func runPeriodic(ctx context.Context, w io.Writer, addrs []string, interval, timeout time.Duration,
	count int, collect collectFunc, now func() time.Time) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for n := 0; ; {
		start := now()
		cctx, cancel := context.WithTimeout(ctx, timeout)
		roots, err := collect(cctx, addrs)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if _, werr := fmt.Fprintln(w, formatTick(start, roots, err, len(addrs))); werr != nil {
			return werr
		}
		n++
		if count > 0 && n >= count {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// formatTick renders the line of one tick (see the package comment):
// "<unix_ts>,<nodes>". roots/err are the result of the collection of nNodes
// nodes. A *audit.CollectError (some nodes did not answer) is not a failure
// of the tick: the nodes that answered are compared among themselves and the
// others are listed as down:<node>. Any other error marks every node down.
func formatTick(ts time.Time, roots map[string][]byte, err error, nNodes int) string {
	var tokens []string
	var ce *audit.CollectError
	switch {
	case err == nil:
		tokens = audit.Divergent(roots)
	case errors.As(err, &ce):
		tokens = audit.Divergent(ce.Roots)
		for _, name := range ce.Failed {
			tokens = append(tokens, "down:"+name)
		}
	default:
		for i := 1; i <= nNodes; i++ {
			tokens = append(tokens, "down:"+audit.NodeName(i))
		}
	}
	return fmt.Sprintf("%d.%03d,%s", ts.Unix(), ts.Nanosecond()/int(time.Millisecond), strings.Join(tokens, "+"))
}
