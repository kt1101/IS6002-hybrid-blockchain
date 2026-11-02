// Command demo runs the Veritas Live Demo: a small HTTP server with a browser
// UI that drives a Veritas cluster — writing transactions, generating load,
// verifying Merkle proofs and injecting faults.
//
//	demo --mode=sim                   # in-process cluster on memnet (default)
//	demo --mode=sim --listen 127.0.0.1:9090 --n 4 --slow-ms 200
//	demo --mode=docker --backend=bft2 --n=4   # cluster from experiments/demo_up.sh
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"hybrid/demo/api"
	"hybrid/demo/cluster"
	"hybrid/demo/hub"
	"hybrid/demo/load"
	"hybrid/demo/web"
)

const shutdownTimeout = 5 * time.Second

type options struct {
	mode   string
	listen string
	n      int
	slowMs int

	// docker mode
	backend      string
	grpcBase     int
	eventsBase   int
	tsoAddr      string
	faultScript  string
	armedCorrupt string
	armedSilent  string
}

// dockerBackends are the backends whose nodes run the demo observer.
var dockerBackends = []string{"kafka", "raft", "tendermint", "bft2"}

func main() {
	var o options
	flag.StringVar(&o.mode, "mode", "sim", "cluster mode: sim (in-process) or docker")
	flag.StringVar(&o.listen, "listen", "127.0.0.1:8080", "address of the demo's web server")
	flag.IntVar(&o.n, "n", 4, "number of nodes (sim mode)")
	flag.IntVar(&o.slowMs, "slow-ms", 200, "extra delay of the slow fault in milliseconds (sim mode)")
	flag.StringVar(&o.backend, "backend", "", "docker mode: consensus backend of the cluster ("+strings.Join(dockerBackends, ", ")+")")
	flag.IntVar(&o.grpcBase, "grpc-base", 19900, "docker mode: node i serves gRPC on 127.0.0.1:(grpc-base+i)")
	flag.IntVar(&o.eventsBase, "events-base", 19800, "docker mode: node i serves its observer on 127.0.0.1:(events-base+i)")
	flag.StringVar(&o.tsoAddr, "tso-addr", "", `docker mode: TSO address (default 127.0.0.1:19870 for kafka and raft, none for tendermint and bft2; "none" = no TSO)`)
	flag.StringVar(&o.faultScript, "fault-script", "", "docker mode: fault script (default experiments/demo_fault.sh)")
	flag.StringVar(&o.armedCorrupt, "armed-corrupt", "node3", `docker mode: nodes started with --fault=corrupt, comma-separated ("none" = no node)`)
	flag.StringVar(&o.armedSilent, "armed-silent", "", `docker mode: nodes started with --fault=silent (default node4 for kafka, none otherwise; "none" = no node)`)
	flag.Parse()

	c, err := newCluster(o)
	if errors.Is(err, cluster.ErrNoNodes) {
		fmt.Fprintf(os.Stderr, "demo: no node answered at 127.0.0.1:%d..; start the cluster with: BACKEND=%s N=%d experiments/demo_up.sh\n",
			o.eventsBase+1, o.backend, o.n)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(2)
	}
	if err := run(o, c); err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

// newCluster builds the cluster for the chosen mode.
func newCluster(o options) (cluster.Cluster, error) {
	switch o.mode {
	case "sim":
		if o.n < 1 {
			return nil, fmt.Errorf("--n must be at least 1, got %d", o.n)
		}
		if o.slowMs < 0 {
			return nil, fmt.Errorf("--slow-ms must not be negative, got %d", o.slowMs)
		}
		return cluster.NewSim(cluster.SimConfig{N: o.n, SlowDelay: time.Duration(o.slowMs) * time.Millisecond})
	case "docker":
		return newDocker(o)
	default:
		return nil, fmt.Errorf("--mode must be sim or docker, got %q", o.mode)
	}
}

// newDocker connects to a cluster started by experiments/demo_up.sh.
func newDocker(o options) (cluster.Cluster, error) {
	known := false
	for _, b := range dockerBackends {
		known = known || o.backend == b
	}
	if !known {
		return nil, fmt.Errorf("--backend is required in docker mode, one of: %s (got %q)", strings.Join(dockerBackends, ", "), o.backend)
	}
	if o.n < 1 {
		return nil, fmt.Errorf("--n must be at least 1, got %d", o.n)
	}
	if o.grpcBase < 1 || o.eventsBase < 1 {
		return nil, errors.New("--grpc-base and --events-base must be positive")
	}
	tso := o.tsoAddr
	switch {
	case tso == "none":
		tso = ""
	case tso == "" && (o.backend == "kafka" || o.backend == "raft"):
		tso = "127.0.0.1:19870"
	}
	cfg := cluster.DockerConfig{
		Backend:      o.backend,
		N:            o.n,
		GRPCAddr:     func(i int) string { return fmt.Sprintf("127.0.0.1:%d", o.grpcBase+i) },
		EventsURL:    func(i int) string { return fmt.Sprintf("http://127.0.0.1:%d", o.eventsBase+i) },
		TSOAddr:      tso,
		FaultScript:  o.faultScript,
		ArmedCorrupt: nodeList(o.armedCorrupt),
		ArmedSilent:  nodeList(o.armedSilent),
	}
	return cluster.NewDocker(context.Background(), cfg)
}

// nodeList parses a comma-separated node list: "" is nil (the cluster's
// default), "none" an empty list.
func nodeList(s string) []string {
	if s == "" {
		return nil
	}
	list := []string{}
	for _, n := range strings.Split(s, ",") {
		if n = strings.TrimSpace(n); n != "" && n != "none" {
			list = append(list, n)
		}
	}
	return list
}

// run serves the demo for c until SIGINT or SIGTERM, then shuts down: the
// server stops accepting, the load stops and the cluster closes.
func run(o options, c cluster.Cluster) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	h := hub.New(c, time.Now)
	hubDone := make(chan struct{})
	go func() {
		defer close(hubDone)
		h.Run(ctx)
	}()
	runner := load.NewRunner(c, h.RecordOp)

	ln, err := net.Listen("tcp", o.listen)
	if err != nil {
		stop()
		<-hubDone
		_ = c.Close()
		return err
	}
	// No WriteTimeout: /api/events is a long-lived stream.
	srv := &http.Server{
		Handler:           api.New(c, h, runner, web.Handler()),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		// Requests (above all the SSE streams) end when a signal arrives.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	fmt.Printf("Veritas Live Demo listening on http://%s\n", ln.Addr())

	select {
	case <-ctx.Done():
		log.Print("demo: shutting down")
		err = nil
	case err = <-serveErr:
	}
	stop()

	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	// Shutdown waits for in-flight requests; if one outlives the timeout,
	// Close cuts it.
	if serr := srv.Shutdown(shutCtx); serr != nil {
		_ = srv.Close()
	}
	runner.Stop()
	<-hubDone
	if cerr := c.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
