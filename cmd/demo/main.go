// Command demo runs the Veritas Live Demo: a small HTTP server with a browser
// UI that drives a Veritas cluster — writing transactions, generating load,
// verifying Merkle proofs and injecting faults.
//
//	demo --mode=sim                   # in-process cluster on memnet (default)
//	demo --mode=sim --listen 127.0.0.1:9090 --n 4 --slow-ms 200
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
}

func main() {
	var o options
	flag.StringVar(&o.mode, "mode", "sim", "cluster mode: sim (in-process) or docker")
	flag.StringVar(&o.listen, "listen", "127.0.0.1:8080", "address of the demo's web server")
	flag.IntVar(&o.n, "n", 4, "number of nodes (sim mode)")
	flag.IntVar(&o.slowMs, "slow-ms", 200, "extra delay of the slow fault in milliseconds (sim mode)")
	flag.Parse()

	c, err := newCluster(o)
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
		return nil, errors.New("--mode=docker is not implemented yet; use --mode=sim")
	default:
		return nil, fmt.Errorf("--mode must be sim or docker, got %q", o.mode)
	}
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
