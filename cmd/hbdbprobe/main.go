// Command hbdbprobe is the cluster health probe used by experiments/cluster/health.sh.
//
//	--mode=verify  calls the gRPC Verify("health") RPC until it succeeds.
//	--mode=setget  Sets a probe key to a unique value, then Gets it until the
//	               value is visible (this proves the write was committed).
//
// Every failed attempt is retried until --timeout (an overall deadline) has
// passed. Exit status: 0 on success, 1 on failure.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/driver"
)

const retryPause = 300 * time.Millisecond

var (
	addr    = flag.String("addr", "", "veritas node gRPC address (host:port)")
	tsoAddr = flag.String("tso-addr", "", "TSO address for --mode=setget (empty: no TSO, e.g. tm-orig)")
	mode    = flag.String("mode", "verify", "probe mode: verify or setget")
	timeout = flag.Duration("timeout", 60*time.Second, "overall deadline; failed attempts are retried until it")
	attempt = flag.Duration("attempt-timeout", 10*time.Second, "deadline of a single RPC attempt")
)

func logf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "hbdbprobe[%s %s]: %s\n", *mode, *addr, fmt.Sprintf(format, args...))
}

func main() {
	flag.Parse()
	if *addr == "" {
		logf("--addr is required")
		os.Exit(1)
	}
	var probe func(ctx context.Context) error
	switch *mode {
	case "verify":
		probe = probeVerify
	case "setget":
		probe = probeSetGet
	default:
		logf("unknown --mode %q (want verify or setget)", *mode)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Run the probe in its own goroutine so that a call that ignores the
	// context (e.g. a TSO round trip) cannot hold us past the deadline.
	done := make(chan error, 1)
	go func() { done <- retry(ctx, probe) }()
	select {
	case err := <-done:
		if err != nil {
			logf("FAILED: %v", err)
			os.Exit(1)
		}
		logf("ok")
	case <-ctx.Done():
		logf("FAILED: no success within %v", *timeout)
		os.Exit(1)
	}
}

// retry calls probe until it succeeds or ctx is done; it returns the last error.
func retry(ctx context.Context, probe func(ctx context.Context) error) error {
	var last error
	for n := 1; ; n++ {
		err := safe(ctx, probe)
		if err == nil {
			return nil
		}
		last = err
		logf("attempt %d: %v", n, err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("deadline reached, last error: %w", last)
		case <-time.After(retryPause):
		}
	}
}

// safe turns a panic inside one attempt (e.g. from a broken TSO client) into an error.
func safe(ctx context.Context, probe func(ctx context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return probe(ctx)
}

func attemptCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, *attempt)
}

func probeVerify(ctx context.Context) error {
	cc, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer cc.Close()
	actx, cancel := attemptCtx(ctx)
	defer cancel()
	res, err := pbv.NewNodeClient(cc).Verify(actx, &pbv.VerifyRequest{Key: "health"})
	if err != nil {
		return err
	}
	if res == nil {
		return errors.New("nil VerifyResponse")
	}
	return nil
}

func probeSetGet(ctx context.Context) error {
	d, err := driver.Open(*addr, *tsoAddr, "hbdbprobe")
	if err != nil {
		return err
	}
	defer d.Close()

	// Alphanumeric only: veritastm splits stored transactions on '='.
	key := fmt.Sprintf("hbdbprobe%d", time.Now().UnixNano())
	val := fmt.Sprintf("v%d", time.Now().UnixNano())

	sctx, cancel := attemptCtx(ctx)
	err = d.Set(sctx, key, val)
	cancel()
	if err != nil {
		return fmt.Errorf("set: %w", err)
	}

	// The write may become visible later (kafka-orig commits asynchronously).
	var last error
	for {
		gctx, cancel := attemptCtx(ctx)
		got, err := d.Get(gctx, key)
		cancel()
		switch {
		case err != nil:
			last = fmt.Errorf("get: %w", err)
		case got == val:
			return nil
		default:
			last = fmt.Errorf("get: value %q, want %q", got, val)
		}
		select {
		case <-ctx.Done():
			return last
		case <-time.After(retryPause):
		}
	}
}
