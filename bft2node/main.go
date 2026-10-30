// Command bft2-node runs ONE HotStuff replica of the bft2 backend
// (github.com/relab/hotstuff v0.5.0) next to a veritas server
// (--consensus=bft2) on the same container, and serves it the local API
// (package wire): POST /submit and the GET /deliver stream of committed
// commands.
//
//	bft2-node --id=2 --peers=1=172.28.0.11:7200,2=172.28.0.12:7200,... \
//	  --listen=0.0.0.0:7200 --api=127.0.0.1:7201
//
// Start-up: the replica server and the local API listen at once (Submit is
// accepted and queued from then on); the replica then waits up to
// --connect-timeout for every peer's server, connects and joins the
// protocol. If that fails it exits 1. On SIGTERM/SIGINT it stops and exits 0.
//
// It is a separate Go module (go 1.23.4, required by relab/hotstuff) so the
// main module keeps its go.mod and Go 1.20 toolchain.
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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/relab/hotstuff"
	"github.com/relab/hotstuff/logging"

	"hybrid/bft2node/api"
	"hybrid/bft2node/node"
)

func main() {
	var (
		id         = flag.Uint("id", 0, "this replica's ID (1..N)")
		peers      = flag.String("peers", "", "all replicas: ID=HOST:PORT,... (IDs exactly 1..N, N >= 2)")
		listen     = flag.String("listen", "", "replica server listen address (default: this replica's --peers address)")
		apiAddr    = flag.String("api", "127.0.0.1:7201", "local API listen address (POST /submit, GET /deliver)")
		keySeed    = flag.String("key-seed", node.DefaultKeySeed, "seed every replica derives all ECDSA keys from (same on every replica)")
		viewTO     = flag.Duration("view-timeout", node.DefaultViewTimeout, "initial view timeout")
		minViewTO  = flag.Duration("min-view-timeout", node.DefaultMinViewTimeout, "lower bound of the adaptive view timeout")
		maxViewTO  = flag.Duration("max-view-timeout", node.DefaultMaxViewTimeout, "upper bound of the adaptive view timeout")
		mult       = flag.Float64("timeout-multiplier", node.DefaultTimeoutMult, "view timeout multiplier after a timeout")
		idle       = flag.Duration("idle-wait", node.DefaultIdleWait, "how long a leader with nothing to propose waits before an empty filler block")
		connectTO  = flag.Duration("connect-timeout", node.DefaultConnectTimeout, "how long to wait for every peer's server at start-up")
		retain     = flag.Int("retain-views", node.DefaultRetainViews, "views of blocks kept in memory below the committed height")
		maxQueue   = flag.Int("max-queue", node.DefaultMaxQueue, "max commands waiting for proposal (Submit then fails)")
		maxQueueB  = flag.Int64("max-queue-bytes", node.DefaultMaxQueueBytes, "max bytes waiting for proposal")
		maxBacklog = flag.Int64("max-backlog-bytes", node.DefaultMaxBacklogBytes, "max committed bytes not yet streamed to veritas (the replica then waits)")
		maxCmd     = flag.Int("max-command-bytes", node.DefaultMaxCommandBytes, "largest command (Veritas block) accepted")
		logLevel   = flag.String("log-level", "warn", "hotstuff library log level: debug|info|warn|error")
		statsEvery = flag.Duration("stats-interval", 10*time.Second, "log view/commit counters this often (0: never)")
	)
	flag.Parse()
	logging.SetLogLevel(*logLevel)

	ps, err := parsePeers(*peers)
	if err != nil {
		log.Fatalf("bft2-node: %v", err)
	}
	if *id == 0 {
		log.Fatalf("bft2-node: --id is required")
	}
	n, err := node.New(node.Config{
		ID:                hotstuff.ID(*id),
		Peers:             ps,
		Listen:            *listen,
		KeySeed:           *keySeed,
		ViewTimeout:       *viewTO,
		MinViewTimeout:    *minViewTO,
		MaxViewTimeout:    *maxViewTO,
		TimeoutMultiplier: *mult,
		IdleWait:          *idle,
		ConnectTimeout:    *connectTO,
		RetainViews:       *retain,
		MaxQueue:          *maxQueue,
		MaxQueueBytes:     *maxQueueB,
		MaxBacklogBytes:   *maxBacklog,
		MaxCommandBytes:   *maxCmd,
	})
	if err != nil {
		log.Fatalf("%v", err)
	}

	lis, err := net.Listen("tcp", *apiAddr)
	if err != nil {
		n.Stop()
		log.Fatalf("bft2-node: local API listen %s: %v", *apiAddr, err)
	}
	// No read/write timeouts: /deliver is a never-ending stream.
	srv := &http.Server{Handler: api.New(n), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("bft2-node: local API: %v", err)
		}
	}()
	log.Printf("bft2-node %d: local API on %s, waiting for %d replicas", *id, *apiAddr, len(ps))

	started := make(chan error, 1)
	go func() { started <- n.Start(context.Background()) }()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
	select {
	case err := <-started:
		if err != nil {
			srv.Close()
			n.Stop()
			log.Fatalf("%v", err)
		}
		log.Printf("bft2-node %d: connected, running", *id)
		if *statsEvery > 0 {
			go func() {
				for range time.Tick(*statsEvery) {
					log.Printf("bft2-node %d: %v", *id, n.Stats())
				}
			}()
		}
	case s := <-sig:
		log.Printf("bft2-node %d: %v before start, stopping", *id, s)
		srv.Close()
		n.Stop()
		return
	}
	s := <-sig
	log.Printf("bft2-node %d: %v, stopping (%v)", *id, s, n.Stats())
	srv.Close() // ends the /deliver stream
	n.Stop()
}

// parsePeers parses --peers: comma-separated ID=HOST:PORT (empty entries
// skipped). node.New checks that the IDs are exactly 1..N.
func parsePeers(s string) ([]node.Peer, error) {
	var ps []node.Peer
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		eq := strings.Index(item, "=")
		if eq <= 0 || eq == len(item)-1 {
			return nil, fmt.Errorf("--peers entry %q: want ID=HOST:PORT", item)
		}
		id, err := strconv.ParseUint(item[:eq], 10, 32)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("--peers entry %q: ID must be a positive integer", item)
		}
		if _, _, err := net.SplitHostPort(item[eq+1:]); err != nil {
			return nil, fmt.Errorf("--peers entry %q: %v", item, err)
		}
		ps = append(ps, node.Peer{ID: hotstuff.ID(id), Addr: item[eq+1:]})
	}
	if len(ps) == 0 {
		return nil, fmt.Errorf("--peers %q: no replicas", s)
	}
	return ps, nil
}
