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
// If the replica can no longer commit (--max-commit-failures failed commits
// in a row: it fell behind past the retained blocks) it exits 3.
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
		retainB    = flag.Int64("retain-bytes", node.DefaultRetainBytes, "bytes of blocks kept in memory for lagging replicas (oldest views evicted first)")
		minRetain  = flag.Int("min-retain-views", node.DefaultMinRetainViews, "views below the committed height never evicted, whatever their size")
		maxFails   = flag.Int("max-commit-failures", node.DefaultMaxCommitFails, "exit (status 3) after this many failed commits in a row")
		maxQueue   = flag.Int("max-queue", node.DefaultMaxQueue, "max commands waiting for proposal (Submit then fails)")
		maxQueueB  = flag.Int64("max-queue-bytes", node.DefaultMaxQueueBytes, "max bytes waiting for proposal")
		maxBacklog = flag.Int64("max-backlog-bytes", node.DefaultMaxBacklogBytes, "max committed bytes not yet streamed to veritas (the replica then waits)")
		maxCmd     = flag.Int("max-command-bytes", node.DefaultMaxCommandBytes, "largest command (Veritas block) accepted")
		logLevel   = flag.String("log-level", "warn", "hotstuff library log level: debug|info|warn|error")
		statsEvery = flag.Duration("stats-interval", 10*time.Second, "log view/commit counters this often (0: never)")
		fwdPort    = flag.Int("forward-port", 0, "port of the forward endpoint (POST /forward on 0.0.0.0) on every replica; every /submit is forwarded to the other replicas' (host of --peers):PORT so any leader can propose it (0: no forwarding, phase-1 behaviour)")
		requeueV   = flag.Int("requeue-views", node.DefaultRequeueViews, "a pending command seen in a proposal and not committed this many views later is queued again")
		dedupWin   = flag.Int("dedup-window", node.DefaultDedupWindow, "committed command ids remembered to deliver a command committed twice only once")
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
	if *fwdPort < 0 || *fwdPort > 65535 {
		log.Fatalf("bft2-node: --forward-port must be 0..65535")
	}
	if *fwdPort > 0 {
		if ps, err = withForwardAddrs(ps, *fwdPort); err != nil {
			log.Fatalf("bft2-node: %v", err)
		}
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
		RetainBytes:       *retainB,
		MinRetainViews:    *minRetain,
		MaxCommitFailures: *maxFails,
		MaxQueue:          *maxQueue,
		MaxQueueBytes:     *maxQueueB,
		MaxBacklogBytes:   *maxBacklog,
		MaxCommandBytes:   *maxCmd,
		RequeueViews:      *requeueV,
		DedupWindow:       *dedupWin,
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
	var fwdSrv *http.Server
	if *fwdPort > 0 {
		fl, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *fwdPort))
		if err != nil {
			srv.Close()
			n.Stop()
			log.Fatalf("bft2-node: forward listen :%d: %v", *fwdPort, err)
		}
		fwdSrv = &http.Server{Handler: api.ForwardHandler(n), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := fwdSrv.Serve(fl); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("bft2-node: forward endpoint: %v", err)
			}
		}()
		defer fwdSrv.Close()
		log.Printf("bft2-node %d: forward endpoint on :%d", *id, *fwdPort)
	}
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
	select {
	case s := <-sig:
		log.Printf("bft2-node %d: %v, stopping (%v)", *id, s, n.Stats())
		srv.Close() // ends the /deliver stream
		n.Stop()
	case <-n.Failed():
		// The replica can no longer commit: exit so the container dies and
		// the run is marked failed instead of measuring a smaller cluster.
		log.Printf("bft2-node %d: replica failed, exiting (%v)", *id, n.Stats())
		srv.Close()
		n.Stop()
		os.Exit(3)
	}
}

// withForwardAddrs sets every peer's ForwardAddr to its --peers host and port.
func withForwardAddrs(ps []node.Peer, port int) ([]node.Peer, error) {
	out := append([]node.Peer(nil), ps...)
	for i := range out {
		host, _, err := net.SplitHostPort(out[i].Addr)
		if err != nil {
			return nil, fmt.Errorf("--peers %s: %v", out[i].Addr, err)
		}
		out[i].ForwardAddr = net.JoinHostPort(host, strconv.Itoa(port))
	}
	return out, nil
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
