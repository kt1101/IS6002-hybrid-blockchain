// Command veritas runs one Veritas node.
//
// Without --consensus it runs the ORIGINAL Kafka approve-all server
// (veritas/server.go) exactly as before, with the same flags and behaviour,
// so the kafka-orig baseline keeps using this binary (veritas-kafka).
//
// With --consensus=<backend> it runs the shared core: core.Executor +
// core.Server over the chosen consensus backend: "kafka" (consensus/kafka),
// "raft" (consensus/raft, configured with the --raft-* flags) or
// "tendermint" (consensus/tendermint: an ABCI socket server for a local
// tendermint process, --tm-abci-socket and --tm-rpc-addr; it requires
// --version-mode=ordered) or "bft2" (consensus/bft2: a client of the local
// API of the bft2-node HotStuff sidecar on the same container, --bft2-api;
// it requires --version-mode=ordered).
// On SIGTERM/SIGINT the core path closes the server, writes its time
// breakdown to --breakdown-out and exits. Every SIGUSR2 writes a snapshot of
// the (cumulative) breakdown next to it, the k-th one to
// <breakdown-out without .json>.snap<k>.json (the harness signals at the
// start and at the end of the measurement window).
//
// Fault injection (RQ3, core path only): --fault=silent (kafka only: the node
// sends no votes, F2) or --fault=corrupt (every applied value gets the
// benchmark.CorruptMarker suffix, F4) turns on --fault-after after the
// process start, or at once on SIGUSR1 (the experiment harness passes a very
// large --fault-after and signals the node at the injection time). A SIGUSR1
// activation writes the spec to --fault-active-file, which the harness reads
// back to confirm it.
package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"

	"hybrid/consensus"
	bft2cons "hybrid/consensus/bft2"
	kafkacons "hybrid/consensus/kafka"
	raftcons "hybrid/consensus/raft"
	tmcons "hybrid/consensus/tendermint"
	"hybrid/dbconn"
	"hybrid/kafkarole"
	pbv "hybrid/proto/veritas"
	"hybrid/veritas"
	"hybrid/veritas/benchmark"
	"hybrid/veritas/core"
	"hybrid/veritas/ledger"
)

var (
	signature  = kingpin.Flag("signature", "server signature").Required().String()
	blockSize  = kingpin.Flag("blk-size", "block size").Default("100").Int()
	parties    = kingpin.Flag("parties", "party1,party2,...").Required().String()
	addr       = kingpin.Flag("addr", "server address").Required().String()
	kafkaAddr  = kingpin.Flag("kafka-addr", "kafka server address (required without --consensus and with --consensus=kafka)").String()
	kafkaGroup = kingpin.Flag("kafka-group", "kafka group id (required without --consensus; with --consensus=kafka the group is the signature)").String()
	kafkaTopic = kingpin.Flag("kafka-topic", "kafka topic (required without --consensus and with --consensus=kafka)").String()
	redisAddr  = kingpin.Flag("redis-addr", "redis server address").Required().String()
	redisDb    = kingpin.Flag("redis-db", "redis db number").Required().Int()
	redisPwd   = kingpin.Flag("redis-pwd", "redis password").String()
	ledgerPath = kingpin.Flag("ledger-path", "ledger path").Required().String()

	// Core path only (ignored without --consensus).
	consensusName = kingpin.Flag("consensus", "consensus backend: kafka|raft|tendermint|bft2 (empty: the original veritas server)").Default("").String()
	commitMode    = kingpin.Flag("commit-mode", "core: when Set returns: sync (block applied on this node) or async (queued)").Default("sync").String()
	versionMode   = kingpin.Flag("version-mode", "core: client (client versions, stale writes abort) or ordered (versions from the consensus order)").Default("client").String()
	batchFlush    = kingpin.Flag("batch-flush", "core: flush interval of the batcher (a partial block is sent after this long; E1-E6 keep the original 1s)").Default("1s").Duration()
	nodeID        = kingpin.Flag("node-id", "core: node id in [0, 2^23), unique per deployment (high bits of every SharedLog seq)").Default("-1").Int()
	breakdownOut  = kingpin.Flag("breakdown-out", "core: breakdown JSON written on SIGTERM/SIGINT (default /results/breakdown-node<node-id>.json)").Default("").String()
	faultSpec     = kingpin.Flag("fault", "core: injected replica fault: none, silent (--consensus=kafka only: send no votes) or corrupt (append "+benchmark.CorruptMarker+" to every applied value)").Default(core.FaultNone).String()
	faultAfter    = kingpin.Flag("fault-after", "core: the fault turns on this long after the process start (e.g. 60s); SIGUSR1 turns it on at once").Default("0s").Duration()
	faultActive   = kingpin.Flag("fault-active-file", "core: written (the --fault spec) when SIGUSR1 activates the fault; removed at start (empty: none)").Default(defaultFaultActiveFile).String()

	// --consensus=raft only. The raft server ID is --signature.
	raftPeers     = kingpin.Flag("raft-peers", "raft: the full static cluster, ID=RAFTADDR/FORWARDADDR,... (ID = a node's signature; RAFTADDR its raft transport address, FORWARDADDR its submit-forwarding HTTP address)").Default("").String()
	raftBind      = kingpin.Flag("raft-bind", "raft: listen address of the raft transport (default: this node's RAFTADDR)").Default("").String()
	raftDir       = kingpin.Flag("raft-dir", "raft: data dir (bolt log store and snapshots)").Default("").String()
	raftBootstrap = kingpin.Flag("raft-bootstrap", "raft: bootstrap the cluster with all of --raft-peers (exactly one node, node1)").Bool()

	// --consensus=tendermint only (same sockets as scripts/start_veritas_tendermint.sh).
	tmABCISocket = kingpin.Flag("tm-abci-socket", "tendermint: listen address of the ABCI socket server (tendermint --proxy-app dials it)").Default("tcp://0.0.0.0:26658").String()
	tmRPCAddr    = kingpin.Flag("tm-rpc-addr", "tendermint: RPC address of the local tendermint node (Submit = broadcast_tx_sync)").Default("tcp://127.0.0.1:26657").String()

	// --consensus=bft2 only.
	bft2API = kingpin.Flag("bft2-api", "bft2: local API (host:port) of the bft2-node HotStuff sidecar on this container").Default("127.0.0.1:7201").String()
)

// defaultFaultActiveFile is --fault-active-file's default; it must match
// HBDB_FAULT_ACTIVE_FILE in experiments/lib.sh (faults.sh fault_confirm).
const defaultFaultActiveFile = "/tmp/hbdb-fault-active"

// processStart anchors --fault-after: the fault turns on --fault-after after
// this moment (package initialisation, i.e. the process start).
var processStart = time.Now()

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	kingpin.Parse()
	if *consensusName == "" {
		runOriginal()
		return
	}
	runCore()
}

// requireFlag fails like kingpin does for a missing required flag.
func requireFlag(name, val string) {
	if val == "" {
		kingpin.Fatalf("required flag --%s not provided", name)
	}
}

// partySet returns the parties from --parties plus this node's signature.
func partySet() map[string]struct{} {
	pm := make(map[string]struct{})
	pm[*signature] = struct{}{}
	for _, s := range strings.Split(*parties, ",") {
		pm[s] = struct{}{}
	}
	return pm
}

// runOriginal is the unchanged original main (veritas/server.go).
func runOriginal() {
	if *faultSpec != core.FaultNone {
		kingpin.Fatalf("--fault=%s needs --consensus (the original server has no fault hooks)", *faultSpec)
	}
	requireFlag("kafka-addr", *kafkaAddr)
	requireFlag("kafka-group", *kafkaGroup)
	requireFlag("kafka-topic", *kafkaTopic)

	r, err := dbconn.NewRedisConn(*redisAddr, *redisPwd, *redisDb)
	check(err)

	c, err := kafkarole.NewConsumer(*kafkaAddr, *kafkaGroup, []string{*kafkaTopic})
	check(err)
	p, err := kafkarole.NewProducer(*kafkaAddr, *kafkaTopic)
	check(err)

	pm := partySet()

	s := grpc.NewServer()
	svr := veritas.NewServer(r, c, p, *ledgerPath, &veritas.Config{
		Signature: *signature,
		Topic:     *kafkaTopic,
		Parties:   pm,
		BlockSize: *blockSize,
	})
	pbv.RegisterNodeServer(s, svr)
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}

	go func() {
		log.Printf("Serving gRPC: %s", *addr)
		s.Serve(lis)
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)
	sig := <-ch
	log.Printf("Received signal %v, quiting gracefully", sig)
}

// runCore runs core.Executor + core.Server over the --consensus backend.
func runCore() {
	var cm core.CommitMode
	switch *commitMode {
	case "sync":
		cm = core.CommitSync
	case "async":
		cm = core.CommitAsync
	default:
		kingpin.Fatalf("--commit-mode must be sync or async, got %q", *commitMode)
	}
	var vm core.VersionMode
	switch *versionMode {
	case "client":
		vm = core.VersionClient
	case "ordered":
		vm = core.VersionOrdered
	default:
		kingpin.Fatalf("--version-mode must be client or ordered, got %q", *versionMode)
	}
	if *batchFlush <= 0 {
		kingpin.Fatalf("--batch-flush must be > 0, got %v", *batchFlush)
	}
	if *nodeID < 0 || *nodeID >= 1<<23 {
		kingpin.Fatalf("--node-id is required with --consensus and must be in [0, 2^23), got %d", *nodeID)
	}
	var peers []raftcons.Peer
	switch *consensusName {
	case "kafka":
		requireFlag("kafka-addr", *kafkaAddr)
		requireFlag("kafka-topic", *kafkaTopic)
		if *kafkaGroup != "" && *kafkaGroup != *signature {
			kingpin.Fatalf("--consensus=kafka: every node needs its own consumer group, which is its signature; drop --kafka-group or set it to %q", *signature)
		}
	case "raft":
		requireFlag("raft-peers", *raftPeers)
		requireFlag("raft-dir", *raftDir)
		var err error
		if peers, err = parseRaftPeers(*raftPeers); err != nil {
			kingpin.Fatalf("%v", err)
		}
	case "tendermint":
		requireFlag("tm-abci-socket", *tmABCISocket)
		requireFlag("tm-rpc-addr", *tmRPCAddr)
		// No TSO: versions come from the consensus order (the core's height
		// is the count of delivered blocks), so client versions would all be
		// stale.
		if vm != core.VersionOrdered {
			kingpin.Fatalf("--consensus=tendermint requires --version-mode=ordered, got %q", *versionMode)
		}
	case "bft2":
		requireFlag("bft2-api", *bft2API)
		// No TSO, as tendermint: versions come from the consensus order.
		if vm != core.VersionOrdered {
			kingpin.Fatalf("--consensus=bft2 requires --version-mode=ordered, got %q", *versionMode)
		}
	default:
		kingpin.Fatalf("unknown --consensus %q (want kafka, raft, tendermint or bft2)", *consensusName)
	}
	out := *breakdownOut
	if out == "" {
		out = fmt.Sprintf("/results/breakdown-node%d.json", *nodeID)
	}
	fp, err := buildFault(*faultSpec, *consensusName, processStart, *faultAfter)
	if err != nil {
		kingpin.Fatalf("%v", err)
	}
	// Install the SIGUSR1 handler before anything listens. Go ignores a
	// SIGUSR1 that has no handler, so a signal arriving before this point is
	// lost (not fatal); the harness detects that because the activation is
	// confirmed through --fault-active-file, which only the handler writes.
	if err := watchFaultSignal(fp, *faultSpec, *faultActive); err != nil {
		log.Fatalf("fault: %v", err)
	}
	if *faultSpec != core.FaultNone {
		log.Printf("fault %s armed: active %v after the process start (%s) or on SIGUSR1",
			*faultSpec, *faultAfter, processStart.UTC().Format(time.RFC3339))
	}

	r, err := dbconn.NewRedisConn(*redisAddr, *redisPwd, *redisDb)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	l, err := ledger.NewLedger(*ledgerPath, true)
	if err != nil {
		log.Fatalf("create ledger: %v", err)
	}
	ex := core.NewExecutor(core.NewRedisStore(r), l, vm, fp)

	// The backend is built before the server, but its drop callback must
	// reach the server: it goes through dropper, pointed at srv.AbortBlock
	// right after NewServer (drops reported before that are of other nodes'
	// blocks, since this node has submitted nothing yet, and are ignored).
	var drops dropper
	var cons consensus.Consensus
	switch *consensusName {
	case "kafka":
		cons, err = kafkacons.New(kafkacons.Config{
			Brokers:    *kafkaAddr,
			Topic:      *kafkaTopic,
			Group:      *signature,
			Signature:  *signature,
			Parties:    partySet(),
			Validate:   validator(ex),
			ShouldVote: fp.OnVote,
			OnDrop:     drops.drop,
		})
		if err != nil {
			log.Fatalf("kafka consensus: %v", err)
		}
	case "raft":
		// No drop callback: raft reports no lost blocks. A block accepted
		// but lost before commit (leader change) times out its sync
		// waiters (core.DefaultSyncTimeout).
		cons, err = raftcons.New(raftcons.Config{
			ID:        *signature,
			Bind:      *raftBind,
			DataDir:   *raftDir,
			Peers:     peers,
			Bootstrap: *raftBootstrap,
		})
		if err != nil {
			log.Fatalf("raft consensus: %v", err)
		}
	case "tendermint":
		// No drop callback: a block accepted by CheckTx but never committed
		// (e.g. evicted from the mempool) times out its sync waiters.
		cons, err = tmcons.New(tmcons.Config{
			ABCISocket: *tmABCISocket,
			RPCAddr:    *tmRPCAddr,
		})
		if err != nil {
			log.Fatalf("tendermint consensus: %v", err)
		}
	case "bft2":
		// No drop callback: a block accepted by the sidecar but never
		// committed (e.g. this replica crashed before proposing it) times
		// out its sync waiters.
		cons, err = bft2cons.New(bft2cons.Config{APIAddr: *bft2API})
		if err != nil {
			log.Fatalf("bft2 consensus: %v", err)
		}
	}

	srv := core.NewServer(core.ServerConfig{
		NodeID:        *nodeID,
		Signature:     *signature,
		BlockSize:     *blockSize,
		Mode:          vm,
		Commit:        cm,
		FlushInterval: *batchFlush, // > 0 (checked above)
	}, cons, ex, l)
	drops.set(srv.AbortBlock)
	// SIGUSR2: breakdown snapshots for the measurement window (the harness
	// signals at its start and its end; experiments/run_one.sh).
	watchSnapshotSignal(srv.WriteBreakdownJSON, out)

	gs := grpc.NewServer()
	pbv.RegisterNodeServer(gs, srv)
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	go func() {
		log.Printf("Serving gRPC: %s (consensus %s, commit %s, version %s, node %d)", *addr, *consensusName, *commitMode, *versionMode, *nodeID)
		if err := gs.Serve(lis); err != nil {
			log.Printf("gRPC serve: %v", err)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, os.Interrupt)
	sig := <-ch
	log.Printf("Received signal %v: closing server, writing breakdown to %s", sig, out)
	// Snapshot first, so a SIGKILL during a slow Close (stop grace period)
	// still leaves a breakdown file; it is overwritten after Close.
	if err := srv.WriteBreakdownJSON(out); err != nil {
		log.Printf("write breakdown snapshot %s: %v", out, err)
	}
	// Close fails pending RPCs with ErrStopped, flushes the batcher, closes
	// the backend and waits for the apply loop, so the breakdown written
	// after it is final.
	if err := srv.Close(); err != nil {
		log.Printf("server close: %v", err)
	}
	if err := srv.WriteBreakdownJSON(out); err != nil {
		log.Printf("write breakdown %s: %v", out, err)
	}
	gs.Stop()
	if err := l.Close(); err != nil {
		log.Printf("ledger close: %v", err)
	}
	if err := r.Close(); err != nil {
		log.Printf("redis close: %v", err)
	}
	log.Printf("bye")
}

// parseRaftPeers parses --raft-peers: comma-separated ID=RAFTADDR/FORWARDADDR
// entries (empty entries are skipped). Duplicates and the presence of this
// node are checked by raftcons.New.
func parseRaftPeers(s string) ([]raftcons.Peer, error) {
	var peers []raftcons.Peer
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		eq := strings.Index(item, "=")
		addrs := strings.Split(item[eq+1:], "/")
		if eq <= 0 || len(addrs) != 2 || addrs[0] == "" || addrs[1] == "" {
			return nil, fmt.Errorf("--raft-peers entry %q: want ID=RAFTADDR/FORWARDADDR", item)
		}
		peers = append(peers, raftcons.Peer{ID: item[:eq], RaftAddr: addrs[0], ForwardAddr: addrs[1]})
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("--raft-peers %q: no peers", s)
	}
	return peers, nil
}

// dropper forwards a backend's drop callback to a function set later
// (srv.AbortBlock). Until set, drops are ignored. Safe for concurrent use.
type dropper struct {
	mu sync.RWMutex
	fn func(raw []byte)
}

func (d *dropper) set(fn func(raw []byte)) {
	d.mu.Lock()
	d.fn = fn
	d.mu.Unlock()
}

func (d *dropper) drop(raw []byte) {
	d.mu.RLock()
	fn := d.fn
	d.mu.RUnlock()
	if fn != nil {
		fn(raw)
	}
}

// validator is the Kafka backend's vote: decode the proposal and check it
// against this replica's current store (Executor.ValidateErr). A decode or
// store error is logged and votes Abort.
func validator(ex *core.Executor) func([]byte) bool {
	return func(raw []byte) bool {
		var blk pbv.Block
		if err := proto.Unmarshal(raw, &blk); err != nil {
			log.Printf("validate: undecodable proposal, voting Abort: %v", err)
			return false
		}
		ok, err := ex.ValidateErr(context.Background(), &blk)
		if err != nil {
			log.Printf("validate: %v, voting Abort", err)
			return false
		}
		return ok
	}
}

// buildFault parses --fault/--fault-after for backend (--consensus). silent
// exists only for kafka, the one backend with a vote hook
// (kafka.Config.ShouldVote); F2 for the other backends is injected at the
// network level (experiments/faults.sh).
func buildFault(spec, backend string, start time.Time, after time.Duration) (core.FaultPolicy, error) {
	fp, err := core.ParseFault(spec, start, after)
	if err != nil {
		return nil, fmt.Errorf("--fault=%s --fault-after=%v: %v", spec, after, err)
	}
	if spec == core.FaultSilent && backend != "kafka" {
		return nil, fmt.Errorf("--fault=silent needs --consensus=kafka (the only backend with a vote hook), got --consensus=%s; "+
			"its silent fault (F2) is injected at the network level (experiments/faults.sh)", backend)
	}
	return fp, nil
}

// watchFaultSignal makes SIGUSR1 activate fp at once (core.Activator) and
// then write spec to activeFile (unless activeFile is ""), so that a reader
// knows the fault really is on; with --fault=none the signal is logged and
// ignored and nothing is written. A stale activeFile is removed first. It
// returns after the handler is installed.
func watchFaultSignal(fp core.FaultPolicy, spec, activeFile string) error {
	if activeFile != "" {
		if err := os.Remove(activeFile); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale %s: %v", activeFile, err)
		}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		for range ch {
			a, ok := fp.(core.Activator)
			if !ok {
				log.Printf("SIGUSR1 ignored: --fault=%s", spec)
				continue
			}
			a.Activate()
			log.Printf("SIGUSR1: fault %s activated", spec)
			if activeFile != "" {
				if err := writeFileAtomic(activeFile, spec+"\n"); err != nil {
					log.Printf("fault: write %s: %v", activeFile, err)
				}
			}
		}
	}()
	return nil
}

// snapshotPath is the file of the k-th SIGUSR2 breakdown snapshot (k >= 1)
// for the breakdown file out: out without its ".json" suffix, then
// ".snap<k>.json" (/results/breakdown-node1.json -> .../breakdown-node1.snap1.json).
func snapshotPath(out string, k int) string {
	return fmt.Sprintf("%s.snap%d.json", strings.TrimSuffix(out, ".json"), k)
}

// watchSnapshotSignal makes the k-th SIGUSR2 (k = 1, 2, ...) write the
// node's cumulative breakdown to snapshotPath(out, k) through write
// (core.Server.WriteBreakdownJSON), so the harness gets the blocks applied
// inside the measurement window as the difference of two snapshots (the
// breakdown at SIGTERM also covers the load phase). A failed write is only
// logged. It returns after the handler is installed.
func watchSnapshotSignal(write func(path string) error, out string) {
	ch := make(chan os.Signal, 4)
	signal.Notify(ch, syscall.SIGUSR2)
	go func() {
		k := 0
		for range ch {
			k++
			p := snapshotPath(out, k)
			if err := write(p); err != nil {
				log.Printf("SIGUSR2: write breakdown snapshot %s: %v", p, err)
				continue
			}
			log.Printf("SIGUSR2: breakdown snapshot %d written to %s", k, p)
		}
	}()
}

// writeFileAtomic writes data to path through a temporary file and a rename,
// so a reader sees either no file or the whole content.
func writeFileAtomic(path, data string) error {
	tmp := path + ".tmp"
	if err := ioutil.WriteFile(tmp, []byte(data), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
