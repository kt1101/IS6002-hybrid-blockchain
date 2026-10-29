// Command veritas runs one Veritas node.
//
// Without --consensus it runs the ORIGINAL Kafka approve-all server
// (veritas/server.go) exactly as before, with the same flags and behaviour,
// so the kafka-orig baseline keeps using this binary (veritas-kafka).
//
// With --consensus=<backend> it runs the shared core: core.Executor +
// core.Server over the chosen consensus backend. Only "kafka"
// (consensus/kafka) exists so far; raft, tendermint and bft2 exit with a
// "not implemented yet" error (Tasks 10-12). On SIGTERM/SIGINT the core path
// closes the server, writes its time breakdown to --breakdown-out and exits.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"

	"hybrid/consensus"
	kafkacons "hybrid/consensus/kafka"
	"hybrid/dbconn"
	"hybrid/kafkarole"
	pbv "hybrid/proto/veritas"
	"hybrid/veritas"
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
	nodeID        = kingpin.Flag("node-id", "core: node id in [0, 2^23), unique per deployment (high bits of every SharedLog seq)").Default("-1").Int()
	breakdownOut  = kingpin.Flag("breakdown-out", "core: breakdown JSON written on SIGTERM/SIGINT (default /results/breakdown-node<node-id>.json)").Default("").String()
)

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
	if *nodeID < 0 || *nodeID >= 1<<23 {
		kingpin.Fatalf("--node-id is required with --consensus and must be in [0, 2^23), got %d", *nodeID)
	}
	switch *consensusName {
	case "kafka":
		requireFlag("kafka-addr", *kafkaAddr)
		requireFlag("kafka-topic", *kafkaTopic)
		if *kafkaGroup != "" && *kafkaGroup != *signature {
			kingpin.Fatalf("--consensus=kafka: every node needs its own consumer group, which is its signature; drop --kafka-group or set it to %q", *signature)
		}
	case "raft", "tendermint", "bft2":
		fmt.Fprintf(os.Stderr, "veritas: --consensus=%s: not implemented yet\n", *consensusName)
		os.Exit(2)
	default:
		kingpin.Fatalf("unknown --consensus %q (want kafka, raft, tendermint or bft2)", *consensusName)
	}
	out := *breakdownOut
	if out == "" {
		out = fmt.Sprintf("/results/breakdown-node%d.json", *nodeID)
	}

	r, err := dbconn.NewRedisConn(*redisAddr, *redisPwd, *redisDb)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	l, err := ledger.NewLedger(*ledgerPath, true)
	if err != nil {
		log.Fatalf("create ledger: %v", err)
	}
	ex := core.NewExecutor(core.NewRedisStore(r), l, vm, nil)

	// The backend is built before the server, but its drop callback must
	// reach the server: it goes through dropper, pointed at srv.AbortBlock
	// right after NewServer (drops reported before that are of other nodes'
	// blocks, since this node has submitted nothing yet, and are ignored).
	var drops dropper
	var cons consensus.Consensus
	switch *consensusName {
	case "kafka":
		cons, err = kafkacons.New(kafkacons.Config{
			Brokers:   *kafkaAddr,
			Topic:     *kafkaTopic,
			Group:     *signature,
			Signature: *signature,
			Parties:   partySet(),
			Validate:  validator(ex),
			OnDrop:    drops.drop,
		})
		if err != nil {
			log.Fatalf("kafka consensus: %v", err)
		}
	}

	srv := core.NewServer(core.ServerConfig{
		NodeID:    *nodeID,
		Signature: *signature,
		BlockSize: *blockSize,
		Mode:      vm,
		Commit:    cm,
	}, cons, ex, l)
	drops.set(srv.AbortBlock)

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
