package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"hybrid/consensus/memnet"
	pbv "hybrid/proto/veritas"
	"hybrid/veritas"
	"hybrid/veritas/core"
	"hybrid/veritas/ledger"
	"hybrid/veritas/observe"
)

// dockerTestNode is one real core.Server behind a gRPC server and an
// observer, standing in for a node container.
type dockerTestNode struct {
	grpcAddr string
	obs      *httptest.Server
}

// startDockerTestNodes starts n core.Servers on one memnet hub, each served
// over gRPC on 127.0.0.1:0 and observed through observe.Handler.
func startDockerTestNodes(t *testing.T, n int) []*dockerTestNode {
	t.Helper()
	hub := memnet.NewHub(n)
	nodes := make([]*dockerTestNode, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("node%d", i+1)
		l, err := ledger.NewLedger(t.TempDir(), true)
		if err != nil {
			t.Fatalf("ledger: %v", err)
		}
		store := core.NewMapStore()
		ex := core.NewExecutor(store, l, core.VersionOrdered, nil)
		b := observe.NewBroadcaster(name)
		srv := core.NewServer(core.ServerConfig{
			NodeID:        i + 1,
			Signature:     name,
			BlockSize:     100,
			Mode:          core.VersionOrdered,
			Commit:        core.CommitSync,
			FlushInterval: 50 * time.Millisecond,
			OnApplied:     b.Publish,
		}, hub.Node(i), ex, l)
		kv := func(ctx context.Context, key string) (string, int64, bool, error) {
			raw, ok, err := store.Get(ctx, key)
			if err != nil || !ok {
				return "", 0, false, err
			}
			v, err := veritas.Decode(raw)
			if err != nil {
				return "", 0, false, err
			}
			return v.Val, v.Version, true, nil
		}
		obs := httptest.NewServer(observe.Handler(b, kv, ex.Prove))

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		gs := grpc.NewServer()
		pbv.RegisterNodeServer(gs, srv)
		go gs.Serve(lis)

		nodes[i] = &dockerTestNode{grpcAddr: lis.Addr().String(), obs: obs}
		t.Cleanup(func() {
			gs.Stop()
			obs.CloseClientConnections()
			obs.Close()
			srv.Close()
			l.Close()
		})
	}
	return nodes
}

// dockerTestConfig points a DockerConfig at the test nodes.
func dockerTestConfig(nodes []*dockerTestNode) DockerConfig {
	return DockerConfig{
		Backend:   "bft2",
		N:         len(nodes),
		GRPCAddr:  func(i int) string { return nodes[i-1].grpcAddr },
		EventsURL: func(i int) string { return nodes[i-1].obs.URL },
	}
}

func newTestDocker(t *testing.T, cfg DockerConfig) *Docker {
	t.Helper()
	d, err := NewDocker(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewDocker: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestDockerSetGetProve(t *testing.T) {
	nodes := startDockerTestNodes(t, 3)
	d := newTestDocker(t, dockerTestConfig(nodes))

	info := d.Info()
	if info.Mode != "docker" || info.Backend != "bft2" || info.N != 3 || info.F != 0 || info.CanReset {
		t.Fatalf("Info = %+v", info)
	}

	mustSet(t, d, "node2", "user42", "hello-committee")
	mustSet(t, d, "node2", "other", "x") // a second leaf, so the proof has side nodes
	eventually(t, testTimeout, "every node to apply the Set", func() bool {
		for _, n := range []string{"node1", "node2", "node3"} {
			if v, ok := getVal(d, n, "user42"); !ok || v != "hello-committee" {
				return false
			}
		}
		return true
	})

	if _, ok, err := d.Get(ctxT(t, testTimeout), "node1", "missing"); err != nil || ok {
		t.Fatalf("Get(missing) = ok %v, err %v; want not found", ok, err)
	}

	p1, err := d.Prove(ctxT(t, testTimeout), "node1", "user42")
	if err != nil {
		t.Fatalf("Prove node1: %v", err)
	}
	p3, err := d.Prove(ctxT(t, testTimeout), "node3", "user42")
	if err != nil {
		t.Fatalf("Prove node3: %v", err)
	}
	if len(p1.Root) == 0 || len(p1.SideNodes) == 0 {
		t.Fatalf("empty proof: %+v", p1)
	}
	if !bytes.Equal(p1.Root, p3.Root) {
		t.Fatalf("roots differ: %x vs %x", p1.Root, p3.Root)
	}

	if _, _, err := d.Get(ctxT(t, testTimeout), "node9", "user42"); err == nil {
		t.Fatal("Get on an unknown node succeeded")
	}
	if err := d.Reset(context.Background()); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Reset = %v, want ErrUnsupported", err)
	}
}

func TestDockerEventsMerged(t *testing.T) {
	nodes := startDockerTestNodes(t, 3)
	d := newTestDocker(t, dockerTestConfig(nodes))
	eventually(t, testTimeout, "all nodes up", func() bool {
		for _, n := range d.Nodes() {
			if !n.Up {
				return false
			}
		}
		return true
	})

	mustSet(t, d, "node1", "k1", "v1")

	seen := map[string]BlockEvent{}
	deadline := time.After(testTimeout)
	for len(seen) < 3 {
		select {
		case ev := <-d.Events():
			if ev.Txs > 0 {
				seen[ev.Node] = ev
			}
		case <-deadline:
			t.Fatalf("events from %d nodes only: %v", len(seen), seen)
		}
	}
	var root []byte
	for name, ev := range seen {
		if ev.Height < 1 || len(ev.Root) == 0 || ev.At.IsZero() || ev.Proposer == "" {
			t.Errorf("%s: incomplete event %+v", name, ev)
		}
		if root == nil {
			root = ev.Root
		} else if !bytes.Equal(root, ev.Root) {
			t.Errorf("%s: root %x differs from %x", name, ev.Root, root)
		}
	}
}

func TestDockerNodeDownWhenObserverStops(t *testing.T) {
	nodes := startDockerTestNodes(t, 3)
	d := newTestDocker(t, dockerTestConfig(nodes))
	up := func(name string) bool {
		for _, n := range d.Nodes() {
			if n.Name == name {
				return n.Up
			}
		}
		return false
	}
	eventually(t, testTimeout, "node3 up", func() bool { return up("node3") })

	nodes[2].obs.CloseClientConnections()
	nodes[2].obs.Close()

	eventually(t, 4*time.Second, "node3 down", func() bool { return !up("node3") })
	if !up("node1") || !up("node2") {
		t.Fatalf("other nodes went down: %+v", d.Nodes())
	}
}

// writeFaultScript writes a bash script that records "<action> <node>
// <HBDB_BACKEND> <armed corrupt>/<armed silent>" to log, then prints stderr and exits with rc.
func writeFaultScript(t *testing.T, rc int, stderr string) (script, log string) {
	t.Helper()
	dir := t.TempDir()
	script = filepath.Join(dir, "fault.sh")
	log = filepath.Join(dir, "calls")
	body := fmt.Sprintf("#!/usr/bin/env bash\necho \"$1 $2 $HBDB_BACKEND $DEMO_ARMED_CORRUPT/$DEMO_ARMED_SILENT\" >>%q\necho 'noise' >&2\n", log)
	if stderr != "" {
		body += fmt.Sprintf("echo %q >&2\n", stderr)
	}
	body += fmt.Sprintf("exit %d\n", rc)
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return script, log
}

func readCalls(t *testing.T, log string) []string {
	t.Helper()
	b, err := os.ReadFile(log)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func nodeFaults(d *Docker, name string) []Fault {
	for _, n := range d.Nodes() {
		if n.Name == name {
			return n.Faults
		}
	}
	return nil
}

func TestDockerInjectRunsScript(t *testing.T) {
	nodes := startDockerTestNodes(t, 3)
	script, log := writeFaultScript(t, 0, "")
	cfg := dockerTestConfig(nodes)
	cfg.FaultScript = script
	d := newTestDocker(t, cfg)

	ctx := ctxT(t, testTimeout)
	if err := d.Inject(ctx, "node2", FaultCrash); err != nil {
		t.Fatalf("Inject crash: %v", err)
	}
	if err := d.Inject(ctx, "node1", FaultSlow); err != nil {
		t.Fatalf("Inject slow: %v", err)
	}
	if err := d.Inject(ctx, "node3", FaultCorrupt); err != nil {
		t.Fatalf("Inject corrupt: %v", err)
	}
	got := readCalls(t, log)
	want := []string{"crash node2 bft2 node3/", "slow node1 bft2 node3/", "corrupt node3 bft2 node3/"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("script calls = %q, want %q", got, want)
	}

	if f := nodeFaults(d, "node2"); len(f) != 1 || f[0] != FaultCrash {
		t.Fatalf("node2 faults = %v, want [crash]", f)
	}
	// crash and corrupt cannot be healed: Heal fails and does not run the script.
	if err := d.Heal(ctx, "node2"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Heal crashed node = %v, want ErrUnsupported", err)
	}
	if err := d.Heal(ctx, "node3"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Heal corrupt node = %v, want ErrUnsupported", err)
	}
	if len(readCalls(t, log)) != 3 {
		t.Fatalf("Heal of unhealable nodes ran the script: %q", readCalls(t, log))
	}
	// slow can: the script runs and the fault is dropped.
	if err := d.Heal(ctx, "node1"); err != nil {
		t.Fatalf("Heal slow: %v", err)
	}
	if got := readCalls(t, log); got[len(got)-1] != "heal node1 bft2 node3/" {
		t.Fatalf("last call = %q, want heal node1", got[len(got)-1])
	}
	if f := nodeFaults(d, "node1"); len(f) != 0 {
		t.Fatalf("node1 faults after heal = %v", f)
	}
	if err := d.Inject(ctx, "node9", FaultCrash); err == nil {
		t.Fatal("Inject on an unknown node succeeded")
	}
	if err := d.Inject(ctx, "node1", Fault("bogus")); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Inject bogus fault = %v, want ErrUnsupported", err)
	}
}

func TestDockerInjectScriptFailure(t *testing.T) {
	nodes := startDockerTestNodes(t, 3)
	script, _ := writeFaultScript(t, 4, "corrupt: node2 is not armed")
	cfg := dockerTestConfig(nodes)
	cfg.FaultScript = script
	d := newTestDocker(t, cfg)

	err := d.Inject(ctxT(t, testTimeout), "node2", FaultCorrupt)
	if err == nil {
		t.Fatal("Inject succeeded though the script exited 4")
	}
	if !strings.Contains(err.Error(), "corrupt: node2 is not armed") {
		t.Fatalf("error %q does not carry the script's last stderr line", err)
	}
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("exit 4 = %v, want ErrUnsupported", err)
	}
	if f := nodeFaults(d, "node2"); len(f) != 0 {
		t.Fatalf("failed Inject left faults %v", f)
	}

	script, _ = writeFaultScript(t, 1, "docker: boom")
	cfg.FaultScript = script
	d2 := newTestDocker(t, cfg)
	err = d2.Inject(ctxT(t, testTimeout), "node2", FaultCrash)
	if err == nil || !strings.Contains(err.Error(), "docker: boom") || errors.Is(err, ErrUnsupported) {
		t.Fatalf("exit 1 error = %v", err)
	}
}

func TestDockerFaultCatalog(t *testing.T) {
	nodes := startDockerTestNodes(t, 3)
	cfg := dockerTestConfig(nodes)

	cfg.Backend = "bft2"
	d := newTestDocker(t, cfg)
	info := d.Info()
	if fmt.Sprint(info.Faults) != "[crash slow silent corrupt]" || fmt.Sprint(info.Healable) != "[slow silent]" {
		t.Fatalf("bft2 faults %v healable %v", info.Faults, info.Healable)
	}

	cfg.Backend = "kafka"
	k := newTestDocker(t, cfg)
	info = k.Info()
	if fmt.Sprint(info.Faults) != "[crash slow silent corrupt]" || fmt.Sprint(info.Healable) != "[slow]" {
		t.Fatalf("kafka faults %v healable %v", info.Faults, info.Healable)
	}

	cfg.ArmedSilent = []string{}
	cfg.ArmedCorrupt = []string{}
	none := newTestDocker(t, cfg)
	if fmt.Sprint(none.Info().Faults) != "[crash slow]" {
		t.Fatalf("unarmed kafka faults %v", none.Info().Faults)
	}
}

func TestDockerConnectTimeout(t *testing.T) {
	// A port nothing listens on.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	start := time.Now()
	_, err = NewDocker(context.Background(), DockerConfig{
		Backend:        "bft2",
		N:              2,
		GRPCAddr:       func(int) string { return addr },
		EventsURL:      func(int) string { return "http://" + addr },
		ConnectTimeout: 300 * time.Millisecond,
	})
	if !errors.Is(err, ErrNoNodes) {
		t.Fatalf("NewDocker = %v, want ErrNoNodes", err)
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Fatalf("took %v", el)
	}
}

func TestDockerDefaults(t *testing.T) {
	var c DockerConfig
	c.applyDefaults()
	if got := c.GRPCAddr(2); got != "127.0.0.1:19902" {
		t.Errorf("GRPCAddr(2) = %q", got)
	}
	if got := c.EventsURL(1); got != "http://127.0.0.1:19801" {
		t.Errorf("EventsURL(1) = %q", got)
	}
	if c.ConnectTimeout != 10*time.Second || c.FaultScript != "experiments/demo_fault.sh" ||
		fmt.Sprint(c.ArmedCorrupt) != "[node3]" || c.ArmedSilent != nil {
		t.Errorf("defaults = %+v", c)
	}
	k := DockerConfig{Backend: "kafka"}
	k.applyDefaults()
	if fmt.Sprint(k.ArmedSilent) != "[node4]" {
		t.Errorf("kafka ArmedSilent = %v", k.ArmedSilent)
	}
}

func TestDockerConnectDoesNotWaitForDeadNodes(t *testing.T) {
	nodes := startDockerTestNodes(t, 1)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := lis.Addr().String()
	lis.Close()

	const connectTimeout = 5 * time.Second
	start := time.Now()
	d, err := NewDocker(context.Background(), DockerConfig{
		Backend: "bft2",
		N:       2,
		GRPCAddr: func(i int) string {
			if i == 1 {
				return nodes[0].grpcAddr
			}
			return dead
		},
		EventsURL: func(i int) string {
			if i == 1 {
				return nodes[0].obs.URL
			}
			return "http://" + dead
		},
		ConnectTimeout: connectTimeout,
	})
	if err != nil {
		t.Fatalf("NewDocker with one dead node: %v", err)
	}
	defer d.Close()
	if el := time.Since(start); el > connectTimeout/2 {
		t.Fatalf("NewDocker took %v with one healthy node, want well under %v", el, connectTimeout)
	}
}
