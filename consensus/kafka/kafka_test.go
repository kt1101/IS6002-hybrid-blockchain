package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/protobuf/proto"

	pbv "hybrid/proto/veritas"
)

// ---- fake single-partition broker ----

// fakeBroker is one in-memory single-partition topic. Every consumer reads
// the whole log from offset 0 (one group per node), each read returning a
// fresh copy of the bytes, as a real consumer does.
type fakeBroker struct {
	mu      sync.Mutex
	log     [][]byte
	changed chan struct{} // closed and replaced on every append
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{changed: make(chan struct{})}
}

func (b *fakeBroker) append(v []byte) {
	b.mu.Lock()
	b.log = append(b.log, append([]byte(nil), v...))
	close(b.changed)
	b.changed = make(chan struct{})
	b.mu.Unlock()
}

// messages returns the decoded log.
func (b *fakeBroker) messages(t *testing.T) []*pbv.Block {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*pbv.Block, len(b.log))
	for i, raw := range b.log {
		var m pbv.Block
		if err := proto.Unmarshal(raw, &m); err != nil {
			t.Fatalf("log[%d]: %v", i, err)
		}
		out[i] = &m
	}
	return out
}

type fakeProducer struct {
	b      *fakeBroker
	events chan ck.Event
	mu     sync.Mutex
	closed bool
}

func (p *fakeProducer) Produce(msg *ck.Message, _ chan ck.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("fake producer: Produce after Close")
	}
	p.b.append(msg.Value)
	return nil
}

func (p *fakeProducer) Events() chan ck.Event { return p.events }
func (p *fakeProducer) Flush(int) int         { return 0 }
func (p *fakeProducer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.events)
	}
}

type fakeConsumer struct {
	b    *fakeBroker
	off  int        // only touched by the backend's consumer goroutine
	errs chan error // injected errors, returned before any message
}

func (c *fakeConsumer) ReadMessage(timeout time.Duration) (*ck.Message, error) {
	select {
	case err := <-c.errs:
		return nil, err
	default:
	}
	for attempt := 0; attempt < 2; attempt++ {
		c.b.mu.Lock()
		if c.off < len(c.b.log) {
			v := append([]byte(nil), c.b.log[c.off]...)
			c.off++
			c.b.mu.Unlock()
			return &ck.Message{Value: v}, nil
		}
		ch := c.b.changed
		c.b.mu.Unlock()
		select {
		case <-ch:
		case <-time.After(timeout):
			return nil, ck.NewError(ck.ErrTimedOut, "fake: timed out", false)
		}
	}
	return nil, ck.NewError(ck.ErrTimedOut, "fake: timed out", false)
}

func (c *fakeConsumer) Close() error { return nil }

// cluster starts one backend per name over a shared fake broker. cfgs may
// adjust each node's Config.
func cluster(t *testing.T, b *fakeBroker, names []string, adjust func(*Config)) ([]*Kafka, []*fakeConsumer) {
	t.Helper()
	nodes := make([]*Kafka, len(names))
	cons := make([]*fakeConsumer, len(names))
	for i, n := range names {
		cfg := Config{Brokers: "fake:9092", Topic: "shared-log", Signature: n, Parties: parties(names...)}
		if adjust != nil {
			adjust(&cfg)
		}
		cons[i] = &fakeConsumer{b: b, errs: make(chan error, 4)}
		nodes[i] = newKafka(cfg, &fakeProducer{b: b, events: make(chan ck.Event, 16)}, cons[i])
	}
	t.Cleanup(func() {
		for _, k := range nodes {
			k.Close()
		}
	})
	return nodes, cons
}

// recv reads one block from ch or fails after testTimeout.
func recv(t *testing.T, ch <-chan []byte) ([]byte, bool) {
	t.Helper()
	select {
	case b, ok := <-ch:
		return b, ok
	case <-time.After(testTimeout / 2):
		t.Fatal("timed out waiting for Deliver")
		return nil, false
	}
}

// quiet reports whether nothing arrives on ch within d.
func quiet(ch <-chan []byte, d time.Duration) bool {
	select {
	case <-ch:
		return false
	case <-time.After(d):
		return true
	}
}

func seqOf(t *testing.T, raw []byte) int64 {
	t.Helper()
	var m pbv.Block
	if err := proto.Unmarshal(raw, &m); err != nil {
		t.Fatalf("delivered block does not decode: %v", err)
	}
	return m.GetTxs()[0].GetSeq()
}

// rawProposal is proposal(...).raw for goroutines other than the test's
// (it must not call t.Fatal); marshalling a valid block cannot fail.
func rawProposal(signer string, seq int64) []byte {
	raw, err := proto.Marshal(&pbv.Block{
		Type:      pbv.MessageType_Approve,
		Signature: signer,
		Txs: []*pbv.SharedLog{{
			Seq:  seq,
			Sets: []*pbv.SetRequest{{Key: fmt.Sprintf("k%d", seq), Value: "v", Version: seq}},
		}},
	})
	if err != nil {
		panic(err)
	}
	return raw
}

func submit(t *testing.T, k *Kafka, raw []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout/2)
	defer cancel()
	if err := k.Submit(ctx, raw); err != nil {
		t.Fatalf("Submit: %v", err)
	}
}

// ---- tests ----

func TestKafkaNodesDeliverSameBlocksInSameOrder(t *testing.T) {
	guard(t)
	b := newFakeBroker()
	nodes, _ := cluster(t, b, three, nil)

	const perNode = 20
	var wg sync.WaitGroup
	for i, k := range nodes {
		wg.Add(1)
		go func(i int, k *Kafka) {
			defer wg.Done()
			for j := 1; j <= perNode; j++ {
				raw := rawProposal(three[i], int64(i+1)<<40|int64(j))
				if err := k.Submit(context.Background(), raw); err != nil {
					t.Errorf("node %d Submit: %v", i, err)
					return
				}
			}
		}(i, k)
	}
	wg.Wait()

	total := len(nodes) * perNode
	var first []int64
	for i, k := range nodes {
		got := make([]int64, 0, total)
		seen := make(map[int64]bool)
		for len(got) < total {
			raw, ok := recv(t, k.Deliver())
			if !ok {
				t.Fatalf("node %d: Deliver closed early", i)
			}
			s := seqOf(t, raw)
			if seen[s] {
				t.Fatalf("node %d: block %#x delivered twice", i, s)
			}
			seen[s] = true
			got = append(got, s)
		}
		if !quiet(k.Deliver(), 50*time.Millisecond) {
			t.Fatalf("node %d: more than %d blocks delivered", i, total)
		}
		if i == 0 {
			first = got
			continue
		}
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("node %d: position %d is %#x, node 0 has %#x", i, j, got[j], first[j])
			}
		}
	}
	// Each node voted on every other node's proposal: proposals + 2 votes each.
	if n := len(b.messages(t)); n != total*3 {
		t.Fatalf("topic has %d messages, want %d", n, total*3)
	}
}

func TestKafkaAbortVoteDropsBlockOnEveryNode(t *testing.T) {
	guard(t)
	const bad, good = 1, 2
	b := newFakeBroker()
	var dropMu sync.Mutex
	drops := make(map[string][]int64) // node -> seqs reported through OnDrop
	nodes, _ := cluster(t, b, three, func(c *Config) {
		sig := c.Signature
		c.OnDrop = func(raw []byte) {
			var m pbv.Block
			if err := proto.Unmarshal(raw, &m); err != nil {
				t.Errorf("%s: OnDrop got undecodable bytes: %v", sig, err)
				return
			}
			dropMu.Lock()
			drops[sig] = append(drops[sig], m.GetTxs()[0].GetSeq())
			dropMu.Unlock()
		}
		if sig != "n2" {
			return
		}
		c.Validate = func(raw []byte) bool {
			var m pbv.Block
			if err := proto.Unmarshal(raw, &m); err != nil {
				return false
			}
			return m.GetTxs()[0].GetSeq() != bad
		}
	})
	submit(t, nodes[0], proposal(t, "n1", bad).raw)
	submit(t, nodes[0], proposal(t, "n1", good).raw)

	// n2 votes in topic order: its Abort for `bad` is on the topic before its
	// Approve for `good`, so `good` can only complete after `bad` was dropped.
	for i, k := range nodes {
		raw, ok := recv(t, k.Deliver())
		if !ok {
			t.Fatalf("node %d: Deliver closed", i)
		}
		if s := seqOf(t, raw); s != good {
			t.Fatalf("node %d: delivered %d, want only %d", i, s, good)
		}
		if !quiet(k.Deliver(), 50*time.Millisecond) {
			t.Fatalf("node %d: aborted block delivered", i)
		}
	}
	aborts := 0
	for _, m := range b.messages(t) {
		if m.GetType() == pbv.MessageType_Abort {
			aborts++
			if m.GetSignature() != "n2" || m.GetTxs()[0].GetSeq() != bad || len(m.GetTxs()[0].GetSets()) != 0 {
				t.Fatalf("unexpected abort vote %v", m)
			}
		}
	}
	if aborts != 1 {
		t.Fatalf("%d abort votes on the topic, want 1", aborts)
	}
	// Each node's consumer reported the drop exactly once, before it
	// delivered `good` (same goroutine, topic order).
	dropMu.Lock()
	defer dropMu.Unlock()
	for _, n := range three {
		if got := drops[n]; len(got) != 1 || got[0] != bad {
			t.Fatalf("%s: OnDrop reported %v, want [%d]", n, got, bad)
		}
	}
}

func TestKafkaSilentPartyBlocksDelivery(t *testing.T) {
	guard(t)
	b := newFakeBroker()
	nodes, _ := cluster(t, b, three, func(c *Config) {
		if c.Signature == "n3" {
			c.ShouldVote = func() bool { return false }
		}
	})
	submit(t, nodes[0], proposal(t, "n1", 5).raw)
	for i, k := range nodes {
		if !quiet(k.Deliver(), 200*time.Millisecond) {
			t.Fatalf("node %d delivered a block that n3 never approved", i)
		}
	}
	for _, m := range b.messages(t) {
		if m.GetSignature() == "n3" {
			t.Fatalf("silent party produced %v", m)
		}
	}
}

// A silent party (ShouldVote false, F2 on kafka) withholds only its votes:
// its OWN proposals still count as its approve and are completed by the
// others' votes, so they are delivered everywhere, while the other parties'
// blocks never complete. This is why F2 on kafka leaves about 1/N of the
// throughput (the silent node's clients), unlike the network-silent F2.
func TestKafkaSilentPartyOwnProposalsStillDelivered(t *testing.T) {
	guard(t)
	b := newFakeBroker()
	nodes, _ := cluster(t, b, three, func(c *Config) {
		if c.Signature == "n3" {
			c.ShouldVote = func() bool { return false }
		}
	})
	const othersSeq, ownSeq = 1<<40 | 1, 3<<40 | 1
	submit(t, nodes[0], rawProposal("n1", othersSeq)) // needs n3's vote: never completes
	submit(t, nodes[2], rawProposal("n3", ownSeq))    // n3's own block
	for i, k := range nodes {
		raw, ok := recv(t, k.Deliver())
		if !ok {
			t.Fatalf("node %d: Deliver closed", i)
		}
		if s := seqOf(t, raw); s != ownSeq {
			t.Fatalf("node %d delivered %#x, want the silent node's own block %#x", i, s, int64(ownSeq))
		}
		if !quiet(k.Deliver(), 200*time.Millisecond) {
			t.Fatalf("node %d delivered n1's block, which n3 never approved", i)
		}
	}
	// n3 put exactly one message on the topic: its proposal, no vote.
	var fromN3 int
	for _, m := range b.messages(t) {
		if m.GetSignature() == "n3" {
			fromN3++
			if len(m.GetTxs()) == 0 || len(m.GetTxs()[0].GetSets()) == 0 {
				t.Fatalf("silent party produced a vote: %v", m)
			}
		}
	}
	if fromN3 != 1 {
		t.Fatalf("n3 produced %d messages, want 1 (its proposal)", fromN3)
	}
}

func TestKafkaSubmitCopiesAndDeliverReturnsFreshSlices(t *testing.T) {
	guard(t)
	b := newFakeBroker()
	nodes, _ := cluster(t, b, []string{"n1", "n2"}, nil)
	want := proposal(t, "n1", 77).raw
	buf := append([]byte(nil), want...)
	submit(t, nodes[0], buf)
	for i := range buf { // the caller reuses its buffer right away
		buf[i] = 0xff
	}
	got0, _ := recv(t, nodes[0].Deliver())
	got1, _ := recv(t, nodes[1].Deliver())
	if !bytes.Equal(got0, want) || !bytes.Equal(got1, want) {
		t.Fatal("delivered bytes differ from the submitted block")
	}
	got0[0] ^= 0xff // the receiver owns its slice
	if !bytes.Equal(got1, want) {
		t.Fatal("nodes share a delivered slice")
	}
}

func TestKafkaCloseIsIdempotentAndSubmitAfterCloseFails(t *testing.T) {
	guard(t)
	b := newFakeBroker()
	nodes, _ := cluster(t, b, []string{"n1"}, nil)
	k := nodes[0]

	// Submits racing with Close must either succeed or return ErrClosed.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := int64(1); ; j++ {
				raw := rawProposal("n1", int64(g+1)<<40|j)
				err := k.Submit(context.Background(), raw)
				if errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					t.Errorf("Submit: %v", err)
					return
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}(g)
	}
	time.Sleep(20 * time.Millisecond)
	if err := k.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(stop)
	wg.Wait()
	if err := k.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := k.Submit(context.Background(), proposal(t, "n1", 1).raw); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
	// Deliver is closed (buffered blocks may still be read first).
	for {
		if _, ok := recv(t, k.Deliver()); !ok {
			break
		}
	}
}

func TestKafkaConsumerErrors(t *testing.T) {
	guard(t)
	b := newFakeBroker()
	nodes, cons := cluster(t, b, []string{"n1"}, nil)
	k := nodes[0]

	// A non-fatal error is logged and the loop keeps reading.
	cons[0].errs <- ck.NewError(ck.ErrTransport, "fake: broker down", false)
	cons[0].errs <- fmt.Errorf("fake: not a kafka.Error")
	submit(t, k, proposal(t, "n1", 1).raw)
	if raw, ok := recv(t, k.Deliver()); !ok || seqOf(t, raw) != 1 {
		t.Fatal("block not delivered after a non-fatal consumer error")
	}

	// A fatal error stops delivery: Deliver is closed.
	cons[0].errs <- ck.NewError(ck.ErrFatal, "fake: fatal", true)
	if _, ok := recv(t, k.Deliver()); ok {
		t.Fatal("Deliver still open after a fatal consumer error")
	}
	if err := k.Close(); err != nil {
		t.Fatalf("Close after fatal error: %v", err)
	}
}
