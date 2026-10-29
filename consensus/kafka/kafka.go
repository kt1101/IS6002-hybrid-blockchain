// Package kafka is the deterministic Kafka approve-all consensus backend.
//
// Every node produces its proposals and its votes to ONE single-partition
// topic and consumes the whole topic with its own consumer group, so every
// node reads every message in the same order. A pure state machine (tracker)
// decides from that message sequence alone when a block has been approved by
// all parties, so every node delivers the same blocks in the same order. See
// tracker.go for the message encoding and the rules.
//
// Differences from the original veritas/server.go protocol (spec decision 8):
// a node's own approve counts only when it is read back from the topic (the
// original counted it at send time, so nodes could disagree), a single Abort
// from any party drops the block for everybody, and votes carry only the
// block id (Txs = [{Seq}]), not the transactions.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/protobuf/proto"

	"hybrid/consensus"
	"hybrid/kafkarole"
	pbv "hybrid/proto/veritas"
)

const (
	// DeliverBuffer is the capacity of the Deliver channel. When it is full
	// the consumer goroutine waits, so it also stops voting (backpressure
	// reaches the other nodes through their pending blocks).
	DeliverBuffer = 128
	// pollTimeout is the ReadMessage timeout; it bounds how long Close waits
	// for the consumer goroutine to notice it.
	pollTimeout = 100 * time.Millisecond
	// queueFullBackoff is the pause before retrying a Produce rejected with
	// ErrQueueFull (librdkafka's local queue is full).
	queueFullBackoff = time.Millisecond
	// closeFlushMs is how long Close waits for queued messages to reach the
	// broker (librdkafka Flush) before closing the producer.
	closeFlushMs = 1000
	// metadataTimeout bounds the whole topic check in New, retries included;
	// metadataAttempt bounds one metadata request, metadataRetryPause is the
	// pause between failed attempts.
	metadataTimeout    = 10 * time.Second
	metadataAttempt    = 2 * time.Second
	metadataRetryPause = 500 * time.Millisecond
)

// ErrClosed is returned by Submit once Close has been called.
var ErrClosed = errors.New("kafka consensus: closed")

// Config configures one node's Kafka backend.
type Config struct {
	// Brokers is the bootstrap server list, Topic the shared single-partition
	// topic (it must exist and have exactly one partition: New checks).
	// Group is this node's consumer group id; it must be unique per node so
	// that every node reads every message from the earliest offset of a
	// fresh topic (empty = Signature). Signature is this node's party name
	// (distinct per node; it is the proposer signature of its blocks).
	Brokers, Topic, Group, Signature string
	// Parties are the signatures whose approval every block needs. Signature
	// is always added. Messages signed by anybody else are ignored.
	Parties map[string]struct{}
	// Validate decides this node's vote on another node's proposal: true =
	// Approve, false = Abort. It gets the proposal's raw bytes and must not
	// keep them. It runs on the consumer goroutine, so it delays the
	// consumption of later messages. nil = always Approve.
	Validate func(blk []byte) bool
	// ShouldVote (Task 15) is asked before each vote; false = send no vote at
	// all (a silent party), so the block stays pending until it is evicted
	// (see tracker). nil = always vote.
	ShouldVote func() bool
	// OnDrop, if set, is called once for every block that will never be
	// delivered: dropped because an Abort vote was read for it, or evicted
	// by the tracker's pending bound (a party stopped voting; logged). It
	// gets the proposer's raw bytes (owned by the callee). It runs on the
	// consumer goroutine, right after the drop decision, in topic order, so
	// it must not block for long and must not call Submit. Every node
	// reports the same drops; core.Server.AbortBlock acts only on its own
	// blocks and answers ErrAborted for both causes. nil = no-op.
	OnDrop func(raw []byte)
}

// producer is the part of *ck.Producer this package uses (a seam for tests).
type producer interface {
	Produce(msg *ck.Message, deliveryChan chan ck.Event) error
	Events() chan ck.Event
	Flush(timeoutMs int) int
	Close()
}

// consumer is the part of *ck.Consumer this package uses (a seam for tests).
type consumer interface {
	ReadMessage(timeout time.Duration) (*ck.Message, error)
	Close() error
}

// Kafka implements consensus.Consensus over a Kafka topic.
//
// Goroutines: one consumer goroutine (consumeLoop) reads the topic, runs the
// tracker, votes, and is the only sender on (and the closer of) the Deliver
// channel. One goroutine drains the producer's delivery reports and logs
// failures (without it librdkafka's reports would pile up in memory, as in
// the original server, which never read them).
//
// Consumer errors: a timeout is the normal idle case. Other non-fatal errors
// (broker down, transport errors) are logged and the loop keeps reading:
// librdkafka reconnects by itself and the committed position is kept, so no
// message is skipped. A fatal error (ck.Error.IsFatal) stops the loop and
// closes Deliver; core.Server then stops the node (crash semantics), which is
// safer than continuing with a consumer that can no longer guarantee the
// order.
//
// Lost messages: if the delivery of a proposal or vote fails (logged by the
// report drainer) the block can never complete; it stays pending until it is
// evicted (tracker maxPending), which reports it through OnDrop (or, without
// OnDrop, its sync waiters time out in core.Server).
type Kafka struct {
	cfg   Config
	topic string // Produce needs a *string; this field is never written
	p     producer
	c     consumer
	tr    *tracker

	out      chan []byte
	done     chan struct{} // closed by Close: stops the consumer loop
	loopDone chan struct{} // closed when consumeLoop has returned (and closed out)
	evStop   chan struct{} // closed by Close after the producer is closed
	evDone   chan struct{} // closed when drainEvents has returned

	mu     sync.RWMutex // guards closed; held (read) around every Produce
	closed bool

	closeOnce sync.Once
	closeErr  error
}

var _ consensus.Consensus = (*Kafka)(nil)

// New connects to the brokers with a fresh producer and a consumer subscribed
// to cfg.Topic (group cfg.Group, or cfg.Signature when empty, reading from
// the earliest offset), checks that the topic has exactly one partition, and
// starts the consumer goroutine.
func New(cfg Config) (*Kafka, error) {
	if cfg.Brokers == "" || cfg.Topic == "" || cfg.Signature == "" {
		return nil, errors.New("kafka consensus: Brokers, Topic and Signature are required")
	}
	if cfg.Group == "" {
		cfg.Group = cfg.Signature
	}
	c, err := kafkarole.NewConsumer(cfg.Brokers, cfg.Group, []string{cfg.Topic})
	if err != nil {
		return nil, fmt.Errorf("kafka consensus: consumer: %w", err)
	}
	if err := checkSinglePartition(c, cfg.Topic); err != nil {
		c.Close()
		return nil, err
	}
	p, err := kafkarole.NewProducer(cfg.Brokers, cfg.Topic)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("kafka consensus: producer: %w", err)
	}
	return newKafka(cfg, p, c), nil
}

// checkSinglePartition fails unless topic exists with exactly one partition:
// with more, consumers would interleave partitions differently and the nodes
// would no longer read one common order. A failed check (broker not reachable
// yet, topic metadata still propagating) is retried every
// metadataRetryPause; the whole check stays within metadataTimeout and
// returns the last error.
func checkSinglePartition(c *ck.Consumer, topic string) error {
	deadline := time.Now().Add(metadataTimeout)
	for {
		attempt := metadataAttempt
		if left := time.Until(deadline); left < attempt {
			attempt = left
		}
		err := checkSinglePartitionOnce(c, topic, int(attempt/time.Millisecond))
		if err == nil {
			return nil
		}
		if time.Until(deadline) <= metadataRetryPause {
			return err
		}
		log.Printf("kafka consensus: topic check failed, retrying: %v", err)
		time.Sleep(metadataRetryPause)
	}
}

func checkSinglePartitionOnce(c *ck.Consumer, topic string, timeoutMs int) error {
	if timeoutMs < 1 {
		timeoutMs = 1
	}
	md, err := c.GetMetadata(&topic, false, timeoutMs)
	if err != nil {
		return fmt.Errorf("kafka consensus: metadata of topic %q: %w", topic, err)
	}
	tm, ok := md.Topics[topic]
	if !ok {
		return fmt.Errorf("kafka consensus: topic %q not in metadata", topic)
	}
	if tm.Error.Code() != ck.ErrNoError {
		return fmt.Errorf("kafka consensus: topic %q: %v", topic, tm.Error)
	}
	if len(tm.Partitions) != 1 {
		return fmt.Errorf("kafka consensus: topic %q has %d partitions, need exactly 1 for a total order", topic, len(tm.Partitions))
	}
	return nil
}

// newKafka wires a backend over p and c (already subscribed) and starts its
// goroutines. Tests pass fakes.
func newKafka(cfg Config, p producer, c consumer) *Kafka {
	if cfg.Group == "" {
		cfg.Group = cfg.Signature
	}
	k := &Kafka{
		cfg:      cfg,
		topic:    cfg.Topic,
		p:        p,
		c:        c,
		tr:       newTracker(cfg.Signature, cfg.Parties),
		out:      make(chan []byte, DeliverBuffer),
		done:     make(chan struct{}),
		loopDone: make(chan struct{}),
		evStop:   make(chan struct{}),
		evDone:   make(chan struct{}),
	}
	k.tr.onDrop = cfg.OnDrop
	go k.drainEvents()
	go k.consumeLoop()
	return k
}

// Submit produces blk (a marshalled pbv.Block proposal) to the topic and
// returns once the producer has accepted it (queued in librdkafka), without
// waiting for the broker or for delivery. blk is copied. It is safe to call
// concurrently with and after Close (then it returns ErrClosed).
func (k *Kafka) Submit(ctx context.Context, blk []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return k.produce(ctx, append([]byte(nil), blk...))
}

// Deliver returns the channel of approved blocks (the proposers' raw bytes,
// each a fresh slice owned by the receiver). It is closed by Close, or when
// the consumer hits a fatal error.
func (k *Kafka) Deliver() <-chan []byte { return k.out }

// Close stops the consumer goroutine (which closes Deliver; blocks still
// buffered in it stay readable), closes the consumer, flushes the producer
// for up to closeFlushMs and closes it. It is idempotent; later Submits
// return ErrClosed. It returns the consumer's Close error.
func (k *Kafka) Close() error {
	k.closeOnce.Do(func() {
		k.mu.Lock() // waits for in-flight Produce calls
		k.closed = true
		k.mu.Unlock()
		close(k.done)
		<-k.loopDone
		k.closeErr = k.c.Close()
		if n := k.p.Flush(closeFlushMs); n > 0 {
			log.Printf("kafka consensus %s: %d messages still queued at close", k.cfg.Signature, n)
		}
		k.p.Close()
		close(k.evStop)
		<-k.evDone
	})
	return k.closeErr
}

// produce sends val to the topic, retrying while librdkafka's local queue is
// full, until ctx ends or the backend is closed.
func (k *Kafka) produce(ctx context.Context, val []byte) error {
	for {
		k.mu.RLock()
		if k.closed {
			k.mu.RUnlock()
			return ErrClosed
		}
		err := k.p.Produce(&ck.Message{
			TopicPartition: ck.TopicPartition{Topic: &k.topic, Partition: ck.PartitionAny},
			Value:          val,
		}, nil)
		k.mu.RUnlock()
		var kerr ck.Error
		if err == nil || !errors.As(err, &kerr) || kerr.Code() != ck.ErrQueueFull {
			return err
		}
		t := time.NewTimer(queueFullBackoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-k.done:
			t.Stop()
			return ErrClosed
		case <-t.C:
		}
	}
}

// consumeLoop reads the topic until Close or a fatal consumer error, feeding
// every message to the tracker in topic order. It closes Deliver on return.
func (k *Kafka) consumeLoop() {
	defer close(k.loopDone)
	defer close(k.out)
	for {
		select {
		case <-k.done:
			return
		default:
		}
		msg, err := k.c.ReadMessage(pollTimeout)
		if err != nil {
			var kerr ck.Error
			if errors.As(err, &kerr) {
				if kerr.Code() == ck.ErrTimedOut {
					continue
				}
				if kerr.IsFatal() {
					log.Printf("kafka consensus %s: fatal consumer error, stopping delivery: %v", k.cfg.Signature, err)
					return
				}
			}
			log.Printf("kafka consensus %s: consumer error (retrying): %v", k.cfg.Signature, err)
			continue
		}
		if msg == nil {
			continue
		}
		if !k.handle(msg.Value) {
			return
		}
	}
}

// handle processes one topic message. It returns false if the backend was
// closed while it waited to deliver.
func (k *Kafka) handle(raw []byte) bool {
	var m pbv.Block
	if err := proto.Unmarshal(raw, &m); err != nil {
		// Every node reads the same bytes, so skipping is deterministic.
		log.Printf("kafka consensus %s: skipping undecodable message: %v", k.cfg.Signature, err)
		return true
	}
	delivered, needVote := k.tr.onMessage(&m, raw)
	if needVote {
		k.vote(&m, raw)
	}
	for _, blk := range delivered {
		select {
		case k.out <- blk: // the tracker's copy, owned by nobody else now
		case <-k.done:
			return false
		}
	}
	return true
}

// vote validates proposal m and produces this node's Approve or Abort. The
// vote counts only when the tracker reads it back from the topic.
func (k *Kafka) vote(m *pbv.Block, raw []byte) {
	if k.cfg.ShouldVote != nil && !k.cfg.ShouldVote() {
		return
	}
	typ := pbv.MessageType_Approve
	if k.cfg.Validate != nil && !k.cfg.Validate(raw) {
		typ = pbv.MessageType_Abort
	}
	seq := m.GetTxs()[0].GetSeq()
	v, err := proto.Marshal(&pbv.Block{
		Type:      typ,
		Signature: k.cfg.Signature,
		Txs:       []*pbv.SharedLog{{Seq: seq}},
	})
	if err != nil {
		log.Printf("kafka consensus %s: marshal vote for %#x: %v", k.cfg.Signature, seq, err)
		return
	}
	if err := k.produce(context.Background(), v); err != nil && !errors.Is(err, ErrClosed) {
		log.Printf("kafka consensus %s: produce %v for %#x: %v", k.cfg.Signature, typ, seq, err)
	}
}

// drainEvents reads the producer's delivery reports and errors, logging
// failures, until the events channel is closed or Close signals evStop.
func (k *Kafka) drainEvents() {
	defer close(k.evDone)
	ev := k.p.Events()
	for {
		select {
		case e, ok := <-ev:
			if !ok {
				return
			}
			switch x := e.(type) {
			case *ck.Message:
				if x.TopicPartition.Error != nil {
					log.Printf("kafka consensus %s: delivery failed: %v", k.cfg.Signature, x.TopicPartition.Error)
				}
			case ck.Error:
				log.Printf("kafka consensus %s: producer error: %v", k.cfg.Signature, x)
			}
		case <-k.evStop:
			return
		}
	}
}
