package core

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	pbv "hybrid/proto/veritas"
)

const (
	// DefaultBlockSize is the block size used when a size < 1 is given
	// (the experiments' default block size).
	DefaultBlockSize = 100
	// DefaultFlushInterval is the flush period used when a flush <= 0 is
	// given; the original veritas batchLoop flushed every second.
	DefaultFlushInterval = time.Second
	// batcherQueue is the capacity of the Add queue, as the original msgCh.
	batcherQueue = 10000
)

// errBatcherClosed is returned by addCtx after Close.
var errBatcherClosed = errors.New("batcher closed")

// Batcher groups SharedLogs into blocks, like the original veritas batchLoop.
// A block is handed to out when it reaches size SharedLogs, or on the next
// tick of a free-running flush ticker (period flush) if it is non-empty. As
// in the original, the ticker is not reset by a size flush, so a timer flush
// may follow a size flush closely.
//
// Each block is a fresh *pbv.Block{Txs, Type: Approve} with the SharedLogs in
// Add order; Signature is left empty for out to fill (the Server sets its own
// signature). out owns the block. out is called from the batcher's single
// goroutine, so never concurrently and in flush order. An out error is
// logged and the block is dropped; the batcher keeps running.
//
// Close stops the batcher: it flushes whatever was already added (including
// items still queued), in blocks of at most size, then returns. Adds that
// race with Close may be dropped; Adds after Close are dropped and never
// block.
type Batcher struct {
	size    int
	flush   time.Duration
	out     func(*pbv.Block) error
	observe func(batch time.Duration) // optional; called before each out

	in     chan *pbv.SharedLog
	done   chan struct{} // closed by Close
	exited chan struct{} // closed when loop returns
	once   sync.Once
}

// NewBatcher starts a batcher. size < 1 means DefaultBlockSize; flush <= 0
// means DefaultFlushInterval (1 s, as the original).
func NewBatcher(size int, flush time.Duration, out func(*pbv.Block) error) *Batcher {
	return newBatcher(size, flush, out, nil)
}

// newBatcher is NewBatcher with an observe hook that receives, for every
// flushed block, the time from the batcher goroutine's dequeue of the
// block's first SharedLog to the flush (called on the batcher goroutine right
// before out). Time a SharedLog spends in the Add queue before it is
// dequeued (e.g. while the goroutine is blocked in a previous out) is NOT
// included.
func newBatcher(size int, flush time.Duration, out func(*pbv.Block) error, observe func(time.Duration)) *Batcher {
	if size < 1 {
		size = DefaultBlockSize
	}
	if flush <= 0 {
		flush = DefaultFlushInterval
	}
	b := &Batcher{
		size:    size,
		flush:   flush,
		out:     out,
		observe: observe,
		in:      make(chan *pbv.SharedLog, batcherQueue),
		done:    make(chan struct{}),
		exited:  make(chan struct{}),
	}
	go b.loop()
	return b
}

// Add queues sl for the next block. It blocks only while the queue
// (10000 entries) is full; after Close it drops sl and returns at once.
func (b *Batcher) Add(sl *pbv.SharedLog) {
	_ = b.addCtx(context.Background(), sl)
}

// addCtx is Add that gives up when ctx ends (returning ctx.Err()) and
// reports errBatcherClosed when sl was not queued because of Close.
func (b *Batcher) addCtx(ctx context.Context, sl *pbv.SharedLog) error {
	select {
	case <-b.done:
		return errBatcherClosed
	default:
	}
	select {
	case b.in <- sl:
		return nil
	case <-b.done:
		return errBatcherClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes the remainder, stops the batcher goroutine and waits for it.
// It is safe to call more than once and always returns nil.
func (b *Batcher) Close() error {
	b.once.Do(func() { close(b.done) })
	<-b.exited
	return nil
}

func (b *Batcher) loop() {
	defer close(b.exited)
	t := time.NewTicker(b.flush)
	defer t.Stop()

	var cur []*pbv.SharedLog
	var first time.Time
	emit := func() {
		if len(cur) == 0 {
			return
		}
		blk := &pbv.Block{Txs: cur, Type: pbv.MessageType_Approve}
		cur = nil
		if b.observe != nil {
			b.observe(time.Since(first))
		}
		if err := b.out(blk); err != nil {
			log.Printf("core: batcher: block of %d txs dropped: %v", len(blk.Txs), err)
		}
	}
	add := func(sl *pbv.SharedLog) {
		if len(cur) == 0 {
			first = time.Now()
		}
		cur = append(cur, sl)
		if len(cur) >= b.size {
			emit()
		}
	}

	for {
		select {
		case sl := <-b.in:
			add(sl)
		case <-t.C:
			emit()
		case <-b.done:
			for {
				select {
				case sl := <-b.in:
					add(sl)
				default:
					emit()
					return
				}
			}
		}
	}
}
