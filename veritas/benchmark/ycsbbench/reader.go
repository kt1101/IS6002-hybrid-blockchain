package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/benchmark"
	"hybrid/veritas/driver"
	"hybrid/veritas/sigs"
)

// nodeAPI is what a reader needs of one node (*driver.Driver).
type nodeAPI interface {
	Get(ctx context.Context, key string) (string, error)
	GetCertifiedResp(ctx context.Context, key string, signed bool) (*pbv.GetVerifiedResponse, error)
	Evicted(ctx context.Context) (int64, []int32, error)
	ReportEvidence(ctx context.Context, ev *pbv.Evidence) error
}

// --sign-reads values.
const (
	signOff       = "off"
	signOnFailure = "on-failure"
	signAlways    = "always"
)

// readerConfig configures a reader (ycsbbench flags).
type readerConfig struct {
	Mode      driver.ReadMode // plain/verified: nodeAPI.Get; certified: GetCertifiedResp; quorum: Get on F+1 nodes
	Failover  bool            // --failover
	SignReads string          // --sign-reads: off | on-failure | always (certified only)
	F         int             // faulty nodes tolerated (quorum reads ask F+1)
}

// ErrNoNode: the reader has excluded every node (evicted or suspected).
var ErrNoNode = errors.New("ycsbbench: every node excluded")

// ErrQuorumMismatch: two sets of F+1 nodes each returned differing values.
var ErrQuorumMismatch = errors.New("ycsbbench: quorum read values differ")

// readStats are the phase 3 counters of the ops sidecar (whole run phase,
// all readers).
type readStats struct {
	Rejected            atomic.Int64 // certified reads refused (proof or certificate)
	QuorumErrors        atomic.Int64 // quorum sets whose values differed
	Failovers           atomic.Int64 // moves of a reader to another node
	WrongBeforeFailover atomic.Int64 // corrupt values accepted from a node the reader left afterwards
	EvidenceSent        atomic.Int64 // bad-read evidence accepted by a node
	EvidenceFailed      atomic.Int64 // bad-read evidence a node refused (e.g. ResourceExhausted: its queue is full)
	CertTimeouts        atomic.Int64 // certified reads that timed out (DeadlineExceeded)
	FirstFailoverMs     atomic.Int64 // unix ms of the first move of any reader, 0 = none
}

// reader reads keys for one worker (closed loop) or one driver slot (open
// loop, shared, hence the mutex). Node i of nodes is node id i+1 (the
// order of --veritas-addrs without duplicates). It reads from its current
// node (primary at first); with Failover it moves round the ring to the next
// node that is neither evicted (the nodes' Evicted list, or a node refusing
// with PermissionDenied) nor suspected (a node that served a read failing
// its proof, or refused with DataLoss). Network calls are made outside the
// mutex.
type reader struct {
	nodes []nodeAPI
	cfg   readerConfig
	stats *readStats

	mu         sync.Mutex
	cur        int
	epoch      int64 // highest eviction epoch seen
	refreshing bool  // an Evicted call is in flight
	evicted    []bool
	suspect    []bool
	corrupt    []int64 // corrupt values accepted from node i since the reader came to it
}

func newReader(nodes []nodeAPI, primary int, cfg readerConfig, stats *readStats) *reader {
	if cfg.SignReads == "" {
		cfg.SignReads = signOff
	}
	return &reader{nodes: nodes, cfg: cfg, stats: stats, cur: primary,
		evicted: make([]bool, len(nodes)), suspect: make([]bool, len(nodes)), corrupt: make([]int64, len(nodes))}
}

// Read reads key according to the reader's mode.
func (r *reader) Read(ctx context.Context, key string) (string, error) {
	switch r.cfg.Mode {
	case driver.ReadQuorum:
		return r.readQuorum(ctx, key)
	case driver.ReadCertified:
		return r.readCertified(ctx, key)
	}
	return r.readPlain(ctx, key)
}

func (r *reader) excludedLocked(i int) bool { return r.evicted[i] || r.suspect[i] }

// pick returns the node to read from: the current one, or without it (with
// Failover) the next one not excluded; ErrNoNode if none is left.
func (r *reader) pick() (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.cfg.Failover || !r.excludedLocked(r.cur) {
		return r.cur, nil
	}
	if !r.moveLocked() {
		return 0, ErrNoNode
	}
	return r.cur, nil
}

// moveLocked moves the reader off its current node to the next node not
// excluded; false if there is none.
func (r *reader) moveLocked() bool {
	n := len(r.nodes)
	for k := 1; k < n; k++ {
		j := (r.cur + k) % n
		if !r.excludedLocked(j) {
			r.stats.Failovers.Add(1)
			r.stats.FirstFailoverMs.CompareAndSwap(0, time.Now().UnixMilli())
			r.stats.WrongBeforeFailover.Add(r.corrupt[r.cur])
			r.corrupt[r.cur] = 0
			r.cur = j
			return true
		}
	}
	return false
}

// exclude marks node i (evicted or suspected) and, with Failover, moves the
// reader off it if it is the current node. It reports whether the read
// should go on with another node.
func (r *reader) exclude(i int, evicted bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if evicted {
		r.evicted[i] = true
	} else {
		r.suspect[i] = true
	}
	if !r.cfg.Failover {
		return false
	}
	if r.cur == i {
		r.moveLocked() // no node left: the next pick returns ErrNoNode
	}
	return true
}

// accepted notes a value read from node i (counts corrupt ones).
func (r *reader) accepted(i int, val string) {
	if strings.HasSuffix(val, benchmark.CorruptMarker) {
		r.mu.Lock()
		r.corrupt[i]++
		r.mu.Unlock()
	}
}

// exclusionErr says whether err shows node i faulty: PermissionDenied (it
// evicted itself) or DataLoss (it diverged).
func exclusionErr(err error) (exclude, evicted bool) {
	switch status.Code(err) {
	case codes.PermissionDenied:
		return true, true
	case codes.DataLoss:
		return true, false
	}
	return false, false
}

func (r *reader) readPlain(ctx context.Context, key string) (string, error) {
	var err error
	for attempt := 0; attempt < len(r.nodes); attempt++ {
		var i int
		if i, err = r.pick(); err != nil {
			return "", err
		}
		var val string
		val, err = r.nodes[i].Get(ctx, key)
		if err == nil {
			r.accepted(i, val)
			return val, nil
		}
		ex, ev := exclusionErr(err)
		if !ex && errors.Is(err, driver.ErrProofInvalid) {
			ex = true // --read-mode=verified
		}
		if !ex || !r.exclude(i, ev) {
			return "", err
		}
	}
	return "", err
}

func (r *reader) readCertified(ctx context.Context, key string) (string, error) {
	var err error
	for attempt := 0; attempt < len(r.nodes); attempt++ {
		var i int
		if i, err = r.pick(); err != nil {
			return "", err
		}
		signed := r.cfg.SignReads == signAlways
		var resp *pbv.GetVerifiedResponse
		resp, err = r.nodes[i].GetCertifiedResp(ctx, key, signed)
		if resp != nil {
			r.checkEpoch(ctx, i, resp.GetEvictedEpoch())
		}
		switch {
		case err == nil:
			r.accepted(i, resp.GetValue())
			return resp.GetValue(), nil
		case status.Code(err) == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded):
			if ctx.Err() == nil { // the node's certified wait, not the run's end
				r.stats.CertTimeouts.Add(1)
			}
			return "", err
		case errors.Is(err, driver.ErrProofInvalid):
			r.stats.Rejected.Add(1)
			if signed {
				r.report(ctx, i, key, resp)
			} else if r.cfg.SignReads == signOnFailure {
				resp2, err2 := r.nodes[i].GetCertifiedResp(ctx, key, true)
				switch {
				case err2 == nil:
					// Evasive: correct once signed. Its value verified;
					// the node stays suspected all the same.
					r.exclude(i, false)
					r.accepted(i, resp2.GetValue())
					return resp2.GetValue(), nil
				case errors.Is(err2, driver.ErrProofInvalid):
					r.report(ctx, i, key, resp2)
				}
			}
			if !r.exclude(i, false) {
				return "", err
			}
		case errors.Is(err, driver.ErrCertInvalid):
			// Not proof of a fault (the node may lag): move on without
			// excluding it.
			r.stats.Rejected.Add(1)
			if !r.cfg.Failover {
				return "", err
			}
			r.mu.Lock()
			if r.cur == i && !r.moveLocked() {
				r.mu.Unlock()
				return "", err
			}
			r.mu.Unlock()
		default:
			ex, ev := exclusionErr(err)
			if !ex || !r.exclude(i, ev) {
				return "", err
			}
		}
	}
	return "", err
}

// report sends evidence of node i's signed response to the next node not
// excluded (EvidenceSent counts the ones accepted). An unsigned response is
// no evidence.
func (r *reader) report(ctx context.Context, i int, key string, resp *pbv.GetVerifiedResponse) {
	if resp == nil || len(resp.GetNodeSig()) == 0 {
		return
	}
	r.mu.Lock()
	to := -1
	for k := 1; k < len(r.nodes); k++ {
		j := (i + k) % len(r.nodes)
		if !r.excludedLocked(j) {
			to = j
			break
		}
	}
	r.mu.Unlock()
	if to < 0 {
		return
	}
	ev := &pbv.Evidence{Culprit: resp.GetNodeId(), Height: resp.GetHeight(), Read: sigs.SignedReadOf(key, resp)}
	if err := r.nodes[to].ReportEvidence(ctx, ev); err == nil {
		r.stats.EvidenceSent.Add(1)
	} else if ctx.Err() == nil {
		r.stats.EvidenceFailed.Add(1)
	}
}

// checkEpoch asks node i for its evicted nodes when it reports an eviction
// epoch above the highest seen (one call at a time).
func (r *reader) checkEpoch(ctx context.Context, i int, epoch int64) {
	r.mu.Lock()
	if epoch <= r.epoch || r.refreshing {
		r.mu.Unlock()
		return
	}
	r.refreshing = true
	r.mu.Unlock()
	got, nodes, err := r.nodes[i].Evicted(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refreshing = false
	if err != nil || got <= r.epoch {
		return
	}
	r.epoch = got
	for _, id := range nodes {
		if k := int(id) - 1; k >= 0 && k < len(r.nodes) {
			r.evicted[k] = true
		}
	}
}

// quorumSet is the first F+1 nodes not excluded from start round the ring
// (nil if fewer are left).
func (r *reader) quorumSet(start int) []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.nodes)
	set := make([]int, 0, r.cfg.F+1)
	for k := 0; k < n && len(set) < r.cfg.F+1; k++ {
		if j := (start + k) % n; !r.excludedLocked(j) {
			set = append(set, j)
		}
	}
	if len(set) < r.cfg.F+1 {
		return nil
	}
	return set
}

// readQuorum reads key from F+1 nodes at once (the current node and the next
// ones not excluded) and returns the value if all agree; otherwise it tries
// the set one node further round the ring once.
func (r *reader) readQuorum(ctx context.Context, key string) (string, error) {
	r.mu.Lock()
	start := r.cur
	r.mu.Unlock()
	var lastErr error
	for try := 0; try < 2; try++ {
		set := r.quorumSet(start)
		if set == nil {
			return "", ErrNoNode
		}
		start = set[0] + 1
		vals := make([]string, len(set))
		errs := make([]error, len(set))
		var wg sync.WaitGroup
		for k, i := range set {
			wg.Add(1)
			go func(k, i int) {
				defer wg.Done()
				vals[k], errs[k] = r.nodes[i].Get(ctx, key)
			}(k, i)
		}
		wg.Wait()
		failed := false
		for k, e := range errs {
			if e == nil {
				continue
			}
			ex, ev := exclusionErr(e)
			if !ex {
				return "", e
			}
			r.exclude(set[k], ev)
			lastErr, failed = e, true
		}
		if failed {
			continue
		}
		agree := true
		for _, v := range vals[1:] {
			agree = agree && v == vals[0]
		}
		if agree {
			return vals[0], nil
		}
		r.stats.QuorumErrors.Add(1)
		lastErr = fmt.Errorf("%w: key %q, nodes %v", ErrQuorumMismatch, key, set)
	}
	return "", lastErr
}
