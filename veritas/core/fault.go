package core

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"hybrid/veritas/benchmark"
)

// FaultPolicy lets an experiment inject replica faults (RQ3) into a node.
// The executor calls OnApply for every Set it applies; the Kafka backend asks
// OnVote before each vote (kafka.Config.ShouldVote). Other backends have no
// vote hook: their silent fault (F2) is injected at the network level
// (experiments/faults.sh).
type FaultPolicy interface {
	// OnVote reports whether the node sends its vote (approve/abort) at all;
	// false = a silent party (F2, Kafka backend).
	OnVote() bool
	// OnApply is called for every Set the executor applies and returns the
	// value that is actually written. The returned value goes into BOTH the
	// KV store and the ledger, so a corrupting policy makes the replica's
	// root digest diverge as well as its reads (F4).
	OnApply(key, val string) string
}

// Activator is implemented by the policies ParseFault returns for "silent"
// and "corrupt". Activate switches the fault on immediately, before
// start+after (cmd/veritas calls it on SIGUSR1: the experiment harness
// signals the node at the exact injection time). Both methods are safe for
// concurrent use with OnVote/OnApply.
type Activator interface {
	Activate()
	Active() bool
}

// NoFault is the honest replica: it always votes and OnApply returns val
// unchanged.
type NoFault struct{}

// OnVote implements FaultPolicy.
func (NoFault) OnVote() bool { return true }

// OnApply implements FaultPolicy.
func (NoFault) OnApply(key, val string) string { return val }

// Fault specs accepted by ParseFault.
const (
	FaultNone    = "none"
	FaultSilent  = "silent"
	FaultCorrupt = "corrupt"
)

// ParseFault builds the policy of --fault=spec:
//
//	none     NoFault{}
//	silent   OnVote returns false (no vote is sent); OnApply is honest
//	corrupt  OnApply returns val + benchmark.CorruptMarker; votes normally
//
// A silent or corrupt policy is honest until start+after (cmd/veritas: start
// = the process start, after = --fault-after), or until Activate is called,
// whichever comes first; from then on it stays faulty. after must be >= 0.
func ParseFault(spec string, start time.Time, after time.Duration) (FaultPolicy, error) {
	if after < 0 {
		return nil, fmt.Errorf("fault: negative delay %v", after)
	}
	switch spec {
	case FaultNone:
		return NoFault{}, nil
	case FaultSilent, FaultCorrupt:
		return &delayedFault{kind: spec, at: start.Add(after)}, nil
	default:
		return nil, fmt.Errorf("fault: unknown spec %q (want %s, %s or %s)", spec, FaultNone, FaultSilent, FaultCorrupt)
	}
}

// delayedFault is a silent or corrupt policy that is active from `at` on, or
// from Activate on.
type delayedFault struct {
	kind   string
	at     time.Time
	forced int32 // 1 after Activate (atomic)
	logged int32 // 1 once the activation was logged (atomic)
}

// Active implements Activator.
func (d *delayedFault) Active() bool {
	if atomic.LoadInt32(&d.forced) == 1 {
		return true
	}
	if time.Now().Before(d.at) {
		return false
	}
	if atomic.CompareAndSwapInt32(&d.logged, 0, 1) {
		log.Printf("fault: %s active (delay reached at %s)", d.kind, d.at.UTC().Format(time.RFC3339Nano))
	}
	return true
}

// Activate implements Activator.
func (d *delayedFault) Activate() {
	atomic.StoreInt32(&d.forced, 1)
	if atomic.CompareAndSwapInt32(&d.logged, 0, 1) {
		log.Printf("fault: %s active (activated before its delay)", d.kind)
	}
}

// OnVote implements FaultPolicy.
func (d *delayedFault) OnVote() bool {
	return d.kind != FaultSilent || !d.Active()
}

// OnApply implements FaultPolicy.
func (d *delayedFault) OnApply(key, val string) string {
	if d.kind == FaultCorrupt && d.Active() {
		return val + benchmark.CorruptMarker
	}
	return val
}
