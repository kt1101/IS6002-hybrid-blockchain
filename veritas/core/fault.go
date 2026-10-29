package core

// FaultPolicy lets an experiment inject replica faults into the executor.
// This is the minimal form needed by the executor (Task 6); Task 15 extends
// it (e.g. vote suppression, delayed activation).
type FaultPolicy interface {
	// OnApply is called for every Set the executor applies and returns the
	// value that is actually written. The returned value goes into BOTH the
	// KV store and the ledger, so a corrupting policy makes the replica's
	// root digest diverge as well as its reads.
	OnApply(key, val string) string
}

// NoFault is the honest replica: OnApply returns val unchanged.
type NoFault struct{}

// OnApply implements FaultPolicy.
func (NoFault) OnApply(key, val string) string { return val }
