// Package cluster is the live demo's view of a Veritas cluster: the Cluster
// interface every other demo package uses, and its implementations (Sim, an
// in-process cluster on consensus/memnet).
package cluster

import (
	"context"
	"errors"
	"time"
)

// Fault is a kind of replica fault the demo can inject.
type Fault string

// The faults a Cluster may support (ClusterInfo.Faults lists which).
const (
	FaultCrash   Fault = "crash"   // node stops ordering and serving; Heal replays what it missed
	FaultSilent  Fault = "silent"  // node's own blocks are swallowed; it still receives others'
	FaultSlow    Fault = "slow"    // extra delay on the node's submits and deliveries
	FaultCorrupt Fault = "corrupt" // node corrupts every value it applies (diverging root)
)

var (
	// ErrUnsupported: the operation or fault is not available in this mode
	// (e.g. healing a corrupt node).
	ErrUnsupported = errors.New("cluster: not supported in this mode")
	// ErrNodeDown: the node is crashed and refuses requests.
	ErrNodeDown = errors.New("cluster: node is down")
)

// ClusterInfo describes a cluster and the faults it supports.
type ClusterInfo struct {
	Mode     string  `json:"mode"`    // "sim" or "docker"
	Backend  string  `json:"backend"` // consensus backend, e.g. "memnet"
	N        int     `json:"n"`
	F        int     `json:"f"` // tolerated faulty nodes, (N-1)/3
	Faults   []Fault `json:"faults"`
	Healable []Fault `json:"healable"`
	CanReset bool    `json:"can_reset"`
}

// NodeInfo is the status of one node.
type NodeInfo struct {
	Name   string  `json:"name"` // "node1".."nodeN"
	Up     bool    `json:"up"`
	Faults []Fault `json:"faults"` // faults currently active on the node
}

// BlockEvent reports one block applied by one node.
type BlockEvent struct {
	Node     string // node that applied the block
	Height   int64
	Proposer string // signature of the block's proposer ("node2"); "" if unknown
	Txs      int    // number of Sets in the block
	Aborted  int
	Root     []byte // ledger root after the block
	At       time.Time
}

// KV is a stored value with its version.
type KV struct {
	Value   string
	Version int64
}

// Proof is a node's ledger root and a sparse Merkle proof for one key.
type Proof struct {
	Root                  []byte
	SideNodes             [][]byte
	NonMembershipLeafData []byte
}

// Cluster is a Veritas cluster as the demo drives it. Nodes are addressed by
// name ("node1".."nodeN").
type Cluster interface {
	Info() ClusterInfo
	Nodes() []NodeInfo
	// Set writes key=value through node and returns once the node has
	// committed it.
	Set(ctx context.Context, node, key, value string) error
	// Get reads key from node's store; ok=false means the key is missing.
	Get(ctx context.Context, node, key string) (KV, bool, error)
	Prove(ctx context.Context, node, key string) (Proof, error)
	// Events merges the BlockEvents of all nodes. The channel is buffered
	// (4096) and events are dropped when it is full, so producers never block.
	Events() <-chan BlockEvent
	Inject(ctx context.Context, node string, f Fault) error
	Heal(ctx context.Context, node string) error
	Reset(ctx context.Context) error
	Close() error
}

// eventBuffer is the capacity of a Cluster's Events channel.
const eventBuffer = 4096
