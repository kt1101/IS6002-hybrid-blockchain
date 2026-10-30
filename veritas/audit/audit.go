// Package audit collects the ledger root digest of every replica over gRPC
// and reports the replicas that disagree with the majority.
//
// Node naming: the i-th address (1-based) of the address list is named
// "node<i>" (see NodeName). This matches the cluster harness, which puts
// node i at 172.28.0.(10+i):1990 and names it node<i>
// (experiments/cluster/gen_compose.py), as long as the list is given in node
// order.
package audit

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"

	pbv "hybrid/proto/veritas"
)

// AuditKey is the key passed to Verify. The proof is ignored; only the root
// digest of the returned response is used.
const AuditKey = "audit"

// PerNodeTimeout bounds the dial plus the Verify call of one node.
var PerNodeTimeout = 10 * time.Second

// NodeName returns the name of the i-th node, 1-based: NodeName(1) == "node1".
func NodeName(i int) string {
	return fmt.Sprintf("node%d", i)
}

// ParseAddrs splits a comma-separated address list. Spaces around an address
// are trimmed; an empty element is an error.
func ParseAddrs(s string) ([]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("audit: empty address list")
	}
	parts := strings.Split(s, ",")
	addrs := make([]string, 0, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, fmt.Errorf("audit: empty address at position %d of %q", i+1, s)
		}
		addrs = append(addrs, p)
	}
	return addrs, nil
}

// CollectRoots calls Verify(AuditKey) on every address concurrently and
// returns the RootDigest of each node, keyed by NodeName of its 1-based
// position in addrs.
//
// If any node cannot be reached or its Verify call fails, CollectRoots returns
// a nil map and an error that names every failed node (and its address); a
// node is never silently dropped.
func CollectRoots(ctx context.Context, addrs []string) (map[string][]byte, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("audit: no addresses")
	}
	type result struct {
		root []byte
		err  error
	}
	results := make([]result, len(addrs))
	done := make(chan struct{}, len(addrs))
	for i, addr := range addrs {
		go func(i int, addr string) {
			root, err := fetchRoot(ctx, addr)
			results[i] = result{root: root, err: err}
			done <- struct{}{}
		}(i, addr)
	}
	for range addrs {
		<-done
	}

	roots := make(map[string][]byte, len(addrs))
	var failed []string
	var failedNames []string
	for i, r := range results {
		name := NodeName(i + 1)
		if r.err != nil {
			failed = append(failed, fmt.Sprintf("%s (%s): %v", name, addrs[i], r.err))
			failedNames = append(failedNames, name)
			continue
		}
		roots[name] = r.root
	}
	if len(failed) > 0 {
		return nil, &CollectError{
			Roots:  roots,
			Failed: failedNames,
			msg:    fmt.Sprintf("audit: cannot collect root digest from %s", strings.Join(failed, "; ")),
		}
	}
	return roots, nil
}

// CollectError is the error CollectRoots returns when at least one node could
// not be queried. CollectRoots itself still returns a nil map; a caller that
// can work with a partial answer (the periodic auditor, which must keep
// ticking while a node is stopped) reads the nodes that did answer from Roots
// (errors.As). Error() names every failed node and its address.
type CollectError struct {
	Roots  map[string][]byte // digest of every node that answered, keyed by node name
	Failed []string          // names of the nodes that did not, in node order
	msg    string
}

func (e *CollectError) Error() string { return e.msg }

// fetchRoot dials addr and returns the root digest of Verify(AuditKey); an
// empty digest is an error (a node with no digest has not reported anything).
// The dial is non-blocking, so an unreachable node fails the RPC fast
// (Unavailable) instead of waiting for the whole timeout.
func fetchRoot(ctx context.Context, addr string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, PerNodeTimeout)
	defer cancel()
	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	res, err := pbv.NewNodeClient(cc).Verify(ctx, &pbv.VerifyRequest{Key: AuditKey})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("nil VerifyResponse")
	}
	if len(res.GetRootDigest()) == 0 {
		return nil, fmt.Errorf("empty root digest")
	}
	return res.GetRootDigest(), nil
}

// majority returns the most common digest and whether it is a strict majority
// (held by more than half of the nodes). With no strict majority the digest
// returned is meaningless.
func majority(roots map[string][]byte) ([]byte, bool) {
	counts := make(map[string]int, len(roots))
	best, bestCount := "", 0
	for _, r := range roots {
		k := string(r)
		counts[k]++
		if counts[k] > bestCount || (counts[k] == bestCount && k < best) {
			best, bestCount = k, counts[k]
		}
	}
	return []byte(best), bestCount*2 > len(roots)
}

// HasMajority reports whether more than half of the nodes hold the same
// digest. It is false for an empty map.
func HasMajority(roots map[string][]byte) bool {
	_, ok := majority(roots)
	return ok
}

// Divergent returns the names (sorted ascending, node2 before node10) of the nodes whose digest differs from
// the majority digest (the one held by more than half of the nodes).
//
// Edge cases:
//   - all digests equal (or no nodes): the result is empty (non-nil);
//   - no strict majority (2-2, all different, 2-1-1, ...): there is no
//     trustworthy reference, so EVERY node is reported as divergent. Use
//     HasMajority to tell this case from a real minority.
func Divergent(roots map[string][]byte) []string {
	out := []string{}
	ref, ok := majority(roots)
	for name, r := range roots {
		if !ok || string(r) != string(ref) {
			out = append(out, name)
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if len(out[a]) != len(out[b]) {
			return len(out[a]) < len(out[b])
		}
		return out[a] < out[b]
	})
	return out
}
