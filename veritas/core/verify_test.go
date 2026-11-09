package core

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pbv "hybrid/proto/veritas"
	"hybrid/veritas/ledger/merkletree"
	"hybrid/veritas/sigs"
)

const testSeed = "test-seed"

func signedSet(client int, key, val string, ver int64) *pbv.SetRequest {
	id := sigs.ClientID(client)
	return &pbv.SetRequest{Key: key, Value: val, Version: ver, ClientId: id,
		Sig: sigs.SignSet(sigs.KeyFor(testSeed, id), key, val, ver)}
}

func sigCfg(mode sigs.SigMode) ServerConfig {
	c := testCfg(VersionOrdered, CommitSync)
	c.Sigs = mode
	c.ClientKeys = sigs.ClientKeys(testSeed)
	return c
}

func TestSetRejectsBadSigIngress(t *testing.T) {
	_, nodes := newCluster(t, sigCfg(sigs.SigIngress), mapStores(2)...)
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	bad := signedSet(1, "k", "v", 0)
	bad.Value = "tampered"
	_, err := nodes[0].srv.Set(ctx, bad)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Set with a bad signature: %v, want Unauthenticated", err)
	}
	unknown := signedSet(1, "k", "v", 0)
	unknown.ClientId = "mallory"
	if _, err := nodes[0].srv.Set(ctx, unknown); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Set from an unknown client: %v, want Unauthenticated", err)
	}
	if _, err := nodes[0].srv.BatchSet(ctx, &pbv.BatchSetRequest{Sets: []*pbv.SetRequest{signedSet(1, "a", "1", 0), bad}}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("BatchSet with one bad signature: %v, want Unauthenticated", err)
	}
	if _, err := nodes[0].srv.Set(ctx, signedSet(1, "k", "v", 0)); err != nil {
		t.Fatalf("Set with a good signature: %v", err)
	}
	eventually(t, srvTestTimeout, "k on node 2", func() bool {
		v, err := getValue(nodes[1].srv, "k")
		return err == nil && v == "v"
	})
}

func TestSigNoneAcceptsUnsigned(t *testing.T) {
	_, nodes := newCluster(t, testCfg(VersionOrdered, CommitSync), mapStores(1)...)
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	if _, err := nodes[0].srv.Set(ctx, setReq("k", "v", 0)); err != nil {
		t.Fatalf("unsigned Set with --verify-sigs=none: %v", err)
	}
}

func TestApplyDropsBadSigAll(t *testing.T) {
	keys := sigs.ClientKeys(testSeed)
	bad := signedSet(2, "b", "2", 0)
	bad.Sig[0] ^= 1
	blk := mkBlock(mkLog(1, signedSet(1, "a", "1", 0)), mkLog(2, bad), mkLog(3, signedSet(3, "c", "3", 0)))
	var roots [][]byte
	for i := 0; i < 3; i++ {
		ex, store := newTestExecutor(t, VersionOrdered, nil)
		ex.SetSetVerifier(SetVerifier(keys))
		committed, aborted := mustApply(t, ex, 1, blk)
		if !eqSeqs(committed, []int64{1, 3}) || !eqSeqs(aborted, []int64{2}) {
			t.Fatalf("committed %v aborted %v, want [1 3] [2]", committed, aborted)
		}
		if _, ok, _ := store.Get(context.Background(), "b"); ok {
			t.Fatal("the badly signed Set was applied")
		}
		roots = append(roots, ex.RootDigest())
	}
	for i := 1; i < len(roots); i++ {
		if string(roots[i]) != string(roots[0]) {
			t.Fatal("roots differ between executors")
		}
	}
}

func verifyResp(r *pbv.GetVerifiedResponse, key string) bool {
	proof := merkletree.SparseMerkleProof{SideNodes: r.GetSideNodes(), NonMembershipLeafData: r.GetNonMembershipLeafData()}
	return merkletree.VerifyProof(proof, r.GetRootDigest(), []byte(key),
		[]byte(fmt.Sprintf("%s-%d", r.GetValue(), r.GetVersion())), sha256.New())
}

func TestProveValueConsistentUnderApply(t *testing.T) {
	ex, _ := newTestExecutor(t, VersionOrdered, nil)
	mustApply(t, ex, 1, mkBlock(mkLog(1, mkSet("k", "v0", 0))))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for h := int64(2); h <= 201; h++ {
			blk := mkBlock(mkLog(h, mkSet("k", fmt.Sprintf("v%d", h), 0), mkSet(fmt.Sprintf("o%d", h), "x", 0)))
			if _, _, err := ex.Apply(context.Background(), h, rawOf(h), blk); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	lastH := int64(0)
	for i := 0; i < 1000; i++ {
		val, ver, h, root, proof, err := ex.ProveValue([]byte("k"))
		if err != nil {
			t.Fatal(err)
		}
		if h < lastH {
			t.Fatalf("height went back from %d to %d", lastH, h)
		}
		lastH = h
		if ver>>orderedIndexBits != h || val != fmt.Sprintf("v%d", h) && h > 1 {
			t.Fatalf("value %q version %d at height %d", val, ver, h)
		}
		if !merkletree.VerifyProof(proof, root, []byte("k"), []byte(fmt.Sprintf("%s-%d", val, ver)), sha256.New()) {
			t.Fatalf("proof %d does not verify (height %d)", i, h)
		}
	}
	wg.Wait()
	if h := ex.LastHeight(); h != 201 {
		t.Fatalf("LastHeight = %d, want 201", h)
	}
}

func TestGetVerifiedConsistentUnderApply(t *testing.T) {
	cfg := testCfg(VersionOrdered, CommitAsync)
	_, nodes := newCluster(t, cfg, mapStores(1)...)
	srv := nodes[0].srv
	ctx, cancel := context.WithTimeout(context.Background(), srvTestTimeout)
	defer cancel()
	if _, err := srv.Set(ctx, setReq("k", "v", 0)); err != nil {
		t.Fatal(err)
	}
	eventually(t, srvTestTimeout, "k applied", func() bool { _, err := getValue(srv, "k"); return err == nil })
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if _, err := srv.Set(ctx, setReq("k", fmt.Sprintf("v%d", i), 0)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		r, err := srv.GetVerified(ctx, &pbv.GetVerifiedRequest{Key: "k"})
		if err != nil {
			t.Fatal(err)
		}
		if !verifyResp(r, "k") {
			t.Fatalf("response %d (height %d) does not verify", i, r.GetHeight())
		}
	}
	wg.Wait()
	if _, err := srv.GetVerified(ctx, &pbv.GetVerifiedRequest{Key: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetVerified of a missing key: %v, want NotFound", err)
	}
}
