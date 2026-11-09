package sigs

import (
	"bytes"
	"testing"

	pbv "hybrid/proto/veritas"
)

func TestKeyForDeterministic(t *testing.T) {
	a, b := KeyFor("seed", "client1"), KeyFor("seed", "client1")
	if !bytes.Equal(a, b) {
		t.Fatal("same seed and id gave different keys")
	}
	if bytes.Equal(a, KeyFor("seed", "client2")) {
		t.Fatal("different ids gave the same key")
	}
	if bytes.Equal(a, KeyFor("other", "client1")) {
		t.Fatal("different seeds gave the same key")
	}
}

func TestSignVerifySet(t *testing.T) {
	k := KeyFor("seed", "client1")
	pub := Public(k)
	sig := SignSet(k, "key", "value", 42)
	if !VerifySet(pub, "key", "value", 42, sig) {
		t.Fatal("valid signature rejected")
	}
	if VerifySet(pub, "key", "value2", 42, sig) {
		t.Fatal("changed value accepted")
	}
	if VerifySet(pub, "key", "value", 43, sig) {
		t.Fatal("changed version accepted")
	}
	if VerifySet(pub, "ke", "yvalue", 42, sig) {
		t.Fatal("moved separator accepted")
	}
	if VerifySet(Public(KeyFor("seed", "client2")), "key", "value", 42, sig) {
		t.Fatal("other client's key accepted")
	}
	if VerifySet(pub, "key", "value", 42, sig[:10]) {
		t.Fatal("short signature accepted")
	}
}

func TestSetBytesLayout(t *testing.T) {
	got := SetBytes("k", "v", 258)
	want := []byte{'k', 0, 'v', 0, 0, 0, 0, 0, 0, 0, 1, 2}
	if !bytes.Equal(got, want) {
		t.Fatalf("SetBytes = %v, want %v", got, want)
	}
}

func TestParseSigMode(t *testing.T) {
	for s, want := range map[string]SigMode{"none": SigNone, "": SigNone, "ingress": SigIngress, "all": SigAll} {
		got, err := ParseSigMode(s)
		if err != nil || got != want {
			t.Fatalf("ParseSigMode(%q) = %v, %v; want %v", s, got, err, want)
		}
	}
	if _, err := ParseSigMode("some"); err == nil {
		t.Fatal("ParseSigMode accepted an unknown mode")
	}
	if SigAll.String() != "all" {
		t.Fatalf("SigAll.String() = %q", SigAll.String())
	}
}

func TestClientKeys(t *testing.T) {
	keys := ClientKeys("seed")
	pub, ok := keys("client7")
	if !ok || !bytes.Equal(pub, Public(KeyFor("seed", "client7"))) {
		t.Fatal("client7 key wrong")
	}
	for _, bad := range []string{"", "client", "clientx", "node1", "client-1"} {
		if _, ok := keys(bad); ok {
			t.Fatalf("keys(%q) ok", bad)
		}
	}
}

func TestAttestBytesAndSignature(t *testing.T) {
	got := AttestBytes(258, []byte{9, 8})
	want := append([]byte("veritas-attest\x00"), 0, 0, 0, 0, 0, 0, 1, 2, 9, 8)
	if !bytes.Equal(got, want) {
		t.Fatalf("AttestBytes = %v, want %v", got, want)
	}
	k := KeyFor("s", NodeID(3))
	sig := SignAttest(k, 7, []byte("root"))
	if !VerifyAttest(Public(k), 7, []byte("root"), sig) {
		t.Fatal("valid attestation rejected")
	}
	if VerifyAttest(Public(k), 8, []byte("root"), sig) || VerifyAttest(Public(k), 7, []byte("rooT"), sig) {
		t.Fatal("changed height or root accepted")
	}
	keys := NodeKeys("s", 4)
	if pub, ok := keys(3); !ok || !bytes.Equal(pub, Public(k)) {
		t.Fatal("NodeKeys(3) wrong")
	}
	if _, ok := keys(5); ok {
		t.Fatal("node 5 of 4 known")
	}
	if _, ok := keys(0); ok {
		t.Fatal("node 0 known")
	}
}

func testRead() *pbv.SignedRead {
	return &pbv.SignedRead{Key: "k1", Value: "v1", Version: 3, Height: 12, Root: []byte("root"),
		SideNodes: [][]byte{[]byte("s1"), []byte("s2")}, NonMembershipLeafData: nil, NodeId: 4}
}

func TestSignVerifyRead(t *testing.T) {
	k := KeyFor("seed", NodeID(4))
	r := testRead()
	r.Sig = SignRead(k, r)
	if !VerifyRead(Public(k), r) {
		t.Fatal("a signed read does not verify")
	}
	if VerifyRead(Public(KeyFor("seed", NodeID(3))), r) {
		t.Fatal("node 3's key verifies node 4's signature")
	}
	resp := &pbv.GetVerifiedResponse{Value: r.Value, Version: r.Version, Height: r.Height, RootDigest: r.Root,
		SideNodes: r.SideNodes, NodeId: 4, NodeSig: r.Sig}
	if got := SignedReadOf("k1", resp); !VerifyRead(Public(k), got) {
		t.Fatalf("SignedReadOf(response) does not verify: %+v", got)
	}
}

func TestVerifyReadRejectsTamperedValue(t *testing.T) {
	k := KeyFor("seed", NodeID(4))
	for name, tamper := range map[string]func(r *pbv.SignedRead){
		"value":   func(r *pbv.SignedRead) { r.Value = "v1-CORRUPT" },
		"version": func(r *pbv.SignedRead) { r.Version++ },
		"height":  func(r *pbv.SignedRead) { r.Height++ },
		"root":    func(r *pbv.SignedRead) { r.Root = []byte("other") },
		"proof":   func(r *pbv.SignedRead) { r.SideNodes = r.SideNodes[:1] },
		"key":     func(r *pbv.SignedRead) { r.Key = "k2" },
	} {
		r := testRead()
		r.Sig = SignRead(k, r)
		tamper(r)
		if VerifyRead(Public(k), r) {
			t.Fatalf("tampered %s still verifies", name)
		}
	}
	if VerifyRead(Public(k), nil) {
		t.Fatal("nil read verifies")
	}
}

func TestReadBytesSplitsFields(t *testing.T) {
	// key/value boundaries are 0x00-separated: ("a", "bc") and ("ab", "c")
	// must sign differently.
	a := ReadBytes("a", "bc", 1, 1, nil, nil, nil)
	b := ReadBytes("ab", "c", 1, 1, nil, nil, nil)
	if bytes.Equal(a, b) {
		t.Fatal("ReadBytes does not separate key and value")
	}
}
