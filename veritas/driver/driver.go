package driver

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	pbv "hybrid/proto/veritas"
	"hybrid/tso"
	"hybrid/veritas/db"
	"hybrid/veritas/ledger/merkletree"
	"hybrid/veritas/sigs"
)

// ErrProofInvalid: a verified read whose Merkle proof does not match the
// returned root (the value is not used).
var ErrProofInvalid = errors.New("driver: proof invalid")

// ErrCertInvalid: a certified read without Quorum valid attestations from
// distinct nodes on the proof's (height, root).
var ErrCertInvalid = errors.New("driver: certification invalid")

// ReadMode selects how Get reads (ycsbbench --read-mode).
type ReadMode int

const (
	// ReadPlain is the phase-1 Get (no proof).
	ReadPlain ReadMode = iota
	// ReadVerified uses GetVerified and checks the Merkle proof.
	ReadVerified
	// ReadCertified also requires a quorum of root attestations (phase 2, B2).
	ReadCertified
	// ReadQuorum (phase 3) reads plain Gets from f+1 nodes and compares
	// them; the comparison is ycsbbench's reader, the driver's Get is plain.
	ReadQuorum
)

// ParseReadMode parses plain|verified|certified|quorum ("" is plain).
func ParseReadMode(s string) (ReadMode, error) {
	switch s {
	case "", "plain":
		return ReadPlain, nil
	case "verified":
		return ReadVerified, nil
	case "certified":
		return ReadCertified, nil
	case "quorum":
		return ReadQuorum, nil
	}
	return ReadPlain, fmt.Errorf("driver: unknown read mode %q (want plain, verified, certified or quorum)", s)
}

// Options are the phase-2 additions of a Driver (zero value: phase 1).
type Options struct {
	// ClientID and Key sign every Set (--verify-sigs on the nodes); Key nil
	// sends unsigned Sets.
	ClientID string
	Key      ed25519.PrivateKey
	// ReadMode selects what Get does.
	ReadMode ReadMode
	// OnVerify, if set, is called after every verified read with the size
	// of the response message and the CPU-side time of the proof check in
	// microseconds (ycsbbench's read_bytes_mean, verify_cpu_us_mean).
	OnVerify func(respBytes int, verifyMicros int64)
	// NodeKeys and Quorum check certified reads: at least Quorum
	// attestations, from distinct nodes known to NodeKeys, with valid
	// signatures on the response's (height, root).
	NodeKeys func(nodeID int32) (ed25519.PublicKey, bool)
	Quorum   int
	// Dialer replaces the TCP dialer (tests: bufconn).
	Dialer func(ctx context.Context, addr string) (net.Conn, error)
}

type Driver struct {
	signature string
	cc        *grpc.ClientConn
	dbCli     pbv.NodeClient
	tsCli     *tso.Client
	opts      Options
}

// Open is OpenWith without phase-2 options.
func Open(serverAddr, tsoAddr, signature string) (*Driver, error) {
	return OpenWith(serverAddr, tsoAddr, signature, Options{})
}

// OpenWith connects to a node (and the TSO, if tsoAddr is set) with opts.
func OpenWith(serverAddr, tsoAddr, signature string, opts Options) (*Driver, error) {
	dialOpts := []grpc.DialOption{grpc.WithInsecure()}
	if opts.Dialer != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(opts.Dialer))
	}
	cc, err := grpc.Dial(serverAddr, dialOpts...)
	if err != nil {
		return nil, err
	}
	dbCli := pbv.NewNodeClient(cc)

	// An empty tsoAddr means no TSO is used (e.g. veritastm): writes carry
	// Version 0 and no TSO client is created.
	var tsCli *tso.Client
	if tsoAddr != "" {
		tsCli, err = tso.NewClient(tsoAddr)
		if err != nil {
			cc.Close()
			return nil, err
		}
	}

	return &Driver{
		signature: signature,
		cc:        cc,
		dbCli:     dbCli,
		tsCli:     tsCli,
		opts:      opts,
	}, nil
}

func (d *Driver) Begin() (*db.TransactionDB, error) {
	if d.tsCli == nil {
		return nil, errors.New("driver: transactions require a TSO address")
	}
	ts, err := d.tsCli.TS()
	if err != nil {
		return nil, err
	}
	return db.NewTransaction(ts, d.dbCli, d.signature), nil
}

// Get reads key according to Options.ReadMode (plain Get, or GetVerified).
func (d *Driver) Get(ctx context.Context, key string) (string, error) {
	switch d.opts.ReadMode {
	case ReadVerified:
		return d.GetVerified(ctx, key)
	case ReadCertified:
		return d.GetCertified(ctx, key)
	}
	res, err := d.dbCli.Get(ctx, &pbv.GetRequest{
		Signature: d.signature,
		Key:       key,
	})
	if err != nil {
		return "", err
	}
	return res.GetValue(), nil
}

// GetVerified reads key with its Merkle proof and checks the proof against
// the returned root (value+"-"+version is the ledger leaf). A proof that
// does not verify is ErrProofInvalid and no value is returned.
func (d *Driver) GetVerified(ctx context.Context, key string) (string, error) {
	res, err := d.dbCli.GetVerified(ctx, &pbv.GetVerifiedRequest{Key: key})
	if err != nil {
		return "", err
	}
	if err := d.checkProof(key, res); err != nil {
		return "", err
	}
	return res.GetValue(), nil
}

// GetCertified is GetVerified(certified): the proof must verify and the
// response must carry at least Options.Quorum valid attestations from
// distinct nodes on its (height, root); otherwise ErrCertInvalid (or
// ErrProofInvalid) and no value.
func (d *Driver) GetCertified(ctx context.Context, key string) (string, error) {
	res, err := d.dbCli.GetVerified(ctx, &pbv.GetVerifiedRequest{Key: key, Certified: true})
	if err != nil {
		return "", err
	}
	if err := d.checkProof(key, res); err != nil {
		return "", err
	}
	if err := d.checkCert(res); err != nil {
		return "", err
	}
	return res.GetValue(), nil
}

// GetCertifiedResp is GetCertified that returns the response (phase 3):
// signed asks the node to sign it (GetVerifiedRequest.signed). A response
// whose proof or certificate does not verify comes back WITH its
// ErrProofInvalid / ErrCertInvalid error, so a client can report a signed
// one as evidence; an RPC error returns no response.
func (d *Driver) GetCertifiedResp(ctx context.Context, key string, signed bool) (*pbv.GetVerifiedResponse, error) {
	res, err := d.dbCli.GetVerified(ctx, &pbv.GetVerifiedRequest{Key: key, Certified: true, Signed: signed})
	if err != nil {
		return nil, err
	}
	if err := d.checkProof(key, res); err != nil {
		return res, err
	}
	if err := d.checkCert(res); err != nil {
		return res, err
	}
	return res, nil
}

// Evicted returns the node's eviction epoch and evicted nodes (phase 3).
func (d *Driver) Evicted(ctx context.Context) (int64, []int32, error) {
	res, err := d.dbCli.Evicted(ctx, &pbv.EvictedRequest{})
	if err != nil {
		return 0, nil, err
	}
	return res.GetEpoch(), res.GetNodes(), nil
}

// ReportEvidence sends evidence of a bad signed read to the node (phase 3).
func (d *Driver) ReportEvidence(ctx context.Context, ev *pbv.Evidence) error {
	_, err := d.dbCli.ReportEvidence(ctx, ev)
	return err
}

func (d *Driver) checkCert(res *pbv.GetVerifiedResponse) error {
	if d.opts.NodeKeys == nil || d.opts.Quorum < 1 {
		return fmt.Errorf("%w: no node keys / quorum configured", ErrCertInvalid)
	}
	seen := make(map[int32]bool)
	for _, a := range res.GetAttestations() {
		if seen[a.GetNodeId()] || a.GetHeight() != res.GetHeight() || string(a.GetRoot()) != string(res.GetRootDigest()) {
			continue
		}
		pub, ok := d.opts.NodeKeys(a.GetNodeId())
		if !ok || !sigs.VerifyAttest(pub, a.GetHeight(), a.GetRoot(), a.GetSig()) {
			continue
		}
		seen[a.GetNodeId()] = true
	}
	if len(seen) < d.opts.Quorum {
		return fmt.Errorf("%w: %d of %d needed signatures on height %d", ErrCertInvalid, len(seen), d.opts.Quorum, res.GetHeight())
	}
	return nil
}

func (d *Driver) checkProof(key string, res *pbv.GetVerifiedResponse) error {
	t0 := time.Now()
	proof := merkletree.SparseMerkleProof{SideNodes: res.GetSideNodes(), NonMembershipLeafData: res.GetNonMembershipLeafData()}
	leaf := fmt.Sprintf("%s-%d", res.GetValue(), res.GetVersion())
	ok := merkletree.VerifyProof(proof, res.GetRootDigest(), []byte(key), []byte(leaf), sha256.New())
	if d.opts.OnVerify != nil {
		d.opts.OnVerify(proto.Size(res), time.Since(t0).Microseconds())
	}
	if !ok {
		return fmt.Errorf("%w: key %q at height %d", ErrProofInvalid, key, res.GetHeight())
	}
	return nil
}

func (d *Driver) Set(ctx context.Context, key, value string) error {
	var ts int64
	if d.tsCli != nil {
		var err error
		if ts, err = d.tsCli.TS(); err != nil {
			return err
		}
	}
	req := &pbv.SetRequest{
		Signature: d.signature,
		Key:       key,
		Value:     value,
		Version:   ts,
	}
	if d.opts.Key != nil {
		req.ClientId = d.opts.ClientID
		req.Sig = sigs.SignSet(d.opts.Key, key, value, ts)
	}
	if _, err := d.dbCli.Set(ctx, req); err != nil {
		return err
	}
	return nil
}

func (d *Driver) Close() error {
	return d.cc.Close()
}
