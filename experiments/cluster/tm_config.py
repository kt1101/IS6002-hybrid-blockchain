#!/usr/bin/env python3
"""Fix the `tendermint testnet` output for the docker cluster (tm-orig and
the new tendermint backend; --backend picks the values, default tm-orig).

`tendermint testnet --v N --o DIR` writes DIR/node0..node{N-1} with a shared
genesis. This script rewrites each node's config/config.toml the way
scripts/tendermint_config.py did for the original testbed:

  [p2p]     persistent-peers = every other node as <id>@172.28.0.(11+j):26656
            addr-book-strict = false, allow-duplicate-ip = true,
            max-packet-msg-payload-size = 4096,
            send-rate = recv-rate = 20000000
  [mempool] size = 100000, recheck = false
            (v1 mempool re-CheckTx floods "transaction mismatch" errors on the
            socket ABCI client under load)
  [rpc]     max-subscription-clients = 2000, max-open-connections = 2000
            (every veritas-tm Set is a BroadcastTxCommit, which holds one RPC
            subscription; the default of 100 is below E1's 256 clients; the
            tm-orig load uses 64 threads; max-subscriptions-per-client keeps
            its default)

and every node's config/genesis.json (identical bytes on all nodes):

  consensus_params.block.max_bytes = 225000
            tendermint v0.35.0's socket ABCI client rejects async requests once
            256 are queued (abci/client/socket_client.go reqQueueSize) and
            DeliverTx is sent async per tx, so a block of >256 txs fails with
            "can't queue req: buffer is full" and consensus halts. A veritas-tm
            tx is ~1 KB, so 225000 bytes keeps blocks at <= ~220 txs.
  consensus_params.evidence.max_bytes = 50000 (if larger than 225000)
            ValidateConsensusParams rejects evidence.max_bytes > block.max_bytes
            (default 1048576), so it must come down too; evidence actually
            included in a block is a few hundred bytes.
            Values keep the JSON type testnet wrote (v0.35 writes int64 as a
            string).

Backend tendermint (consensus/tendermint: one ABCI tx = one marshalled
Veritas block, ~1 KB per Set, so ~100 KB at block size 100 and ~1 MB at
1000) gets every edit above with these differences:

  [mempool] max-tx-bytes = 4194304 (4 MiB; default 1 MiB is below a block of
            1000 Sets plus headroom)
            max-txs-bytes = 134217728 (128 MiB; the 1 GiB default does not
            fit the 768m node cap, of which the veritas server may use
            GOMEMLIMIT = 537 MiB; a full mempool rejects broadcast_tx_sync,
            which Submit reports as an error)
  [rpc]     max-body-bytes = 8388608 (8 MiB; broadcast_tx_sync carries the
            tx base64-encoded in JSON, 4/3 of its size, so the default 1 MB
            body limit would refuse blocks above ~730 KB)
  genesis   consensus_params.block.max_bytes = 5242880 (5 MiB) instead of
            225000: a block must hold a tx of max-tx-bytes plus header,
            commit (~109 B per validator) and evidence (the mempool reaps in
            order and stops at the first tx that does not fit, so a smaller
            limit would stall on one large tx).
            consensus_params.evidence.max_bytes is capped at 50000 (always,
            not only when above block.max_bytes).
            consensus_params.block.max_gas keeps the testnet default (-1),
            as for tm-orig.

  The 256-request limit above holds for any app speed: execBlockOnProxyApp
  (internal/state/execution.go) enqueues every DeliverTx of a block with
  DeliverTxAsync in a tight loop, faster than the socket client's single
  sender goroutine writes them, so a TM block of noticeably more than 256 txs
  halts consensus. CheckTx is async too (mempool connection): while DeliverTx
  waits on a full Deliver channel, >256 concurrent broadcast_tx_sync calls
  fail with "buffer is full" (Submit errors). 5 MiB alone would allow ~470
  txs at BLOCK_SIZE=10. What keeps blocks below 256 txs is an invariant of
  sync commit: every Veritas block carries at least one outstanding Set, so
  the txs in all mempools <= outstanding Sets <= CLIENTS (run) or
  LOAD_THREADS (load). run_one.sh and check_consistency.sh therefore refuse
  BACKEND=tendermint unless COMMIT_MODE=sync, CLIENTS <= 256 and
  LOAD_THREADS <= 256 (lib.sh hbdb_check_tendermint_limits). A gas cap
  (CheckTx GasWanted = 1 + block.max_gas = 200) was tried and caused a
  reproducible ~30 s stall at load start (empty TM blocks, broadcasts timing
  out), so it was dropped.

tm-orig's values are unchanged.

Directory node$j belongs to compose service node$(j+1) at 172.28.0.(11+j).
Node IDs come from config/node_key.json ("id", or derived from the ed25519
public key as hex(sha256(pubkey)[:20])). Standard library only.
"""

import argparse
import base64
import hashlib
import json
import os
import re
import sys

P2P_PORT = 26656

# Keys whose absence is fatal (the experiments depend on them).
REQUIRED = (
    ("p2p", "persistent-peers"),
    ("rpc", "max-subscription-clients"),
    ("rpc", "max-open-connections"),
    ("mempool", "recheck"),
)

BACKENDS = ("tm-orig", "tendermint")

GENESIS_BLOCK_MAX_BYTES = {"tm-orig": 225000, "tendermint": 5242880}
GENESIS_EVIDENCE_MAX_BYTES = 50000

# Backend tendermint: config.toml edits on top of the common ones, all
# required (a missing key is fatal).
TENDERMINT_MAX_TX_BYTES = 4194304
TENDERMINT_RPC_MAX_BODY_BYTES = 8388608
TENDERMINT_MAX_TXS_BYTES = 134217728
TENDERMINT_EDITS = {
    ("mempool", "max-tx-bytes"): str(TENDERMINT_MAX_TX_BYTES),
    ("mempool", "max-txs-bytes"): str(TENDERMINT_MAX_TXS_BYTES),
    ("rpc", "max-body-bytes"): str(TENDERMINT_RPC_MAX_BODY_BYTES),
}


def _same_type(old, value):
    return str(value) if isinstance(old, str) else value


def edit_genesis(dirs, backend="tm-orig"):
    """Set consensus_params.block.max_bytes in every genesis.json.

    All input genesis files must be equal (testnet writes one genesis); the
    same bytes are written to every node.
    """
    paths = [os.path.join(d, "config", "genesis.json") for d in dirs]
    docs = []
    for path in paths:
        with open(path) as f:
            docs.append(json.load(f))
    for path, doc in zip(paths[1:], docs[1:]):
        if doc != docs[0]:
            raise SystemExit("tm_config: %s differs from %s" % (path, paths[0]))
    doc = docs[0]
    try:
        block = doc["consensus_params"]["block"]
        old = block["max_bytes"]
    except (KeyError, TypeError):
        raise SystemExit("tm_config: consensus_params.block.max_bytes not found in %s" % paths[0])
    try:
        evidence = doc["consensus_params"]["evidence"]
        old_ev = evidence["max_bytes"]
    except (KeyError, TypeError):
        raise SystemExit("tm_config: consensus_params.evidence.max_bytes not found in %s" % paths[0])
    max_bytes = GENESIS_BLOCK_MAX_BYTES[backend]
    block["max_bytes"] = _same_type(old, max_bytes)
    # tm-orig: lowered only when above block.max_bytes (as before);
    # tendermint: always capped at 50000, so the room left for txs never
    # depends on the testnet default (1048576).
    cap_above = max_bytes if backend == "tm-orig" else GENESIS_EVIDENCE_MAX_BYTES
    if int(old_ev) > cap_above:
        evidence["max_bytes"] = _same_type(old_ev, GENESIS_EVIDENCE_MAX_BYTES)
    text = json.dumps(doc, indent=2) + "\n"
    for path in paths:
        with open(path, "w") as f:
            f.write(text)
    sys.stderr.write("tm_config: %s genesis block.max_bytes %r -> %r, evidence.max_bytes %r -> %r on %d nodes\n"
                     % (backend, old, block["max_bytes"], old_ev, evidence["max_bytes"], len(paths)))


def node_ip(j):
    """IP of testnet directory node$j (0-based) == compose node$(j+1)."""
    return "172.28.0.%d" % (11 + j)


def node_id(node_dir):
    with open(os.path.join(node_dir, "config", "node_key.json")) as f:
        nk = json.load(f)
    derived = None
    try:
        raw = base64.b64decode(nk["priv_key"]["value"])
        if len(raw) == 64:  # ed25519: seed || pubkey
            derived = hashlib.sha256(raw[32:]).hexdigest()[:40]
    except (KeyError, TypeError, ValueError):
        pass
    nid = nk.get("id") or derived
    if not nid:
        raise SystemExit("tm_config: cannot determine node id in %s" % node_dir)
    if derived and nk.get("id") and nk["id"] != derived:
        sys.stderr.write("tm_config: warning: %s id %s != derived %s, using id\n"
                         % (node_dir, nk["id"], derived))
    return nid


def edit_toml(text, edits):
    """Replace `key = value` lines inside [section]s.

    edits: {(section, key): value_literal}. Returns (new_text, missing_keys).
    """
    section = ""
    done = set()
    out = []
    header = re.compile(r"^\s*\[([^\]]+)\]\s*$")
    for line in text.splitlines(True):
        m = header.match(line)
        if m:
            section = m.group(1).strip()
            out.append(line)
            continue
        replaced = False
        for (sec, key), val in edits.items():
            if sec == section and re.match(r"^\s*%s\s*=" % re.escape(key), line):
                out.append("%s = %s\n" % (key, val))
                done.add((sec, key))
                replaced = True
                break
        if not replaced:
            out.append(line)
    return "".join(out), [k for k in edits if k not in done]


def main(argv=None):
    p = argparse.ArgumentParser(description="fix tendermint testnet configs for the docker cluster")
    p.add_argument("--dir", required=True, help="testnet output dir (contains node0..)")
    p.add_argument("--n", type=int, required=True)
    p.add_argument("--backend", default="tm-orig", choices=BACKENDS)
    p.add_argument("--timeout-commit", default="", metavar="DURATION",
                   help="[consensus] timeout-commit of every node (e.g. 100ms, 0s); empty: left as"
                        " tendermint's default (1s); a config without the key is then fatal")
    a = p.parse_args(argv)
    if a.timeout_commit and not re.match(r"^[0-9]+(ms|s)$", a.timeout_commit):
        raise SystemExit("tm_config: --timeout-commit must look like 100ms or 0s, got %r" % a.timeout_commit)

    dirs = [os.path.join(a.dir, "node%d" % j) for j in range(a.n)]
    ids = [node_id(d) for d in dirs]
    edit_genesis(dirs, a.backend)
    required = REQUIRED + (tuple(TENDERMINT_EDITS) if a.backend == "tendermint" else ())
    if a.timeout_commit:
        required += (("consensus", "timeout-commit"),)
    for j, d in enumerate(dirs):
        peers = ",".join("%s@%s:%d" % (ids[k], node_ip(k), P2P_PORT) for k in range(a.n) if k != j)
        edits = {
            ("p2p", "persistent-peers"): '"%s"' % peers,
            ("p2p", "addr-book-strict"): "false",
            ("p2p", "allow-duplicate-ip"): "true",
            ("p2p", "max-packet-msg-payload-size"): "4096",
            ("p2p", "send-rate"): "20000000",
            ("p2p", "recv-rate"): "20000000",
            ("mempool", "size"): "100000",
            ("mempool", "recheck"): "false",
            ("rpc", "max-subscription-clients"): "2000",
            ("rpc", "max-open-connections"): "2000",
        }
        if a.backend == "tendermint":
            edits.update(TENDERMINT_EDITS)
        if a.timeout_commit:
            edits[("consensus", "timeout-commit")] = '"%s"' % a.timeout_commit
        path = os.path.join(d, "config", "config.toml")
        with open(path) as f:
            text = f.read()
        text, missing = edit_toml(text, edits)
        fatal = [k for k in required if k in missing]
        if fatal:
            raise SystemExit("tm_config: %s not found in %s"
                             % (", ".join("[%s] %s" % k for k in fatal), path))
        for sec, key in missing:
            sys.stderr.write("tm_config: warning: [%s] %s not found in %s\n" % (sec, key, path))
        with open(path, "w") as f:
            f.write(text)
        sys.stderr.write("tm_config: node%d (%s) id=%s peers=%d\n" % (j, node_ip(j), ids[j], a.n - 1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
