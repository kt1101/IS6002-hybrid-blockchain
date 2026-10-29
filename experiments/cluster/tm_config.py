#!/usr/bin/env python3
"""Fix the `tendermint testnet` output for the docker cluster (tm-orig).

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

GENESIS_BLOCK_MAX_BYTES = 225000
GENESIS_EVIDENCE_MAX_BYTES = 50000


def _same_type(old, value):
    return str(value) if isinstance(old, str) else value


def edit_genesis(dirs):
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
    block["max_bytes"] = _same_type(old, GENESIS_BLOCK_MAX_BYTES)
    if int(old_ev) > GENESIS_BLOCK_MAX_BYTES:
        evidence["max_bytes"] = _same_type(old_ev, GENESIS_EVIDENCE_MAX_BYTES)
    text = json.dumps(doc, indent=2) + "\n"
    for path in paths:
        with open(path, "w") as f:
            f.write(text)
    sys.stderr.write("tm_config: genesis block.max_bytes %r -> %r, evidence.max_bytes %r -> %r on %d nodes\n"
                     % (old, block["max_bytes"], old_ev, evidence["max_bytes"], len(paths)))


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
    a = p.parse_args(argv)

    dirs = [os.path.join(a.dir, "node%d" % j) for j in range(a.n)]
    ids = [node_id(d) for d in dirs]
    edit_genesis(dirs)
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
        path = os.path.join(d, "config", "config.toml")
        with open(path) as f:
            text = f.read()
        text, missing = edit_toml(text, edits)
        fatal = [k for k in REQUIRED if k in missing]
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
