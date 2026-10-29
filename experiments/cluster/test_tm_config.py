import base64
import hashlib
import json

import pytest

from tm_config import edit_toml, main

TOML = """moniker = "node0"

[rpc]
laddr = "tcp://127.0.0.1:26657"
max-open-connections = 900
max-subscription-clients = 100
max-subscriptions-per-client = 5

[p2p]
laddr = "tcp://0.0.0.0:26656"
persistent-peers = "x@node1:26656"
addr-book-strict = true
max-packet-msg-payload-size = 1400
send-rate = 5120000
recv-rate = 5120000
allow-duplicate-ip = false

[mempool]
recheck = true
size = 5000
cache-size = 10000
"""

GENESIS = {
    "genesis_time": "2026-10-29T00:00:00.000000Z",
    "chain_id": "chain-abc",
    "initial_height": "1",
    "consensus_params": {
        "block": {"max_bytes": "22020096", "max_gas": "-1"},
        "evidence": {"max_age_num_blocks": "100000", "max_age_duration": "172800000000000", "max_bytes": "1048576"},
        "validator": {"pub_key_types": ["ed25519"]},
        "version": {"app_version": "0"},
    },
    "validators": [],
    "app_hash": "",
}


def test_edit_toml_is_section_aware():
    out, missing = edit_toml(TOML, {("mempool", "size"): "100000", ("p2p", "nope"): "1"})
    assert "size = 100000\ncache-size = 10000\n" in out
    assert missing == [("p2p", "nope")]


def _testnet(tmp_path, n, toml=TOML, genesis=None):
    ids = []
    for j in range(n):
        cfg = tmp_path / ("node%d" % j) / "config"
        cfg.mkdir(parents=True)
        pub = bytes([j + 1]) * 32
        raw = bytes(32) + pub
        nk = {"priv_key": {"type": "tendermint/PrivKeyEd25519", "value": base64.b64encode(raw).decode()}}
        if j == 1:
            nk["id"] = hashlib.sha256(pub).hexdigest()[:40]
        (cfg / "node_key.json").write_text(json.dumps(nk))
        (cfg / "config.toml").write_text(toml)
        (cfg / "genesis.json").write_text(json.dumps(genesis or GENESIS, indent=1))
        ids.append(hashlib.sha256(pub).hexdigest()[:40])
    return ids


def _section(text, name):
    out, cur = [], None
    for line in text.splitlines():
        if line.startswith("["):
            cur = line.strip("[] ")
        elif cur == name:
            out.append(line)
    return out


def test_main_sets_peers_by_ip(tmp_path):
    ids = _testnet(tmp_path, 3)
    assert main(["--dir", str(tmp_path), "--n", "3"]) == 0
    t = (tmp_path / "node1" / "config" / "config.toml").read_text()
    assert 'persistent-peers = "%s@172.28.0.11:26656,%s@172.28.0.13:26656"' % (ids[0], ids[2]) in t
    assert "addr-book-strict = false" in t
    assert "allow-duplicate-ip = true" in t
    assert "max-packet-msg-payload-size = 4096" in t
    assert "send-rate = 20000000" in t and "recv-rate = 20000000" in t
    assert 'laddr = "tcp://127.0.0.1:26657"' in t
    mempool = _section(t, "mempool")
    assert "recheck = false" in mempool and "size = 100000" in mempool


def test_main_raises_rpc_subscription_limits(tmp_path):
    _testnet(tmp_path, 2)
    assert main(["--dir", str(tmp_path), "--n", "2"]) == 0
    for j in range(2):
        rpc = _section((tmp_path / ("node%d" % j) / "config" / "config.toml").read_text(), "rpc")
        assert "max-subscription-clients = 2000" in rpc
        assert "max-open-connections = 2000" in rpc
        assert "max-subscriptions-per-client = 5" in rpc  # default kept


def test_main_fails_without_rpc_limit_keys(tmp_path):
    _testnet(tmp_path, 2, toml=TOML.replace("max-subscription-clients = 100\n", ""))
    with pytest.raises(SystemExit) as e:
        main(["--dir", str(tmp_path), "--n", "2"])
    assert "max-subscription-clients" in str(e.value)


def test_main_fails_without_mempool_recheck(tmp_path):
    _testnet(tmp_path, 2, toml=TOML.replace("recheck = true\n", ""))
    with pytest.raises(SystemExit) as e:
        main(["--dir", str(tmp_path), "--n", "2"])
    assert "recheck" in str(e.value)


def test_genesis_block_max_bytes_identical_on_all_nodes(tmp_path):
    _testnet(tmp_path, 3)
    assert main(["--dir", str(tmp_path), "--n", "3"]) == 0
    raw = [(tmp_path / ("node%d" % j) / "config" / "genesis.json").read_bytes() for j in range(3)]
    assert raw[0] == raw[1] == raw[2]
    g = json.loads(raw[0])
    # v0.35 writes int64 fields as JSON strings; the type is kept.
    assert g["consensus_params"]["block"]["max_bytes"] == "225000"
    assert g["consensus_params"]["block"]["max_gas"] == "-1"
    # evidence.max_bytes must not exceed block.max_bytes (ValidateConsensusParams).
    assert g["consensus_params"]["evidence"]["max_bytes"] == "50000"
    assert g["consensus_params"]["evidence"]["max_age_num_blocks"] == "100000"
    assert g["consensus_params"]["validator"] == GENESIS["consensus_params"]["validator"]
    assert g["chain_id"] == "chain-abc" and g["initial_height"] == "1"


def test_genesis_block_max_bytes_keeps_number_type(tmp_path):
    g = json.loads(json.dumps(GENESIS))
    g["consensus_params"]["block"]["max_bytes"] = 22020096
    g["consensus_params"]["evidence"]["max_bytes"] = 1000
    _testnet(tmp_path, 2, genesis=g)
    assert main(["--dir", str(tmp_path), "--n", "2"]) == 0
    out = json.loads((tmp_path / "node1" / "config" / "genesis.json").read_text())
    assert out["consensus_params"]["block"]["max_bytes"] == 225000
    assert out["consensus_params"]["evidence"]["max_bytes"] == 1000  # already small: kept


def test_genesis_without_block_max_bytes_is_fatal(tmp_path):
    g = json.loads(json.dumps(GENESIS))
    del g["consensus_params"]["block"]["max_bytes"]
    _testnet(tmp_path, 2, genesis=g)
    with pytest.raises(SystemExit) as e:
        main(["--dir", str(tmp_path), "--n", "2"])
    assert "max_bytes" in str(e.value)
