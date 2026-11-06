import json

import pytest

from gen_compose import gen, gomemlimit, main, parse_node_faults


def _nodes(c):
    return sorted((k for k in c["services"] if k.startswith("node")), key=lambda k: int(k[4:]))


def _ip(c, svc):
    return c["services"][svc]["networks"]["hbdb"]["ipv4_address"]


def test_kafka_compose_has_n_nodes_and_broker():
    c = gen(backend="kafka-orig", n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    assert sorted(k for k in c["services"] if k.startswith("node")) == ["node1", "node2", "node3", "node4"]
    assert c["services"]["node1"]["networks"]["hbdb"]["ipv4_address"] == "172.28.0.11"
    assert "kafka" in c["services"] and "tso" in c["services"]
    assert c["services"]["node2"]["cap_add"] == ["NET_ADMIN"]

    assert c["networks"]["hbdb"]["ipam"]["config"] == [{"subnet": "172.28.0.0/16"}]
    assert [_ip(c, k) for k in _nodes(c)] == ["172.28.0.11", "172.28.0.12", "172.28.0.13", "172.28.0.14"]
    assert _ip(c, "kafka") == "172.28.0.2"
    assert _ip(c, "tso") == "172.28.0.3"
    assert _ip(c, "client") == "172.28.0.4"
    for k in _nodes(c):
        s = c["services"][k]
        assert s["cpus"] == 0.5 and s["mem_limit"] == "768m"
        assert s["depends_on"] == {"kafka": {"condition": "service_healthy"}}
    cmd = c["services"]["node3"]["command"][-1]
    assert "veritas-kafka --signature=node3 --parties=node1,node2,node3,node4 --blk-size=100" in cmd
    assert "--kafka-addr=172.28.0.2:9092 --kafka-group=3 --kafka-topic=shared-log" in cmd
    assert "advertised.listeners=PLAINTEXT://172.28.0.2:9092" in c["services"]["kafka"]["command"][-1]


@pytest.mark.parametrize("backend", ["kafka-orig", "kafka", "raft"])
def test_broker_and_tso_can_take_netem(backend):
    # E5: experiments/net.sh delays the broker's and the TSO's egress too.
    c = gen(backend=backend, n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    assert c["services"]["tso"]["cap_add"] == ["NET_ADMIN"]
    if backend != "raft":
        assert c["services"]["kafka"]["cap_add"] == ["NET_ADMIN"]
    else:
        assert "kafka" not in c["services"]
    assert "cap_add" not in c["services"]["client"]


def test_tendermint_compose_has_no_kafka_or_tso():
    c = gen(backend="tm-orig", n=7, block_size=50, commit_mode="sync", cpus=1, mem="1g",
            tm_dir="/gen/tm")
    assert "kafka" not in c["services"]
    assert "tso" not in c["services"]
    assert _nodes(c) == ["node%d" % i for i in range(1, 8)]
    assert _ip(c, "node7") == "172.28.0.17"
    assert _ip(c, "client") == "172.28.0.4"
    for i, k in enumerate(_nodes(c), start=1):
        s = c["services"][k]
        assert "depends_on" not in s
        assert s["cap_add"] == ["NET_ADMIN"]
        assert s["cpus"] == 1 and s["mem_limit"] == "1g"
        # node$i gets testnet directory node$((i-1)), read-only.
        assert "/gen/tm/node%d:/tmcfg:ro" % (i - 1) in s["volumes"]
        cmd = s["command"][-1]
        assert "veritas-tm --signature=node%d " % i in cmd
        assert "--blk-size=50" in cmd
        assert "tendermint start --home /data/tm" in cmd
        assert "kafka" not in cmd


def test_volumes_are_per_node_and_named():  # để down -v xóa sạch
    for backend in ("kafka-orig", "tm-orig", "kafka", "raft", "tendermint", "bft2"):
        c = gen(backend=backend, n=5, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
        declared = c["volumes"]
        seen = set()
        for i, k in enumerate(_nodes(c), start=1):
            vols = c["services"][k]["volumes"]
            assert "node%d-data:/data" % i in vols
            named = [v.split(":")[0] for v in vols if not v.startswith("/")]
            assert named == ["node%d-data" % i]
            assert not seen.intersection(named), "volume shared between nodes"
            seen.update(named)
        # Every hbdb-node container shadows the image's anonymous VOLUME /data
        # with a declared named volume, and every named source is declared.
        for name, s in c["services"].items():
            vols = s.get("volumes", [])
            for v in vols:
                src = v.split(":")[0]
                if not src.startswith("/"):
                    assert src in declared, (name, v)
            if s["image"] == "hbdb-node":
                assert any(v.endswith(":/data") and not v.startswith("/") for v in vols), name
        assert len(declared) == len(set(declared))


def test_every_service_has_init_and_short_grace():
    c = gen(backend="kafka-orig", n=3, block_size=100, commit_mode="async", cpus=0.5, mem="768m")
    for name, s in c["services"].items():
        assert s["init"] is True, name
        assert s["stop_grace_period"] == "5s", name


def test_every_known_backend_is_implemented(capsys):
    for backend in ("kafka-orig", "tm-orig", "kafka", "raft", "tendermint", "bft2"):
        gen(backend=backend, n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    with pytest.raises(ValueError):
        gen(backend="nope", n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")


def test_cli_writes_json(tmp_path):
    out = tmp_path / "compose.json"
    assert main(["--backend", "kafka-orig", "--n", "2", "--out", str(out)]) == 0
    c = json.loads(out.read_text())
    assert _nodes(c) == ["node1", "node2"]
    assert c["services"]["node1"]["cpus"] == 0.5
    assert c["services"]["node1"]["mem_limit"] == "768m"
    # '$' must be escaped so compose does not interpolate shell variables.
    for s in c["services"].values():
        for part in s.get("command", []):
            assert "$" not in part.replace("$$", "")


def test_gomemlimit_is_70_percent_of_mem_in_mib():
    assert gomemlimit("768m") == "537MiB"  # 537.6 rounded down
    assert gomemlimit("768M") == "537MiB"
    assert gomemlimit("2g") == "1433MiB"  # 1433.6 rounded down
    assert gomemlimit("2G") == "1433MiB"


def test_veritas_server_gets_gomemlimit_on_every_backend():
    c = gen(backend="kafka-orig", n=2, block_size=100, commit_mode="async", cpus=0.5, mem="768m")
    for k in _nodes(c):
        assert "GOMEMLIMIT=537MiB veritas-kafka --signature=%s " % k in c["services"][k]["command"][-1]
    c = gen(backend="tm-orig", n=2, block_size=100, commit_mode="sync", cpus=0.5, mem="2g",
            tm_dir="/gen/tm")
    for k in _nodes(c):
        cmd = c["services"][k]["command"][-1]
        assert "GOMEMLIMIT=1433MiB veritas-tm --signature=%s " % k in cmd
        # Only the veritas server is limited, not tendermint.
        assert "\ntendermint start " in cmd and cmd.count("GOMEMLIMIT") == 1


def test_invalid_mem_is_rejected(capsys):
    for mem in ("lots", "768", "768mb", "1.5t", ""):
        with pytest.raises(ValueError, match="invalid --mem"):
            gomemlimit(mem)
        with pytest.raises(ValueError, match="invalid --mem"):
            gen(backend="kafka-orig", n=1, block_size=100, commit_mode="sync", cpus=0.5, mem=mem)
    assert main(["--backend", "tm-orig", "--n", "1", "--mem", "lots"]) == 1
    assert "invalid --mem 'lots'" in capsys.readouterr().err


def test_node_exits_when_any_long_lived_process_dies():
    expect = {
        "kafka-orig": ("redis-server ", "veritas-kafka "),
        "tm-orig": ("redis-server ", "veritas-tm ", "tendermint start "),
        "kafka": ("redis-server ", "veritas-kafka "),
        "raft": ("redis-server ", "veritas-kafka "),
        "tendermint": ("redis-server ", "veritas-kafka ", "tendermint start "),
        "bft2": ("redis-server ", "bft2-node ", "veritas-kafka "),
    }
    for backend, procs in expect.items():
        c = gen(backend=backend, n=2, block_size=100, commit_mode="sync", cpus=0.5, mem="768m",
                tm_dir="/gen/tm")
        for k in _nodes(c):
            lines = c["services"][k]["command"][-1].rstrip("\n").split("\n")
            assert lines[-3] == "wait -n || true", (backend, k)
            assert lines[-1] == "exit 1", (backend, k)
            assert not any(l.startswith("exec ") or " exec " in l for l in lines), (backend, k)
            for proc in procs:
                started = [l for l in lines if proc in l and not l.startswith(("until", "#", "echo"))]
                assert len(started) == 1 and started[0].endswith(" &"), (backend, k, proc, started)


def test_kafka_backend_runs_core_server_with_kafka_orig_wiring():
    c = gen(backend="kafka", n=4, block_size=50, commit_mode="sync", cpus=0.5, mem="768m",
            results_dir="/res")
    # Same broker and TSO services as kafka-orig.
    assert "kafka" in c["services"] and "tso" in c["services"]
    assert _ip(c, "kafka") == "172.28.0.2" and _ip(c, "tso") == "172.28.0.3"
    assert _nodes(c) == ["node1", "node2", "node3", "node4"]
    sigs = set()
    for i, k in enumerate(_nodes(c), start=1):
        s = c["services"][k]
        assert s["depends_on"] == {"kafka": {"condition": "service_healthy"}}
        assert s["volumes"] == ["node%d-data:/data" % i, "/res:/results"]
        cmd = s["command"][-1]
        assert ("GOMEMLIMIT=537MiB veritas-kafka --consensus=kafka --commit-mode=sync"
                " --version-mode=client --node-id=%d --signature=node%d " % (i, i)) in cmd
        assert "--parties=node1,node2,node3,node4 --blk-size=50 --addr=:1990" in cmd
        assert "--kafka-addr=172.28.0.2:9092 --kafka-topic=shared-log" in cmd
        # One consumer group per node = its signature (no shared --kafka-group).
        assert "--kafka-group" not in cmd
        assert "--redis-addr=127.0.0.1:6379 --redis-db=0 --ledger-path=/data/veritas%d" % i in cmd
        assert "--breakdown-out=/results/breakdown-node%d.json &" % i in cmd
        sigs.add(cmd.split("--signature=")[1].split()[0])
    assert len(sigs) == 4, "signatures must be distinct"


def test_kafka_backend_passes_commit_mode():
    for mode in ("sync", "async"):
        c = gen(backend="kafka", n=2, block_size=100, commit_mode=mode, cpus=0.5, mem="768m")
        for k in _nodes(c):
            assert "--commit-mode=%s " % mode in c["services"][k]["command"][-1]


@pytest.mark.parametrize("backend", ["kafka", "raft", "tendermint", "bft2"])
def test_core_backend_forwards_sigterm_to_veritas(backend):
    # tini signals only bash; bash must pass SIGTERM on so the server can
    # write its breakdown before the stop grace period ends.
    c = gen(backend=backend, n=2, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    for k in _nodes(c):
        lines = c["services"][k]["command"][-1].rstrip("\n").split("\n")
        start = [j for j, l in enumerate(lines) if l.startswith("GOMEMLIMIT=") and "veritas-kafka " in l]
        assert len(start) == 1
        j = start[0]
        # The trap is installed BEFORE veritas starts (no unprotected window)
        # and tolerates an unset pid; the pid is recorded right after.
        assert lines[j - 2] == "veritas_pid="
        trap = lines[j - 1]
        assert trap.startswith("trap 'if [ -n \"$$veritas_pid\" ]; then kill -TERM \"$$veritas_pid\"")
        assert "wait \"$$veritas_pid\"" in trap
        assert trap.endswith("exit 143' TERM INT")
        assert lines[j + 1] == "veritas_pid=$$!"
        assert lines[-3] == "wait -n || true"


def test_original_backends_unchanged_by_kafka():
    c = gen(backend="kafka-orig", n=2, block_size=100, commit_mode="async", cpus=0.5, mem="768m")
    for k in _nodes(c):
        cmd = c["services"][k]["command"][-1]
        assert "--consensus" not in cmd and "trap " not in cmd
        assert "--kafka-group=%s " % k[4:] in cmd
        assert not any(v.endswith(":/results") for v in c["services"][k]["volumes"])


def test_cli_accepts_kafka(tmp_path):
    out = tmp_path / "compose.json"
    assert main(["--backend", "kafka", "--n", "3", "--commit-mode", "async", "--out", str(out)]) == 0
    c = json.loads(out.read_text())
    assert _nodes(c) == ["node1", "node2", "node3"]
    assert "--commit-mode=async" in c["services"]["node1"]["command"][-1]


RAFT_PEERS_4 = ("node1=172.28.0.11:7100/172.28.0.11:7101,node2=172.28.0.12:7100/172.28.0.12:7101,"
                "node3=172.28.0.13:7100/172.28.0.13:7101,node4=172.28.0.14:7100/172.28.0.14:7101")


def test_raft_backend_runs_core_server_with_tso_and_no_kafka():
    c = gen(backend="raft", n=4, block_size=50, commit_mode="sync", cpus=0.5, mem="768m",
            results_dir="/res")
    assert "kafka" not in c["services"]
    assert "tso" in c["services"] and _ip(c, "tso") == "172.28.0.3"
    assert _nodes(c) == ["node1", "node2", "node3", "node4"]
    for i, k in enumerate(_nodes(c), start=1):
        s = c["services"][k]
        assert "depends_on" not in s
        assert s["cap_add"] == ["NET_ADMIN"]
        assert s["volumes"] == ["node%d-data:/data" % i, "/res:/results"]
        cmd = s["command"][-1]
        assert ("GOMEMLIMIT=537MiB veritas-kafka --consensus=raft --commit-mode=sync"
                " --version-mode=client --node-id=%d --signature=node%d " % (i, i)) in cmd
        assert "--parties=node1,node2,node3,node4 --blk-size=50 --addr=:1990" in cmd
        # Every node gets the same full static peer set, raft state in its volume.
        assert " --raft-peers=%s " % RAFT_PEERS_4 in cmd
        assert " --raft-bind=0.0.0.0:7100 --raft-dir=/data/raft" in cmd
        assert "--redis-addr=127.0.0.1:6379 --redis-db=0 --ledger-path=/data/veritas%d" % i in cmd
        assert "--breakdown-out=/results/breakdown-node%d.json &" % i in cmd
        assert "kafka-addr" not in cmd and "kafka-topic" not in cmd and "kafka-group" not in cmd


def test_raft_only_node1_bootstraps():
    c = gen(backend="raft", n=5, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    boot = [k for k in _nodes(c) if "--raft-bootstrap" in c["services"][k]["command"][-1]]
    assert boot == ["node1"]
    assert c["services"]["node1"]["command"][-1].count("--raft-bootstrap") == 1


def test_raft_backend_passes_commit_mode():
    for mode in ("sync", "async"):
        c = gen(backend="raft", n=2, block_size=100, commit_mode=mode, cpus=0.5, mem="768m")
        for k in _nodes(c):
            assert "--consensus=raft --commit-mode=%s " % mode in c["services"][k]["command"][-1]


def test_raft_peers_follow_node_ips():
    c = gen(backend="raft", n=12, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    cmd = c["services"]["node12"]["command"][-1]
    peers = cmd.split("--raft-peers=")[1].split()[0].split(",")
    assert len(peers) == 12
    assert peers[11] == "node12=172.28.0.22:7100/172.28.0.22:7101"
    assert [p.split("=")[0] for p in peers] == ["node%d" % i for i in range(1, 13)]


def test_cli_accepts_raft(tmp_path):
    out = tmp_path / "compose.json"
    assert main(["--backend", "raft", "--n", "3", "--commit-mode", "async", "--out", str(out)]) == 0
    c = json.loads(out.read_text())
    assert _nodes(c) == ["node1", "node2", "node3"]
    assert "--consensus=raft --commit-mode=async" in c["services"]["node1"]["command"][-1]


def test_tendermint_backend_runs_core_server_without_kafka_or_tso():
    c = gen(backend="tendermint", n=4, block_size=50, commit_mode="sync", cpus=0.5, mem="768m",
            tm_dir="/gen/tm", results_dir="/res")
    assert "kafka" not in c["services"] and "tso" not in c["services"]
    assert _nodes(c) == ["node1", "node2", "node3", "node4"]
    for i, k in enumerate(_nodes(c), start=1):
        s = c["services"][k]
        assert "depends_on" not in s
        assert s["cap_add"] == ["NET_ADMIN"]
        # node$i gets testnet directory node$((i-1)) read-only, plus the results dir.
        assert s["volumes"] == ["node%d-data:/data" % i, "/gen/tm/node%d:/tmcfg:ro" % (i - 1), "/res:/results"]
        cmd = s["command"][-1]
        assert ("GOMEMLIMIT=537MiB veritas-kafka --consensus=tendermint --commit-mode=sync"
                " --version-mode=ordered --node-id=%d --signature=node%d " % (i, i)) in cmd
        assert "--parties=node1,node2,node3,node4 --blk-size=50 --addr=:1990" in cmd
        assert " --tm-abci-socket=tcp://0.0.0.0:26658 --tm-rpc-addr=tcp://127.0.0.1:26657 " in cmd
        assert "--redis-addr=127.0.0.1:6379 --redis-db=0 --ledger-path=/data/veritas%d" % i in cmd
        assert "--breakdown-out=/results/breakdown-node%d.json &" % i in cmd
        assert "kafka-addr" not in cmd and "tso" not in cmd
        assert cmd.count("GOMEMLIMIT") == 1  # tendermint itself is not limited


def test_tendermint_starts_after_abci_socket_listens():
    c = gen(backend="tendermint", n=2, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    lines = c["services"]["node2"]["command"][-1].rstrip("\n").split("\n")
    assert lines[1] == "mkdir -p /data/tm && cp -R /tmcfg/. /data/tm/"
    v = [j for j, l in enumerate(lines) if "--consensus=tendermint" in l][0]
    wait = [j for j, l in enumerate(lines) if l.startswith("until (echo > /dev/tcp/127.0.0.1/26658) 2>/dev/null; do")]
    tm = lines.index("tendermint start --home /data/tm --proxy-app=tcp://127.0.0.1:26658 &")
    assert len(wait) == 1 and v < wait[0] < tm
    assert lines[wait[0] - 1] == "abci_wait=0"


def test_tendermint_abci_wait_is_bounded():
    # A server that never listens must end the node script (exit 1) instead
    # of hanging: give up after 300 polls (60 s) or as soon as it died.
    c = gen(backend="tendermint", n=1, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    lines = c["services"]["node1"]["command"][-1].rstrip("\n").split("\n")
    wait = [l for l in lines if l.startswith("until (echo > /dev/tcp/127.0.0.1/26658)")][0]
    assert 'kill -0 "$$veritas_pid" 2>/dev/null || {' in wait
    assert 'if [ "$$abci_wait" -ge 300 ]; then' in wait
    assert wait.count("exit 1") == 2
    assert wait.endswith(" sleep 0.2; done")


def test_tendermint_backend_passes_commit_mode():
    for mode in ("sync", "async"):
        c = gen(backend="tendermint", n=2, block_size=100, commit_mode=mode, cpus=0.5, mem="768m")
        for k in _nodes(c):
            assert "--consensus=tendermint --commit-mode=%s " % mode in c["services"][k]["command"][-1]


def test_tm_orig_unchanged_by_tendermint():
    c = gen(backend="tm-orig", n=2, block_size=100, commit_mode="sync", cpus=0.5, mem="768m",
            tm_dir="/gen/tm")
    for k in _nodes(c):
        cmd = c["services"][k]["command"][-1]
        # tm-orig keeps its original (unbounded) wait for the ABCI socket.
        assert "\nuntil (echo > /dev/tcp/127.0.0.1/26658) 2>/dev/null; do sleep 0.2; done\n" in cmd
        assert "abci_wait" not in cmd
        assert "--consensus" not in cmd and "trap " not in cmd
        assert not any(v.endswith(":/results") for v in c["services"][k]["volumes"])


def test_cli_accepts_tendermint(tmp_path):
    out = tmp_path / "compose.json"
    assert main(["--backend", "tendermint", "--n", "3", "--commit-mode", "async", "--out", str(out)]) == 0
    c = json.loads(out.read_text())
    assert _nodes(c) == ["node1", "node2", "node3"]
    assert "--consensus=tendermint --commit-mode=async" in c["services"]["node1"]["command"][-1]


BFT2_PEERS_4 = "1=172.28.0.11:7200,2=172.28.0.12:7200,3=172.28.0.13:7200,4=172.28.0.14:7200"


def test_bft2_backend_runs_sidecar_and_core_server_without_kafka_or_tso():
    c = gen(backend="bft2", n=4, block_size=50, commit_mode="sync", cpus=0.5, mem="768m",
            results_dir="/res")
    assert "kafka" not in c["services"] and "tso" not in c["services"]
    assert _nodes(c) == ["node1", "node2", "node3", "node4"]
    for i, k in enumerate(_nodes(c), start=1):
        s = c["services"][k]
        assert "depends_on" not in s
        assert s["cap_add"] == ["NET_ADMIN"]
        assert s["volumes"] == ["node%d-data:/data" % i, "/res:/results"]
        cmd = s["command"][-1]
        # Replica i of the static set 1..4, every node gets the same peers.
        assert ("\nGOMEMLIMIT=153MiB bft2-node --id=%d --peers=%s --listen=0.0.0.0:7200 --api=127.0.0.1:7201 --forward-port=7202 &"
                % (i, BFT2_PEERS_4)) in cmd
        assert ("GOMEMLIMIT=430MiB veritas-kafka --consensus=bft2 --commit-mode=sync"
                " --version-mode=ordered --node-id=%d --signature=node%d " % (i, i)) in cmd
        assert "--parties=node1,node2,node3,node4 --blk-size=50 --addr=:1990" in cmd
        assert " --bft2-api=127.0.0.1:7201 " in cmd
        assert "--redis-addr=127.0.0.1:6379 --redis-db=0 --ledger-path=/data/veritas%d" % i in cmd
        assert "--breakdown-out=/results/breakdown-node%d.json &" % i in cmd
        assert "kafka-addr" not in cmd and "tso" not in cmd
        # veritas 70% (537MiB) and the sidecar 20% (153MiB) of the 768m cap.
        assert cmd.count("GOMEMLIMIT") == 2


def test_bft2_veritas_starts_after_local_api_listens_with_bounded_wait():
    c = gen(backend="bft2", n=2, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    lines = c["services"]["node2"]["command"][-1].rstrip("\n").split("\n")
    side = [j for j, l in enumerate(lines) if l.startswith("GOMEMLIMIT=") and " bft2-node " in l]
    wait = [j for j, l in enumerate(lines) if l.startswith("until (echo > /dev/tcp/127.0.0.1/7201) 2>/dev/null; do")]
    v = [j for j, l in enumerate(lines) if "--consensus=bft2" in l]
    assert len(side) == 1 and len(wait) == 1 and len(v) == 1
    assert side[0] < wait[0] < v[0]
    assert lines[side[0] + 1] == "bft2_pid=$$!"
    assert lines[wait[0] - 1] == "api_wait=0"
    w = lines[wait[0]]
    assert 'kill -0 "$$bft2_pid" 2>/dev/null || {' in w
    assert 'if [ "$$api_wait" -ge 300 ]; then' in w
    assert w.count("exit 1") == 2
    assert w.endswith(" sleep 0.2; done")


def test_bft2_peers_follow_node_ips():
    c = gen(backend="bft2", n=7, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    cmd = c["services"]["node7"]["command"][-1]
    peers = cmd.split("--peers=")[1].split()[0].split(",")
    assert peers == ["%d=172.28.0.%d:7200" % (j, 10 + j) for j in range(1, 8)]
    assert "bft2-node --id=7 " in cmd


def test_bft2_backend_passes_commit_mode():
    for mode in ("sync", "async"):
        c = gen(backend="bft2", n=2, block_size=100, commit_mode=mode, cpus=0.5, mem="768m")
        for k in _nodes(c):
            assert "--consensus=bft2 --commit-mode=%s " % mode in c["services"][k]["command"][-1]


def test_cli_accepts_bft2(tmp_path):
    out = tmp_path / "compose.json"
    assert main(["--backend", "bft2", "--n", "4", "--out", str(out)]) == 0
    c = json.loads(out.read_text())
    assert _nodes(c) == ["node1", "node2", "node3", "node4"]
    assert "--consensus=bft2 --commit-mode=sync" in c["services"]["node1"]["command"][-1]


# ---- fault injection (Task 15: veritas --fault, activated by SIGUSR1) ----

def _veritas_line(c, k):
    lines = c["services"][k]["command"][-1].split("\n")
    start = [l for l in lines if l.startswith("GOMEMLIMIT=") and "veritas-kafka " in l]
    assert len(start) == 1
    return start[0]


@pytest.mark.parametrize("backend", ["kafka", "raft", "tendermint", "bft2"])
def test_node_fault_arms_only_the_named_nodes(backend):
    c = gen(backend=backend, n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m",
            node_faults={4: "corrupt", 3: "corrupt"})
    for k in _nodes(c):
        line = _veritas_line(c, k)
        if k in ("node3", "node4"):
            # Armed, never on by itself: faults.sh signals it at AT.
            assert line.endswith(" --breakdown-out=/results/breakdown-%s.json --fault=corrupt --fault-after=87600h &" % k)
        else:
            assert "--fault" not in line and line.endswith(" &")


def test_kafka_silent_node_fault():
    c = gen(backend="kafka", n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m",
            node_faults={4: "silent"})
    assert " --fault=silent --fault-after=87600h &" in _veritas_line(c, "node4")


@pytest.mark.parametrize("backend", ["kafka", "raft", "tendermint", "bft2"])
def test_core_nodes_write_the_veritas_pidfile(backend):
    c = gen(backend=backend, n=2, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    for k in _nodes(c):
        lines = c["services"][k]["command"][-1].split("\n")
        j = lines.index("veritas_pid=$$!")
        assert lines[j + 1] == 'echo "$$veritas_pid" >/tmp/hbdb-veritas.pid'


def test_bad_node_faults_are_rejected(capsys):
    for backend, faults in [
        ("raft", {4: "silent"}),          # no vote hook: F2 is network-level
        ("tendermint", {4: "silent"}),
        ("bft2", {4: "silent"}),
        ("kafka-orig", {4: "corrupt"}),   # original servers have no --fault
        ("tm-orig", {4: "corrupt"}),
        ("kafka", {5: "corrupt"}),        # n=4
        ("kafka", {4: "bogus"}),
    ]:
        with pytest.raises(ValueError):
            gen(backend=backend, n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m",
                node_faults=faults)
    assert main(["--backend", "raft", "--n", "4", "--node-fault", "node4=silent"]) == 1
    assert "silent needs backend kafka" in capsys.readouterr().err
    for bad in (["node4"], ["4=corrupt"], ["node4=corrupt", "node4=corrupt"], ["node0=corrupt"]):
        with pytest.raises(ValueError):
            parse_node_faults(bad)


def test_cli_node_fault(tmp_path):
    out = tmp_path / "compose.json"
    assert main(["--backend", "tendermint", "--n", "4", "--node-fault", "node4=corrupt",
                 "--node-fault", "node3=corrupt", "--out", str(out)]) == 0
    c = json.loads(out.read_text())
    assert "--fault=corrupt" in _veritas_line(c, "node3") and "--fault=corrupt" in _veritas_line(c, "node4")
    assert "--fault" not in _veritas_line(c, "node1")


def test_bft2_fault_goes_to_veritas_not_the_sidecar():
    # F4 / F5-F4 on bft2: the --fault flags and the pid file the injector's
    # SIGUSR1 uses belong to veritas, never to bft2-node.
    c = gen(backend="bft2", n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m",
            node_faults={3: "corrupt", 4: "corrupt"})
    for k in ("node3", "node4"):
        lines = c["services"][k]["command"][-1].split("\n")
        sidecar = [l for l in lines if " bft2-node " in l and l.endswith(" &")]
        assert len(sidecar) == 1 and "--fault" not in sidecar[0]
        assert _veritas_line(c, k).endswith(" --fault=corrupt --fault-after=87600h &")
        pid = [j for j, l in enumerate(lines) if "/tmp/hbdb-veritas.pid" in l]
        assert len(pid) == 1 and lines[pid[0]] == 'echo "$$veritas_pid" >/tmp/hbdb-veritas.pid'
        assert lines[pid[0] - 1] == "veritas_pid=$$!"
        assert lines.index(sidecar[0]) < lines.index(_veritas_line(c, k))
    assert "--fault" not in c["services"]["node1"]["command"][-1]


# ---- BATCH_FLUSH (Task 17: veritas --batch-flush, the core batcher's flush interval) ----

@pytest.mark.parametrize("backend", ["kafka", "raft", "tendermint", "bft2"])
def test_batch_flush_goes_to_veritas_of_every_core_node(backend):
    c = gen(backend=backend, n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m",
            batch_flush="250ms")
    for k in _nodes(c):
        line = _veritas_line(c, k)
        assert line.endswith(" --breakdown-out=/results/breakdown-%s.json --batch-flush=250ms &" % k)
        # only veritas gets it: not the sidecar, redis or the pid file
        assert "--batch-flush" not in c["services"][k]["command"][-1].replace(line, "")


@pytest.mark.parametrize("backend", ["kafka", "raft", "tendermint", "bft2"])
def test_default_has_no_batch_flush_flag(backend):
    # Unchanged compose when the knob is off: the binary's default (1 s) applies.
    c = gen(backend=backend, n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    assert all("--batch-flush" not in c["services"][k]["command"][-1] for k in _nodes(c))
    same = gen(backend=backend, n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m", batch_flush=None)
    assert same == c


def test_batch_flush_after_fault_flags():
    c = gen(backend="raft", n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m",
            node_faults={4: "corrupt"}, batch_flush="1s")
    assert _veritas_line(c, "node4").endswith(" --fault=corrupt --fault-after=87600h --batch-flush=1s &")
    assert _veritas_line(c, "node1").endswith(" --batch-flush=1s &")


def test_bad_batch_flush_is_rejected(capsys):
    for bad in ("0s", "0ms", "1", "s", "1.5s", "-1s", "1 s", "", "1d", "1s\n"):
        with pytest.raises(ValueError):
            gen(backend="raft", n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m", batch_flush=bad)
    for backend in ("kafka-orig", "tm-orig"):
        with pytest.raises(ValueError, match="core backend"):
            gen(backend=backend, n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m", batch_flush="1s")
    assert main(["--backend", "raft", "--n", "4", "--batch-flush", "bogus"]) == 1
    assert "invalid --batch-flush" in capsys.readouterr().err


def test_cli_batch_flush(tmp_path):
    out = tmp_path / "compose.json"
    assert main(["--backend", "kafka", "--n", "4", "--batch-flush", "100ms", "--out", str(out)]) == 0
    c = json.loads(out.read_text())
    assert _veritas_line(c, "node2").endswith(" --batch-flush=100ms &")


def test_bft2_memory_limits_fit_the_node_cap():
    from gen_compose import bft2_veritas_gomemlimit, gomemlimit, BFT2_GOMEMLIMIT_SHARE
    assert bft2_veritas_gomemlimit("768m") == "430MiB"
    assert bft2_veritas_gomemlimit("2g") == "1147MiB"  # (2048 - 409) * 0.7 = 1147.3
    for mem, cap in (("768m", 768), ("1g", 1024), ("2g", 2048)):
        side = int(gomemlimit(mem, BFT2_GOMEMLIMIT_SHARE)[:-3])
        ver = int(bft2_veritas_gomemlimit(mem)[:-3])
        assert side + ver < cap
    c = gen(backend="bft2", n=2, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    cmd = c["services"]["node1"]["command"][-1]
    assert "GOMEMLIMIT=153MiB bft2-node " in cmd and "GOMEMLIMIT=430MiB veritas-kafka " in cmd
    assert "GOMEMLIMIT=537MiB" not in cmd
    # the other core backends keep 70% of the whole cap
    c = gen(backend="raft", n=2, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    assert "GOMEMLIMIT=537MiB veritas-kafka " in c["services"]["node1"]["command"][-1]


def test_kafka_resources_flag(tmp_path):
    out = tmp_path / "c.json"
    assert main(["--backend", "kafka", "--n", "4", "--kafka-cpus", "0.5", "--kafka-mem", "768m",
                 "--out", str(out)]) == 0
    k = json.loads(out.read_text())["services"]["kafka"]
    assert k["cpus"] == 0.5 and k["mem_limit"] == "768m"
    assert main(["--backend", "kafka", "--n", "4", "--out", str(out)]) == 0
    k = json.loads(out.read_text())["services"]["kafka"]
    assert k["cpus"] == 2 and k["mem_limit"] == "1536m"


@pytest.mark.parametrize("backend", ["kafka", "raft", "tendermint", "bft2"])
def test_verify_sigs_and_key_seed_flags(backend):
    doc = gen(backend, 4, 100, "sync", 0.5, "1g", verify_sigs="all", key_seed="seed1")
    for i in range(1, 5):
        cmd = doc["services"]["node%d" % i]["command"][2]
        assert " --verify-sigs=all --key-seed=seed1 &" in cmd
    doc = gen(backend, 4, 100, "sync", 0.5, "1g")
    assert "--verify-sigs" not in doc["services"]["node1"]["command"][2]


def test_verify_sigs_needs_core_backend():
    with pytest.raises(ValueError):
        gen("kafka-orig", 4, 100, "async", 0.5, "1g", verify_sigs="ingress")
    with pytest.raises(ValueError):
        gen("raft", 4, 100, "sync", 0.5, "1g", verify_sigs="some")


def test_attest_every_flags():
    doc = gen("bft2", 4, 100, "sync", 0.5, "1g", key_seed="s", attest_every=10)
    assert " --key-seed=s --attest-every=10 --n=4 &" in doc["services"]["node2"]["command"][2]
    with pytest.raises(ValueError):
        gen("raft", 4, 100, "sync", 0.5, "1g", attest_every=-1)
