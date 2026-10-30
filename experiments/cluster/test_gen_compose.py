import json

import pytest

from gen_compose import gen, gomemlimit, main


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
    for backend in ("kafka-orig", "tm-orig", "kafka", "raft", "tendermint"):
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


def test_unimplemented_backends_are_rejected(capsys):
    for backend in ("bft2",):
        with pytest.raises(NotImplementedError):
            gen(backend=backend, n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
        assert main(["--backend", backend, "--n", "4"]) != 0
        assert "backend %s not implemented yet" % backend in capsys.readouterr().err


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


@pytest.mark.parametrize("backend", ["kafka", "raft", "tendermint"])
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
    wait = lines.index("until (echo > /dev/tcp/127.0.0.1/26658) 2>/dev/null; do sleep 0.2; done")
    tm = lines.index("tendermint start --home /data/tm --proxy-app=tcp://127.0.0.1:26658 &")
    assert v < wait < tm


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
        assert "--consensus" not in cmd and "trap " not in cmd
        assert not any(v.endswith(":/results") for v in c["services"][k]["volumes"])


def test_cli_accepts_tendermint(tmp_path):
    out = tmp_path / "compose.json"
    assert main(["--backend", "tendermint", "--n", "3", "--commit-mode", "async", "--out", str(out)]) == 0
    c = json.loads(out.read_text())
    assert _nodes(c) == ["node1", "node2", "node3"]
    assert "--consensus=tendermint --commit-mode=async" in c["services"]["node1"]["command"][-1]
