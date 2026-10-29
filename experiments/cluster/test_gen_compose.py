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
    for backend in ("kafka-orig", "tm-orig"):
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
    for backend in ("kafka", "raft", "tendermint", "bft2"):
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
