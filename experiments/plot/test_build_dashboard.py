"""Tests for experiments/plot/build_dashboard.py (stdlib only).

Run: cd experiments/plot && python3 -m pytest -q test_build_dashboard.py
Reads the committed datasets named in dashboard_datasets.json (M3
results/final, Cloud results/cloud-bf100ms, P0 baseline); never writes into
the repo (outputs go to tmp_path).
"""
import json
import re
from pathlib import Path

import pytest

import build_dashboard as bd

ROOT = Path(__file__).resolve().parents[2]
CONFIG = json.loads((Path(__file__).resolve().parent / "dashboard_datasets.json").read_text(encoding="utf-8"))
TEMPLATE = Path(__file__).resolve().parent / "dashboard_template.html"


def _row(rep, tps, lat_mean, lat_p99, errors, backend="raft", clients="4"):
    return {
        "backend": backend, "n": "4", "f": "0", "clients": clients, "block_size": "100",
        "workload": "a", "rtt_ms": "0", "fault": "none", "commit_mode": "sync",
        "rep": str(rep), "tps": str(tps), "lat_mean_ms": str(lat_mean),
        "lat_p99_ms": str(lat_p99), "errors": str(errors),
    }


@pytest.fixture(scope="module")
def data():
    return bd.build(CONFIG, ROOT)


def _ds(data, ds_id):
    return next(d for d in data["datasets"] if d["id"] == ds_id)


def test_aggregate_mean_std():
    rows = [
        _row(1, 10.0, 100.0, 200.0, 1),
        _row(2, 20.0, 200.0, 400.0, 0),
        _row(3, 30.0, 300.0, 600.0, 2),
        _row(1, 5.0, 50.0, 70.0, 0, backend="kafka"),
    ]
    rows[0]["net_bytes_per_block"] = "1000"
    rows[1]["net_bytes_per_block"] = ""
    rows[2]["net_bytes_per_block"] = "3000"
    rows[2]["saturated"] = "1"
    groups = bd.aggregate(rows, bd.GROUP_KEYS)
    assert len(groups) == 2
    raft = next(g for g in groups if g["backend"] == "raft")
    assert raft["reps"] == 3
    assert raft["tps"]["mean"] == pytest.approx(20.0)
    assert raft["tps"]["std"] == pytest.approx(10.0)  # sample std (n-1)
    assert raft["lat_mean_ms"]["mean"] == pytest.approx(200.0)
    assert raft["lat_p99_ms"]["mean"] == pytest.approx(400.0)
    assert raft["errors"] == 3
    assert raft["clients"] == 4 and raft["n"] == 4  # numeric keys parsed
    assert raft["net_bytes_per_block"]["mean"] == pytest.approx(2000.0)  # empty cell skipped
    assert raft["saturated"] is True
    kafka = next(g for g in groups if g["backend"] == "kafka")
    assert kafka["tps"]["std"] is None
    assert kafka["net_bytes_per_block"] is None  # no value at all
    assert kafka["saturated"] is False
    assert "w_lat_mean_ms" not in kafka  # no sidecars given


def test_read_write_latency(tmp_path):
    p = tmp_path / "x.ops.csv"
    p.write_text("op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted\n"
                 "read,10,1,1.0,1.0,2.0,0,0\nwrite,5,1,120.5,100.0,300.25,0,0\n")
    assert bd.read_write_latency(p) == (120.5, 300.25)
    p.write_text("op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted\nwrite,0,0,0,0,0,9,0\n")
    assert bd.read_write_latency(p) is None  # no successful write
    assert bd.read_write_latency(tmp_path / "missing.ops.csv") is None


def test_write_ratio(tmp_path):
    # timeline second 0 = start of the run phase; warmup 10 s; fault at timeline 20 s
    w = [100] * 20 + [0] * 10 + [50] * 40
    (tmp_path / "r.wtimeline").write_text("\n".join(str(x) for x in w) + "\n")
    (tmp_path / "r.faults").write_text(
        "utc,epoch_ms,t_s,timeline_s,fault,action,node,detail\n"
        "2026-11-02T00:00:00Z,1000,,0,F1:node4@10,run_phase_start,,\n"
        "2026-11-02T00:00:20Z,21000,10,20,F1:node4@10,docker_stop,node4,x\n")
    # pre = mean of seconds 10..19 = 100; post = seconds 30.. = 50
    assert bd.write_ratio(tmp_path / "r.wtimeline", tmp_path / "r.faults") == pytest.approx(0.5)
    assert bd.write_ratio(tmp_path / "none.wtimeline", tmp_path / "r.faults") is None


def test_build_reads_both_datasets(data):
    assert [d["id"] for d in data["datasets"]] == ["cloud", "m3"]
    m3, cloud = _ds(data, "m3"), _ds(data, "cloud")
    for exp in ("e1", "e1-bf100ms", "e2", "e3", "e4", "e5", "e6"):
        assert m3["experiments"][exp], exp
    assert "e5" not in cloud["experiments"] and "e5" in cloud["missing"]
    assert m3["write_latency"] is False and cloud["write_latency"] is True
    assert {g["backend"] for g in cloud["experiments"]["e1"]} == {"kafka", "raft", "tendermint", "bft2"}
    # write latency from the ops sidecars of every rep
    raft256 = next(g for g in cloud["experiments"]["e1"] if g["backend"] == "raft" and g["clients"] == 256)
    assert raft256["w_reps"] == raft256["reps"]
    assert raft256["w_lat_mean_ms"]["mean"] > raft256["lat_mean_ms"]["mean"]  # writes wait for a block, reads do not
    assert raft256["net_bytes_per_block"]["mean"] > 0
    # E6: MEM=1g supplement merged in, write ratio computed
    e6 = cloud["experiments"]["e6"]
    bft2_f4 = [g for g in e6 if g["backend"] == "bft2" and g["fault"].startswith("F4:")]
    assert bft2_f4 and all(g["mem"] == "1g" for g in bft2_f4)
    raft_f1 = next(g for g in e6 if g["backend"] == "raft" and g["n"] == 4 and g["fault"] == "F1:node4@60")
    assert raft_f1["write_ratio"]["n"] == 3
    assert 0.5 < raft_f1["write_ratio"]["mean"] < 0.8  # NOTES: raft F1 N=4 0.60-0.67
    kafka_f1 = next(g for g in e6 if g["backend"] == "kafka" and g["n"] == 4 and g["fault"] == "F1:node4@60")
    assert kafka_f1["write_ratio"]["max"] == 0  # approve-all stops writing
    # RQ3 summary incl. the 1g supplement; Kafka counters: reads = N x writes
    assert any(r.get("mem") == "1g" for r in cloud["rq3"])
    assert all(isinstance(r["recovery_status"], str) for r in m3["rq3"])
    assert any(r["recovery_s_mean"] is None for r in m3["rq3"])
    ko = cloud["kafka_ops"]
    assert [k["n"] for k in ko] == [4, 7]
    for k in ko:
        assert k["reads"]["mean"] == pytest.approx(k["n"] * k["writes"]["mean"], rel=0.01)
    assert {g["backend"] for g in data["p0"]} == {"kafka-orig", "tm-orig"}


def test_render_embeds_valid_json(data):
    data = json.loads(json.dumps(data))
    data["datasets"][0]["experiments"]["e1"][0]["backend"] = "x</script><b>"  # must stay inert
    html = bd.render(data, TEMPLATE.read_text(encoding="utf-8"))
    assert "/*__DATA__*/" not in html
    m = re.search(r'<script type="application/json" id="data">(.*?)</script>', html, re.S)
    assert m, "data script tag missing"
    parsed = json.loads(m.group(1))
    assert parsed["datasets"][0]["experiments"]["e1"][0]["backend"] == "x</script><b>"
    assert "</script><b>" not in m.group(1)
    assert "<script src=" not in html
    assert not re.search(r'<link[^>]*href="http', html)
    for line in html.splitlines():
        if "http://" in line or "https://" in line:
            pytest.fail("external URL in output: %s" % line.strip())
    assert "Results overview" in html


def test_main_writes_identical_outputs(tmp_path):
    a, b = tmp_path / "a.html", tmp_path / "sub" / "b.html"
    rc = bd.main(["--base", str(ROOT), "--out", str(a), "--out", str(b)])
    assert rc == 0
    assert a.read_bytes() == b.read_bytes()
