"""Tests for experiments/plot/build_dashboard.py (stdlib only).

Run: cd experiments/plot && python3 -m pytest -q test_build_dashboard.py
Reads the committed results/final CSVs and results/figs/rq3_summary.csv;
never writes into the repo (outputs go to tmp_path).
"""
import json
import re
from pathlib import Path

import pytest

import build_dashboard as bd

ROOT = Path(__file__).resolve().parents[2]
FINAL = ROOT / "results" / "final"
RQ3 = ROOT / "results" / "figs" / "rq3_summary.csv"
TEMPLATE = Path(__file__).resolve().parent / "dashboard_template.html"
EXPERIMENTS = ("e1", "e1-bf100ms", "e2", "e3", "e4", "e5", "e6")


def _row(rep, tps, lat_mean, lat_p99, errors, backend="raft", clients="4"):
    return {
        "backend": backend, "n": "4", "f": "0", "clients": clients, "block_size": "100",
        "workload": "a", "rtt_ms": "0", "fault": "none", "commit_mode": "sync",
        "rep": str(rep), "tps": str(tps), "lat_mean_ms": str(lat_mean),
        "lat_p99_ms": str(lat_p99), "errors": str(errors),
    }


def test_aggregate_mean_std():
    rows = [
        _row(1, 10.0, 100.0, 200.0, 1),
        _row(2, 20.0, 200.0, 400.0, 0),
        _row(3, 30.0, 300.0, 600.0, 2),
        _row(1, 5.0, 50.0, 70.0, 0, backend="kafka"),
    ]
    groups = bd.aggregate(rows, bd.GROUP_KEYS)
    assert len(groups) == 2
    raft = next(g for g in groups if g["backend"] == "raft")
    assert raft["reps"] == 3
    assert raft["tps"]["mean"] == pytest.approx(20.0)
    assert raft["tps"]["std"] == pytest.approx(10.0)  # sample std (n-1)
    assert raft["lat_mean_ms"]["mean"] == pytest.approx(200.0)
    assert raft["lat_mean_ms"]["std"] == pytest.approx(100.0)
    assert raft["lat_p99_ms"]["mean"] == pytest.approx(400.0)
    assert raft["lat_p99_ms"]["std"] == pytest.approx(200.0)
    assert raft["errors"] == 3
    assert raft["clients"] == 4 and raft["n"] == 4  # numeric keys parsed
    kafka = next(g for g in groups if g["backend"] == "kafka")
    assert kafka["reps"] == 1
    assert kafka["tps"]["mean"] == pytest.approx(5.0)
    assert kafka["tps"]["std"] is None


def test_build_reads_all_experiments():
    data = bd.build(FINAL, RQ3)
    assert isinstance(data["generated"], str) and data["generated"]
    for exp in EXPERIMENTS:
        assert exp in data["experiments"], exp
        assert data["experiments"][exp], exp
    faults = {g["fault"] for g in data["experiments"]["e6"]}
    assert any(f.startswith("F1:") for f in faults)
    assert {g["backend"] for g in data["experiments"]["e1"]} == {"kafka", "raft", "tendermint", "bft2"}
    assert data["rq3"]
    first = data["rq3"][0]
    assert isinstance(first["pre_mean_tps_mean"], float)
    assert isinstance(first["n"], float)
    assert isinstance(first["backend"], str)
    # empty numeric cells become None
    assert any(r["recovery_s_mean"] is None for r in data["rq3"])
    assert all(isinstance(r["recovery_status"], str) for r in data["rq3"])


def test_render_embeds_valid_json(tmp_path):
    data = bd.build(FINAL, RQ3)
    data["experiments"]["e1"][0]["backend"] = "x</script><b>"  # must stay inert
    html = bd.render(data, TEMPLATE.read_text(encoding="utf-8"))
    assert "/*__DATA__*/" not in html
    m = re.search(r'<script type="application/json" id="data">(.*?)</script>', html, re.S)
    assert m, "data script tag missing"
    parsed = json.loads(m.group(1))
    assert parsed["experiments"]["e1"][0]["backend"] == "x</script><b>"
    assert "</script><b>" not in m.group(1)
    assert "<script src=" not in html
    assert not re.search(r'<link[^>]*href="http', html)
    # Only the Live demo link logic may mention http(s) at all.
    for line in html.splitlines():
        if "http://" in line or "https://" in line:
            pytest.fail("external URL in output: %s" % line.strip())
    assert "Results overview" in html


def test_main_writes_identical_outputs(tmp_path):
    a, b = tmp_path / "a.html", tmp_path / "sub" / "b.html"
    rc = bd.main(["--final", str(FINAL), "--rq3", str(RQ3), "--out", str(a), "--out", str(b)])
    assert rc == 0
    assert a.read_bytes() == b.read_bytes()
