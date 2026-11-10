import csv
from pathlib import Path

import plot
import saturation

OPS_HEADER = "op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted,dropped"


def _row(backend, rate, tps, rep=1, exp="e7"):
    r = {c: "" for c in plot.EXPECTED_COLUMNS}
    r.update(ts="2026-11-06T00:00:00Z", commit="abc", exp=exp, backend=backend, n=4, f=0,
             clients=256, block_size=100, workload="a", rtt_ms=0, fault="none", rep=rep,
             commit_mode="sync", tps=tps, lat_mean_ms=1, lat_p50_ms=1, lat_p99_ms=1,
             errors=0, wrong_gets=0, offered_rps=rate)
    return r


def _fixture(tmp_path, points, suffix="_bf100ms", backend="raft", exp="e7", dropped=0):
    """points: [(rate, tps, write_p99)] -> CSV path; ops sidecars under tmp/raw/<exp>/."""
    raw = tmp_path / "raw"
    (raw / exp).mkdir(parents=True, exist_ok=True)
    csv_path = raw / (exp + ".csv")
    new = not csv_path.exists()
    with open(csv_path, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=plot.EXPECTED_COLUMNS, lineterminator="\n")
        if new:
            w.writeheader()
        for rate, tps, p99 in points:
            row = _row(backend, rate, tps, exp=exp)
            w.writerow(row)
            tag = plot.run_tag(row) + suffix + "_rate%s" % rate
            (raw / exp / (tag + ".ops.csv")).write_text(
                "%s\nread,1,1.00,1.000,1.000,1.000,0,0,%d\nwrite,1,1.00,1.000,1.000,%.3f,0,0,%d\n"
                % (OPS_HEADER, dropped, p99, dropped))
    return csv_path, raw


def test_sat_point(tmp_path):
    csv_path, raw = _fixture(tmp_path, [(500, 499, 300), (1000, 990, 800), (1500, 1200, 2500)])
    res = saturation.analyse([csv_path], raw)
    assert len(res.sat) == 1
    s = res.sat[0]
    assert s["backend"] == "raft" and s["config"] == "default"
    assert s["sat_rps"] == 1000
    assert s["tps_at_sat"] == 990
    assert s["write_p99_ms_at_sat"] == 800
    assert s["reps"] == 1


def test_no_sat_point(tmp_path):
    csv_path, raw = _fixture(tmp_path, [(500, 300, 300), (1000, 400, 800)])
    res = saturation.analyse([csv_path], raw)
    assert res.sat[0]["sat_rps"] == ""
    assert any("raft" in n and "no rate" in n for n in res.notes)


def test_dropped_disqualifies(tmp_path):
    csv_path, raw = _fixture(tmp_path, [(500, 499, 300)], dropped=3)
    res = saturation.analyse([csv_path], raw)
    assert res.sat[0]["sat_rps"] == ""


def test_config_from_tag_suffix(tmp_path):
    csv_path, raw = _fixture(tmp_path, [(500, 499, 300)], suffix="_bf100ms_kc0.5-768m",
                             backend="kafka", exp="e7f-kafka")
    res = saturation.analyse([csv_path], raw)
    assert res.sat[0]["config"] == "kc0.5-768m"


def test_main_writes_csv(tmp_path, capsys):
    csv_path, raw = _fixture(tmp_path, [(500, 499, 300), (1000, 990, 800)])
    out = tmp_path / "out"
    assert saturation.main([str(csv_path), "--ops-dir", str(raw), "--out", str(out)]) == 0
    lines = (out / "saturation.csv").read_text().splitlines()
    assert lines[0] == ("backend,config,sat_rps,tps_at_sat,write_p99_ms_at_sat,reps,"
                        "slo_rps,write_p99_ms_at_slo,e9_rate")
    assert lines[1].startswith("raft,default,1000,")
    assert lines[1].endswith(",1000,800,800")
    assert (out / "saturation_points.csv").exists()
    assert "raft" in capsys.readouterr().out


def test_slo_rate_is_the_knee(tmp_path):
    # 1000 is sustained (p99 <= 2 s) but over the 1 s SLO; 1500 is within the
    # SLO again (noise) and must not count: slo_rps stops at the first miss.
    csv_path, raw = _fixture(tmp_path, [(500, 499, 300), (700, 699, 900), (1000, 995, 1500),
                                        (1500, 1490, 950)])
    res = saturation.analyse([csv_path], raw)
    s = res.sat[0]
    assert s["sat_rps"] == 1500
    assert s["slo_rps"] == 700
    assert s["write_p99_ms_at_slo"] == 900
    assert s["e9_rate"] == 560


def test_slo_ms_option(tmp_path):
    csv_path, raw = _fixture(tmp_path, [(500, 499, 300), (700, 699, 900)])
    res = saturation.analyse([csv_path], raw, slo_ms=500)
    assert res.sat[0]["slo_rps"] == 500
    assert res.sat[0]["e9_rate"] == 400
    assert [p["within_slo"] for p in res.points] == [1, 0]


def test_slo_none(tmp_path):
    csv_path, raw = _fixture(tmp_path, [(500, 499, 1300)])
    res = saturation.analyse([csv_path], raw)
    assert res.sat[0]["slo_rps"] == "" and res.sat[0]["e9_rate"] == ""
    assert res.sat[0]["sat_rps"] == 500
    assert any("SLO" in n for n in res.notes)
