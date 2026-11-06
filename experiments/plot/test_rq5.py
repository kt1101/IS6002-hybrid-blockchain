import csv

import plot
import rq5

FAULTS_HEADER = "utc,epoch_ms,t_s,timeline_s,fault,action,node,detail"
OPS_HEADER = ("op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted,dropped,"
              "read_bytes_mean,verify_cpu_us_mean,proof_errors,cert_errors,dataloss_errors")


def _run(tmp_path, exp="e9-a1-certified", fault="F4:node4@60", backend="bft2", wrong=0,
         diverged_ms=1450, audit=None, rejected=(0, 2, 5), rate=800):
    raw = tmp_path / "raw"
    (raw / exp).mkdir(parents=True, exist_ok=True)
    row = {c: "" for c in plot.EXPECTED_COLUMNS}
    row.update(ts="t", commit="c", exp=exp, backend=backend, n=4, f=1, clients=256, block_size=100,
               workload="a", rtt_ms=0, fault=fault, rep=1, commit_mode="sync", tps=700, lat_mean_ms=1,
               lat_p50_ms=1, lat_p99_ms=1, errors=0, wrong_gets=wrong, offered_rps=rate)
    p = raw / (exp + ".csv")
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=plot.EXPECTED_COLUMNS, lineterminator="\n")
        w.writeheader()
        w.writerow(row)
    tag = plot.run_tag(row) + "_bf100ms_rate%d" % rate
    d = raw / exp
    (d / (tag + ".ops.csv")).write_text(
        "%s\nread,1,1.00,1.000,1.000,2.000,0,0,0,900,30,%d,%d,%d\nwrite,1,1.00,1.000,1.000,123.000,0,0,0,900,30,%d,%d,%d\n"
        % ((OPS_HEADER,) + rejected + rejected))
    if fault != "none":
        (d / (tag + ".faults")).write_text(
            FAULTS_HEADER + "\n1970-01-01T00:00:00Z,0,0,0,%s,run_phase_start,,\n"
            "1970-01-01T00:00:01Z,1000,1,70,%s,corrupt_on,node4,\n" % (fault, fault))
    if diverged_ms is not None:
        s, ms = divmod(diverged_ms, 1000)
        (d / (tag + ".diverged")).write_text(
            "node4 node4  | 1970-01-01T00:00:%02d.%03d000000Z 2026/11/07 core: diverged at height 7 (x)\n" % (s, ms))
    if audit is not None:
        (d / (tag + ".audit")).write_text(audit)
    return p, raw


def test_detect_protocol(tmp_path):
    p, raw = _run(tmp_path)
    res = rq5.analyse([p], raw)
    r = res.runs[0]
    assert r["detect_protocol_s"] == 0.45
    assert r["attest_every"] == 1 and r["read_mode"] == "certified"
    assert r["rejected_gets"] == 7
    assert r["wrong_gets"] == 0
    assert r["write_p99_ms"] == 123.0


def test_no_divergence(tmp_path):
    p, raw = _run(tmp_path, diverged_ms=None)
    res = rq5.analyse([p], raw)
    assert res.runs[0]["detect_protocol_s"] == ""
    assert any("no .diverged" in n for n in res.notes)


def test_detect_auditor(tmp_path):
    p, raw = _run(tmp_path, audit="0.5,\n1.0,\n2.0,\n3.0,node4\n")
    res = rq5.analyse([p], raw)
    assert res.runs[0]["detect_auditor_s"] == 2.0


def test_summary_and_main(tmp_path, capsys):
    p, raw = _run(tmp_path, wrong=3)
    out = tmp_path / "out"
    assert rq5.main([str(p), "--raw-dir", str(raw), "--out", str(out)]) == 0
    runs = (out / "rq5_runs.csv").read_text().splitlines()
    assert runs[0].startswith("exp,backend,attest_every,read_mode,fault,rep,")
    summ = (out / "rq5_summary.csv").read_text().splitlines()
    assert len(summ) == 2 and summ[1].startswith("bft2,1,certified,F4,1,")
    assert "bft2" in capsys.readouterr().out


def test_fault_none(tmp_path):
    p, raw = _run(tmp_path, exp="e9-a0-plain", fault="none", diverged_ms=None)
    r = rq5.analyse([p], raw).runs[0]
    assert r["fault_kind"] == "none" and r["detect_protocol_s"] == "" and r["attest_every"] == 0
