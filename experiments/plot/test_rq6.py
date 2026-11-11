import csv

import pytest

import rq6

FAULTS_HEADER = "utc,epoch_ms,t_s,timeline_s,fault,action,node,detail"
OPS_HEADER = ("op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted,dropped,"
              "read_bytes_mean,verify_cpu_us_mean,proof_errors,cert_errors,dataloss_errors,"
              "rejected_reads,failovers,evidence_sent,cert_timeouts")


def _ts(sec):
    """Docker log timestamp of `sec` seconds after the epoch (sec < 3600)."""
    m, s = divmod(sec, 60)
    return "1970-01-01T00:%02d:%09.6fZ" % (m, s)


def _run(tmp_path, exp="e11-F6-cert-fo-sign", fault="F6:node4@60", backend="raft", n=4, wrong=0,
         inject_action="readlie_on", evictions=(), certs=(), ops=(3, 4, 2, 0), rate=800, first_fo=None, ev_failed=None):
    """One CSV row plus its sidecars; evictions: (reporter, sec, culprit id, height, reason)."""
    raw = tmp_path / "raw"
    (raw / exp).mkdir(parents=True, exist_ok=True)
    row = {c: "" for c in rq6.EXPECTED_COLUMNS}
    row.update(ts="t", commit="c", exp=exp, backend=backend, n=n, f=1, clients=256, block_size=100,
               workload="a", rtt_ms=0, fault=fault, rep=1, commit_mode="sync", tps=700, lat_mean_ms=1,
               lat_p50_ms=1, lat_p99_ms=1, errors=0, wrong_gets=wrong, offered_rps=rate)
    p = raw / (exp + ".csv")
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=rq6.EXPECTED_COLUMNS, lineterminator="\n")
        w.writeheader()
        w.writerow(row)
    tag = rq6.run_tag(row) + "_bf100ms_rate%d" % rate
    d = raw / exp
    rej, fo, sent, cto = ops
    hdr, extra = OPS_HEADER, ""
    if first_fo is not None:
        hdr, extra = OPS_HEADER + ",first_failover_unix_ms", ",%d" % first_fo
    if ev_failed is not None:
        hdr, extra = hdr + ",evidence_failed", extra + ",%d" % ev_failed
    (d / (tag + ".ops.csv")).write_text(
        "%s\nread,1,1.00,1.000,1.000,2.000,0,0,0,900,30,0,0,0,%d,%d,%d,%d%s\n"
        "write,1,1.00,1.000,1.000,123.000,0,0,0,900,30,0,0,0,0,0,0,0%s\n" % (hdr, rej, fo, sent, cto, extra, extra))
    if fault != "none":
        (d / (tag + ".faults")).write_text(
            FAULTS_HEADER + "\n1970-01-01T00:00:00Z,0,0,0,%s,run_phase_start,,\n"
            "1970-01-01T00:01:00Z,60000,60,70,%s,%s,node4,\n" % (fault, fault, inject_action))
    (d / (tag + ".evict")).write_text("".join(
        "%s %s  | %s 2026/11/09 core: evicted node %d at height %d (%s)\n" % (rep, rep, _ts(sec), cul, h, why)
        for rep, sec, cul, h, why in evictions))
    (d / (tag + ".certs")).write_text("".join(
        "node1 node1  | %s core: certified height %d root %s\n" % (_ts(sec), h, root) for sec, h, root in certs))
    return p, raw


ALL_EVICT_NODE4 = (("node1", 60.8, 4, 812, "bad_read"), ("node2", 60.9, 4, 812, "bad_read"),
                   ("node3", 61.0, 4, 812, "bad_read"), ("node4", 60.85, 4, 812, "bad_read"))


def test_detect_and_evict_times(tmp_path):
    p, raw = _run(tmp_path, evictions=ALL_EVICT_NODE4)
    r = rq6.analyse([p], raw).runs[0]
    assert r["detect_evidence_s"] == pytest.approx(0.8, abs=1e-3)
    assert r["evict_all_s"] == pytest.approx(1.0, abs=1e-3)
    assert r["same_height"] is True
    assert r["honest_evicted"] == 0 and r["wrong_root_heights"] == 0
    assert (r["rejected_reads"], r["evidence_sent"], r["cert_timeouts"]) == (3, 2, 0)
    assert r["fault_kind"] == "F6" and r["failover_s"] == ""


def test_not_every_honest_node_evicted(tmp_path):
    p, raw = _run(tmp_path, evictions=ALL_EVICT_NODE4[:2])
    r = rq6.analyse([p], raw).runs[0]
    assert r["detect_evidence_s"] == pytest.approx(0.8, abs=1e-3)
    assert r["evict_all_s"] == ""


def test_different_heights_flagged(tmp_path):
    ev = ALL_EVICT_NODE4[:2] + (("node3", 61.0, 4, 813, "bad_read"),)
    p, raw = _run(tmp_path, evictions=ev)
    assert rq6.analyse([p], raw).runs[0]["same_height"] is False


def test_honest_evicted_counted(tmp_path):
    # F5-F4 at quorum f+1: the wrong root was certified first, so the honest
    # node1 and node2 were evicted for divergent_root (twice at height 40).
    ev = (("node3", 61.0, 1, 40, "divergent_root"), ("node3", 61.0, 2, 40, "divergent_root"),
          ("node4", 61.1, 1, 40, "divergent_root"), ("node4", 61.1, 2, 41, "divergent_root"))
    p, raw = _run(tmp_path, exp="e12-qf1-certified", fault="F5-F4:node4+node3@60", inject_action="corrupt_on",
                  evictions=ev, certs=((61.0, 40, "ab"), (61.1, 41, "cd")))
    r = rq6.analyse([p], raw).runs[0]
    assert r["honest_evicted"] == 2
    assert r["wrong_root_heights"] == 2
    assert r["certs_heights"] == 2
    assert r["evict_all_s"] == ""  # the culprits node3/node4 were never evicted


def test_no_evict_line(tmp_path):
    p, raw = _run(tmp_path, exp="e11-F6-plain", evictions=())
    res = rq6.analyse([p], raw)
    r = res.runs[0]
    assert r["detect_evidence_s"] == "" and r["evict_all_s"] == "" and r["same_height"] == ""
    assert any("no .evict line" in n for n in res.notes)


def test_fault_none(tmp_path):
    p, raw = _run(tmp_path, exp="e10-certified", fault="none", ops=(0, 0, 0, 0))
    res = rq6.analyse([p], raw)
    r = res.runs[0]
    assert r["fault_kind"] == "none" and r["detect_evidence_s"] == "" and r["rejected_reads"] == 0
    assert not res.notes


def test_phase2_ops_fall_back_to_proof_errors(tmp_path):
    p, raw = _run(tmp_path, exp="e10-plain", fault="none")
    ops = next((raw / "e10-plain").glob("*.ops.csv"))
    ops.write_text("op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted,dropped,read_bytes_mean,"
                   "verify_cpu_us_mean,proof_errors,cert_errors,dataloss_errors\n"
                   "read,1,1,1,1,1,0,0,0,9,3,2,1,4\nwrite,1,1,1,1,1,0,0,0,9,3,0,0,0\n")
    r = rq6.analyse([p], raw).runs[0]
    assert r["rejected_reads"] == 7 and r["evidence_sent"] == ""


def test_summary_and_main(tmp_path, capsys):
    p, raw = _run(tmp_path, evictions=ALL_EVICT_NODE4, wrong=0)
    out = tmp_path / "out"
    assert rq6.main([str(p), "--raw-dir", str(raw), "--out", str(out)]) == 0
    runs = (out / "rq6_runs.csv").read_text().splitlines()
    assert runs[0].startswith("exp,backend,n,fault,rep,fault_kind,injection_race,detect_evidence_s,evict_all_s,")
    summ = (out / "rq6_summary.csv").read_text().splitlines()
    assert len(summ) == 2 and summ[1].startswith("e11-F6-cert-fo-sign,raft,F6,1,0,1,1,0.8,1.0,")
    assert "e11-F6-cert-fo-sign" in capsys.readouterr().out


def test_bad_header_is_an_input_error(tmp_path, capsys):
    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n")
    assert rq6.main([str(p), "--raw-dir", str(tmp_path), "--out", str(tmp_path / "o")]) == 2
    assert "unexpected CSV header" in capsys.readouterr().err


def test_run_tag_matches_plot():
    plot = pytest.importorskip("plot")
    row = {"backend": "raft", "n": 4, "f": 1, "clients": 256, "block_size": 100, "workload": "a",
           "rtt_ms": 0, "fault": "F5-F4:node4+node3@60", "commit_mode": "sync", "rep": 2, "offered_rps": 800}
    assert rq6.run_tag(row) == plot.run_tag(row)
    assert rq6.rate_suffix(row) == plot.rate_suffix(row)


def test_failover_s_from_first_failover(tmp_path):
    p, raw = _run(tmp_path, evictions=ALL_EVICT_NODE4, first_fo=61250)
    assert rq6.analyse([p], raw).runs[0]["failover_s"] == pytest.approx(1.25, abs=1e-3)


def test_failover_before_injection_noted(tmp_path):
    p, raw = _run(tmp_path, evictions=ALL_EVICT_NODE4, first_fo=30000)
    res = rq6.analyse([p], raw)
    assert res.runs[0]["failover_s"] == ""
    assert any("before the injection" in n for n in res.notes)


def test_no_failover_no_value(tmp_path):
    p, raw = _run(tmp_path, ops=(0, 0, 0, 0), first_fo=0)
    res = rq6.analyse([p], raw)
    assert res.runs[0]["failover_s"] == ""
    assert not any("failover" in n for n in res.notes)


def test_old_sidecar_failover_noted(tmp_path):
    p, raw = _run(tmp_path)  # failovers=4, no first_failover_unix_ms column
    res = rq6.analyse([p], raw)
    assert res.runs[0]["failover_s"] == ""
    assert any("failover_s not recorded" in n for n in res.notes)


def test_evidence_failed_column(tmp_path):
    p, raw = _run(tmp_path, evictions=ALL_EVICT_NODE4, first_fo=61000, ev_failed=3)
    res = rq6.analyse([p], raw)
    assert res.runs[0]["evidence_failed"] == 3
    assert res.summary[0]["evidence_failed"] == 3


def test_evidence_failed_missing_column_empty(tmp_path):
    p, raw = _run(tmp_path, evictions=ALL_EVICT_NODE4)
    assert rq6.analyse([p], raw).runs[0]["evidence_failed"] == ""


def test_f5_partial_eviction_at_2f1_is_an_injection_race(tmp_path):
    ev = (("node1", 60.8, 3, 900, "divergent_root"), ("node2", 60.8, 3, 900, "divergent_root"))
    p, raw = _run(tmp_path, exp="e12-q2f1-certified", fault="F5-F4:node4+node3@60", inject_action="corrupt_on",
                  evictions=ev)
    res = rq6.analyse([p], raw)
    assert res.runs[0]["injection_race"] == 1
    assert res.summary[0]["runs"] == 0 and res.summary[0]["race_runs"] == 1
    assert any("injection race" in n for n in res.notes)


def test_f5_all_faulty_evicted_at_2f1_warns(tmp_path):
    ev = (("node1", 60.8, 3, 900, "divergent_root"), ("node1", 60.9, 4, 901, "divergent_root"))
    p, raw = _run(tmp_path, exp="e12-q2f1-certified", fault="F5-F4:node4+node3@60", inject_action="corrupt_on",
                  evictions=ev)
    res = rq6.analyse([p], raw)
    assert res.runs[0]["injection_race"] == 0 and res.summary[0]["runs"] == 1
    assert any(n.startswith("WARNING") for n in res.notes)


def test_f5_at_qf1_is_never_a_race(tmp_path):
    ev = (("node1", 60.8, 3, 900, "divergent_root"),)
    p, raw = _run(tmp_path, exp="e12-qf1-certified", fault="F5-F4:node4+node3@60", inject_action="corrupt_on",
                  evictions=ev)
    res = rq6.analyse([p], raw)
    assert res.runs[0]["injection_race"] == 0 and res.summary[0]["race_runs"] == 0
