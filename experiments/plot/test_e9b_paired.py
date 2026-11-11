import csv
import json

import e9b_paired

RUN_HEADER = ["exp", "backend", "attest_every", "read_mode", "fault", "rep", "fault_kind",
              "write_p99_ms", "write_p50_ms", "write_mean_ms", "read_p50_ms"]


def _setup(tmp_path, runs):
    """runs: (exp, backend, attest, read, rep, p50, p99, queue_ms per node or None)."""
    raw = tmp_path / "raw"
    rows = []
    for exp, b, k, rd, rep, p50, p99, q in runs:
        rows.append(dict(exp=exp, backend=b, attest_every=k, read_mode=rd, fault="none", rep=rep, fault_kind="none",
                         write_p99_ms=p99, write_p50_ms=p50, write_mean_ms=p50 + 10, read_p50_ms=1.1))
        if q is None:
            continue
        d = raw / exp / ("%s_n4_f1_c256_b100_wa_rtt0_none_sync_r%d_bf100ms_rate480.breakdown" % (b, rep))
        d.mkdir(parents=True)
        for n, qms in enumerate(q, 1):
            (d / ("breakdown-node%d.snap1.json" % n)).write_text(json.dumps({"queue_ns": 0, "queue_blocks": 10}))
            (d / ("breakdown-node%d.snap2.json" % n)).write_text(json.dumps({"queue_ns": int(qms * 1e6 * 100), "queue_blocks": 110}))
    p = tmp_path / "rq5_runs.csv"
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_HEADER, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    return str(p), str(raw)


def test_queue_block_and_paired_differences(tmp_path):
    runs = []
    for rep, (p0, p1) in enumerate([(80, 90), (85, 80), (82, 95)], 1):
        runs.append(("e9b-a0-plain", "raft", 0, "plain", rep, p0, 500, (2, 2, 2, 2)))
        runs.append(("e9b-a1-plain", "raft", 1, "plain", rep, p1, 600, (3, 3, 3, 3)))
    path, raw = _setup(tmp_path, runs)
    rs, notes = e9b_paired.load_runs(path, raw)
    assert notes == []
    assert {round(r["queue_block_ms"], 6) for r in rs} == {2.0, 3.0}
    pr, dcols = e9b_paired.paired(rs)
    assert dcols == ["d_rep1", "d_rep2", "d_rep3"]
    got = {(r["metric"], r["config"]): r for r in pr}
    w = got[("write_p50_ms", "1-plain")]
    assert (w["baseline"], w["d_rep1"], w["d_rep2"], w["d_rep3"]) == ("0-plain", 10, -5, 13)
    assert (w["median"], w["positive"], w["reps"]) == (10, 2, 3)
    assert got[("queue_block_ms", "1-plain")]["median"] == 1.0
    # no 10-plain / certified runs: those pairs are absent
    assert ("write_p50_ms", "10-plain") not in got


def test_missing_breakdown_is_a_note_and_main_writes(tmp_path):
    path, raw = _setup(tmp_path, [("e9b-a0-plain", "bft2", 0, "plain", 1, 95, 700, None),
                                  ("e9b-a1-plain", "bft2", 1, "plain", 1, 99, 720, (1, 1, 1, 1))])
    out = tmp_path / "out"
    assert e9b_paired.main([path, "--raw-dir", raw, "--out", str(out)]) == 0
    runs = (out / "e9b_runs.csv").read_text().splitlines()
    assert runs[0] == ",".join(e9b_paired.RUN_COLUMNS)
    assert runs[1].startswith("e9b-a0-plain,bft2,0-plain,1,95.0,")
    assert runs[1].endswith(",")  # queue_block_ms empty
    paired = list(csv.DictReader(open(out / "e9b_paired.csv")))
    q = [r for r in paired if r["metric"] == "queue_block_ms"]
    assert q == []  # baseline has no queue figure
    p99 = [r for r in paired if r["metric"] == "write_p99_ms"][0]
    assert (p99["d_rep1"], p99["positive"]) == ("20.0", "1")
