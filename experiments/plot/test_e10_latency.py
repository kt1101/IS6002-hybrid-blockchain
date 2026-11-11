import csv

import e10_latency
import rq6

OPS_HEADER = "op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors"


def _rows(tmp_path, runs):
    """runs: (exp, backend, workload, rep, read (p50, p99) or None, write (p50, p99) or None)."""
    raw = tmp_path / "raw"
    by_exp = {}
    for exp, backend, wl, rep, rd, wr in runs:
        (raw / exp).mkdir(parents=True, exist_ok=True)
        row = {c: "" for c in rq6.EXPECTED_COLUMNS}
        row.update(ts="t", commit="c", exp=exp, backend=backend, n=4, f=1, clients=256, block_size=100,
                   workload=wl, rtt_ms=0, fault="none", rep=rep, commit_mode="sync", tps=480, lat_mean_ms=1,
                   lat_p50_ms=1, lat_p99_ms=1, errors=0, wrong_gets=0, offered_rps=480)
        by_exp.setdefault(exp, []).append(row)
        lines = [OPS_HEADER]
        for op, v in (("read", rd), ("write", wr)):
            if v is None:
                lines.append("%s,0,0,0,0,0,0" % op)
            else:
                lines.append("%s,100,240,%s,%s,%s,0" % (op, v[0] + 1, v[0], v[1]))
        tag = rq6.run_tag(row) + "_bf100ms" + rq6.rate_suffix(row)
        (raw / exp / (tag + ".ops.csv")).write_text("\n".join(lines) + "\n")
    paths = []
    for exp, rows in by_exp.items():
        p = raw / (exp + ".csv")
        with open(p, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=rq6.EXPECTED_COLUMNS, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        paths.append(str(p))
    return paths, str(raw)


def test_median_and_range_per_mode_workload_op(tmp_path):
    paths, raw = _rows(tmp_path, [
        ("e10-certified", "raft", "a", 1, (90, 400), (80, 350)),
        ("e10-certified", "raft", "a", 2, (100, 450), (85, 360)),
        ("e10-certified", "raft", "a", 3, (95, 420), (82, 380)),
        ("e10-plain", "raft", "c", 1, (1.4, 7), None),
    ])
    rows, notes = e10_latency.analyse(paths, raw)
    assert notes == []
    got = {(r["read_mode"], r["workload"], r["op"]): r for r in rows}
    assert set(got) == {("certified", "a", "read"), ("certified", "a", "write"), ("plain", "c", "read")}
    r = got[("certified", "a", "read")]
    assert (r["reps"], r["p50_ms"], r["p50_min_ms"], r["p50_max_ms"]) == (3, 95, 90, 100)
    assert (r["p99_ms"], r["p99_min_ms"], r["p99_max_ms"], r["mean_ms"]) == (420, 400, 450, 96)
    assert got[("certified", "a", "write")]["p99_ms"] == 360
    # workload c has no writes (count 0): no write group
    assert got[("plain", "c", "read")]["reps"] == 1


def test_missing_sidecar_and_other_exp_are_notes(tmp_path):
    paths, raw = _rows(tmp_path, [("e10-quorum", "bft2", "a", 1, (2, 60), (100, 500)),
                                  ("e11-F4-plain", "bft2", "a", 1, (2, 60), (100, 500))])
    for p in (tmp_path / "raw" / "e10-quorum").glob("*.ops.csv"):
        p.unlink()
    rows, notes = e10_latency.analyse(paths, raw)
    assert rows == []
    assert any("no .ops.csv sidecar" in n for n in notes)
    assert any("not an E10 EXP" in n for n in notes)


def test_main_writes_csv(tmp_path):
    paths, raw = _rows(tmp_path, [("e10-plain", "raft", "a", 1, (1.5, 40), (90, 377))])
    out = tmp_path / "out"
    assert e10_latency.main(paths + ["--raw-dir", raw, "--out", str(out)]) == 0
    lines = (out / "e10_latency.csv").read_text().splitlines()
    assert lines[0] == ",".join(e10_latency.COLUMNS)
    assert lines[1].startswith("raft,plain,a,read,1,1.5,")
    assert len(lines) == 3


def test_read_mode_of():
    assert e10_latency.read_mode_of("e10-certified-signall") == "certified-signall"
    assert e10_latency.read_mode_of("e9-a1-plain") is None
    assert e10_latency.read_mode_of("e10-") is None
