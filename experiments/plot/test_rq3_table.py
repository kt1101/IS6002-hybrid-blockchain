"""Tests for experiments/plot/rq3_table.py (pure Python, no pandas needed).

Run: python3 -m pytest experiments/plot -q
Never writes into the repo: everything goes to tmp_path.
"""
import csv

import pytest

import rq3
import rq3_table

FAULTS_HEADER = "utc,epoch_ms,t_s,timeline_s,fault,action,node,detail"
# WARMUP 10 s, fault at timeline second 30.2 (F1 on node4 of a 4-node cluster).
F1_ROWS = [
    "u,1000000000,-10.000,0.000,F1:node4@20,run_phase_start,,ycsbbench run phase",
    "u,1000030200,20.200,30.200,F1:node4@20,docker_stop,node4,containers node4=abc",
    "u,1000031100,21.100,31.100,F1:node4@20,stopped,node4,docker stop returned",
]
TAG = "raft_n4_f1_c256_b100_wa_rtt0_F1_node4_20_sync_r%d"


def write_run(d, tag, timeline, rows=F1_ROWS, audit=None, wtimeline=None):
    """wtimeline: None (no file), a list of numbers, or a str written as is."""
    d.mkdir(parents=True, exist_ok=True)
    (d / (tag + ".timeline")).write_text("".join("%s\n" % v for v in timeline))
    if wtimeline is not None:
        (d / (tag + ".wtimeline")).write_text(
            wtimeline if isinstance(wtimeline, str) else "".join("%s\n" % v for v in wtimeline))
    if rows is not None:
        (d / (tag + ".faults")).write_text(FAULTS_HEADER + "\n" + "".join(r + "\n" for r in rows))
    if audit is not None:
        (d / (tag + ".audit")).write_text("".join("%s,%s\n" % r for r in audit))


def read_table(out):
    with open(out / "rq3_summary.csv", newline="") as fh:
        return list(csv.DictReader(fh))


def run_main(capsys, argv):
    rc = rq3_table.main(argv)
    cap = capsys.readouterr()
    return rc, cap.out, cap.err


def test_parse_tag():
    assert rq3_table.parse_tag(TAG % 2) == ("raft", 4, 1, 2)
    assert rq3_table.parse_tag("bft2_n7_f2_c256_b100_wa_rtt0_F5-F4_node7_node6_node5_60_sync_r3_bf100ms") == ("bft2", 7, 2, 3)
    with pytest.raises(rq3.InputError):
        rq3_table.parse_tag("junk")


def test_metrics_cover_every_rq3_key(tmp_path):
    write_run(tmp_path, "r", [10] * 40, audit=[(1000031.0, "")])
    m = rq3.load_run(tmp_path / "r.timeline", tmp_path / "r.faults", tmp_path / "r.audit", n=4)
    classified = set(rq3_table.METRICS) | set(rq3_table.STATUS_COLS) | {"fault", "n", "warnings"}
    assert set(m) == classified  # a new rq3 metric must be classified here
    assert set(rq3_table.NULLABLE) <= set(rq3_table.METRICS)


def test_table_aggregates_two_reps(tmp_path, capsys):
    d = tmp_path / "e6"
    # rep 1: pre-fault mean 10, 3 s stall
    write_run(d, TAG % 1, [10] * 31 + [0, 0, 0, 10, 10, 10] + [10] * 3)
    # rep 2: pre-fault mean 20, stall of 1 s
    write_run(d, TAG % 2, [20] * 31 + [0, 20, 20, 20] + [20] * 5)
    out = tmp_path / "figs"
    rc, stdout, err = run_main(capsys, [str(d), "--out", str(out)])
    assert rc == 0 and err == ""
    assert stdout.splitlines()[-1] == "skipped 0"
    rows = read_table(out)
    assert len(rows) == 1
    r = rows[0]
    assert (r["backend"], r["n"], r["f"], r["fault"]) == ("raft", "4", "1", "F1:node4@20")
    assert r["reps"] == "2"
    assert float(r["pre_mean_tps_mean"]) == pytest.approx(15.0)
    # sample std of 10 and 20 (ddof=1)
    assert float(r["pre_mean_tps_std"]) == pytest.approx(7.071, abs=1e-3)
    assert float(r["stall_s_mean"]) == pytest.approx(2.0)
    assert float(r["stall_s_std"]) == pytest.approx(1.414, abs=1e-3)
    assert r["recovery_status"] == "recovered:2"
    assert r["detect_status"] == "no_audit:2"
    assert r["timeline_source"] == "total:2"
    assert r["detect_s_mean"] == "" and r["detect_s_n"] == "0"
    assert r["recovery_s_n"] == "2"
    assert list(rows[0]) == rq3_table.header()


def test_single_rep_has_empty_std(tmp_path, capsys):
    d = tmp_path / "e6"
    write_run(d, TAG % 1, [10] * 40)
    rc, _, _ = run_main(capsys, [str(d), "--out", str(tmp_path / "o")])
    assert rc == 0
    r = read_table(tmp_path / "o")[0]
    assert r["reps"] == "1" and r["pre_mean_tps_std"] == "" and r["pre_mean_tps_mean"] == "10.0"


def test_separate_rows_per_fault_variant_and_backend(tmp_path, capsys):
    d = tmp_path / "e6"
    write_run(d, TAG % 1, [10] * 40)
    leader = [row.replace("node4", "node1") for row in F1_ROWS]
    write_run(d, "raft_n4_f1_c256_b100_wa_rtt0_F1_node1_20_sync_r1", [10] * 40, leader)
    write_run(d, "kafka_n4_f1_c256_b100_wa_rtt0_F1_node4_20_sync_r1", [10] * 40)
    rc, _, _ = run_main(capsys, [str(d), "--out", str(tmp_path / "o")])
    assert rc == 0
    keys = [(r["backend"], r["fault"]) for r in read_table(tmp_path / "o")]
    assert keys == [("kafka", "F1:node4@20"), ("raft", "F1:node1@20"), ("raft", "F1:node4@20")]


def test_skips_unusable_runs_without_aborting(tmp_path, capsys):
    d = tmp_path / "e6"
    write_run(d, TAG % 1, [10] * 40)
    write_run(d, TAG % 2, [10] * 40, rows=None)                      # no .faults file
    write_run(d, "raft_n4_f1_c256_b100_wa_rtt0_F1_node4_20_sync_r3", [10] * 40,
              rows=["u,1000000000,-10.000,0.000,F1:node4@20,run_phase_start,,x"])  # no injection row
    write_run(d, "oddname", [10] * 40)                                # not a run tag
    out = tmp_path / "o"
    rc, stdout, err = run_main(capsys, [str(d), "--out", str(out)])
    assert rc == 0
    assert len(err.strip().splitlines()) == 3
    assert "_r2: skipped" in err and "_r3: skipped" in err and "oddname: skipped" in err
    assert stdout.splitlines()[-1] == "skipped 3"
    rows = read_table(out)
    assert len(rows) == 1 and rows[0]["reps"] == "1"


def test_all_runs_unusable_exit_2_and_no_csv(tmp_path, capsys):
    d = tmp_path / "e6"
    write_run(d, TAG % 1, [10] * 40, rows=None)
    rc, stdout, err = run_main(capsys, [str(d), "--out", str(tmp_path / "o")])
    assert rc == 2
    assert "no usable run" in err
    assert stdout.splitlines()[-1] == "skipped 1"
    assert not (tmp_path / "o" / "rq3_summary.csv").exists()


def test_missing_directory_exit_2(tmp_path, capsys):
    rc, _, err = run_main(capsys, [str(tmp_path / "nope"), "--out", str(tmp_path / "o")])
    assert rc == 2 and "no such run directory" in err


def test_later_directory_replaces_same_tag(tmp_path, capsys):
    a, b = tmp_path / "e6", tmp_path / "e6-rerun"
    write_run(a, TAG % 1, [10] * 40)
    write_run(a, TAG % 2, [10] * 40)
    write_run(b, TAG % 2, [30] * 40)  # re-run of rep 2
    rc, stdout, _ = run_main(capsys, [str(a), str(b), "--out", str(tmp_path / "o")])
    assert rc == 0
    r = read_table(tmp_path / "o")[0]
    assert r["reps"] == "2" and float(r["pre_mean_tps_mean"]) == pytest.approx(20.0)


# Reads keep completing after the fault (blips in the total timeline) while no
# write succeeds any more: only the write timeline shows the loss of liveness.
TOTAL_WITH_BLIPS = [10] * 31 + [0] * 3 + [10, 4] + [0] * 5 + [10] + [0] * 5
WRITES_DEAD = [5] * 31 + [0] * (len(TOTAL_WITH_BLIPS) - 31)


def test_timeline_source_column_counts_write_and_total_runs(tmp_path, capsys):
    d = tmp_path / "e6"
    write_run(d, TAG % 1, TOTAL_WITH_BLIPS, wtimeline=WRITES_DEAD)
    write_run(d, TAG % 2, TOTAL_WITH_BLIPS, wtimeline=WRITES_DEAD)
    write_run(d, TAG % 3, TOTAL_WITH_BLIPS)                  # no .wtimeline: falls back
    write_run(d, TAG % 4, TOTAL_WITH_BLIPS, wtimeline="")    # empty .wtimeline: falls back
    rc, _, err = run_main(capsys, [str(d), "--out", str(tmp_path / "o")])
    assert rc == 0 and err == ""
    r = read_table(tmp_path / "o")[0]
    assert r["timeline_source"] == "total:2;write:2"
    assert r["recovery_status"] == "not_recovered:2;recovered:2"
    assert r["warned_reps"] == "1"  # only the unusable file warns


def test_write_timeline_gives_not_recovered_and_the_full_stall(tmp_path, capsys):
    d = tmp_path / "e6"
    write_run(d, TAG % 1, TOTAL_WITH_BLIPS, wtimeline=WRITES_DEAD)
    rc, _, _ = run_main(capsys, [str(d), "--out", str(tmp_path / "o")])
    assert rc == 0
    r = read_table(tmp_path / "o")[0]
    assert r["timeline_source"] == "write:1" and r["recovery_status"] == "not_recovered:1"
    assert float(r["stall_s_mean"]) == len(WRITES_DEAD) - 31
    assert float(r["pre_mean_tps_mean"]) == 5.0 and float(r["min_after_fault_tps_mean"]) == 0.0
