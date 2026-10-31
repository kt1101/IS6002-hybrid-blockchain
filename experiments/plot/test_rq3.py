"""Tests for experiments/plot/rq3.py (pure Python, no pandas needed).

Run: python3 -m pytest experiments/plot -q
Never writes into the repo: everything goes to tmp_path.
"""
import pytest

import rq3
from rq3 import detect_latency, recovery_time, stall_seconds


# ---- the three metrics ----------------------------------------------------

def test_recovery_time():
    assert recovery_time([10] * 60 + [0, 0, 5, 9, 10], 60, 10) == 3


def test_never_recovers():
    assert recovery_time([10] * 60 + [0] * 10, 60, 10) is None


def test_stall_seconds():
    assert stall_seconds([10] * 60 + [0, 0, 0, 4], 60) == 3


def test_detect_latency():
    assert detect_latency([(61, ""), (62, "node4")], 60) == 2


def test_recovery_immediate_is_zero():
    # F3-like fault: throughput never drops below 90 %.
    assert recovery_time([10] * 60 + [9, 10, 10], 60, 10) == 0


def test_recovery_threshold_is_90_percent():
    assert recovery_time([10] * 5 + [8, 8.9, 9.0], 5, 10) == 2
    assert recovery_time([10] * 5 + [8, 8.9, 8.99], 5, 10) is None


def test_recovery_ignores_samples_before_the_fault():
    assert recovery_time([10] * 60 + [0, 0], 60, 10) is None


def test_recovery_without_baseline_is_none():
    assert recovery_time([0] * 70, 60, 0) is None
    assert recovery_time([5] * 70, 60, None) is None


def test_recovery_fractional_fault_starts_at_first_full_second():
    # Fault at 60.4: second 60 is partly before it and is skipped.
    assert recovery_time([10] * 60 + [10, 0, 10], 60.4, 10) == pytest.approx(1.6)


def test_recovery_fault_past_the_end_is_none():
    assert recovery_time([10] * 10, 20, 10) is None


def test_stall_zero_when_traffic_continues():
    assert stall_seconds([10] * 60 + [3, 0, 0], 60) == 0


def test_stall_runs_to_the_end():
    assert stall_seconds([10] * 60 + [0] * 5, 60) == 5


def test_stall_counts_only_the_first_run():
    assert stall_seconds([10] * 60 + [0, 0, 7, 0, 0, 0], 60) == 2


def test_stall_fractional_fault():
    assert stall_seconds([10] * 60 + [0, 0, 4], 59.5) == 2  # second 59 is mixed
    assert stall_seconds([10] * 60 + [0, 0, 4], 60.2) == 1


def test_detect_latency_none_when_never_divergent():
    assert detect_latency([(61, ""), (62, "")], 60) is None
    assert detect_latency([], 60) is None


def test_detect_latency_ignores_rows_before_the_fault():
    assert detect_latency([(50, "node4"), (61, ""), (64.5, "node3")], 60) == 4.5


def test_detect_latency_does_not_count_unreachable_nodes():
    rows = [(61, "down:node4"), (62, "down:node3+down:node4"), (63, "node2+down:node4")]
    assert detect_latency(rows, 60) == 3


def test_detect_latency_row_at_the_fault_counts():
    assert detect_latency([(60, "node4")], 60) == 0


def test_node_tokens():
    assert rq3.divergent_nodes("node2+down:node4+node3") == ["node2", "node3"]
    assert rq3.unreachable_nodes("node2+down:node4+down:node3") == ["node4", "node3"]
    assert rq3.divergent_nodes("") == [] and rq3.unreachable_nodes("") == []


# ---- the loader -----------------------------------------------------------

FAULTS_HEADER = "utc,epoch_ms,t_s,timeline_s,fault,action,node,detail"


def write_run(tmp_path, timeline, faults_rows, audit_rows=None):
    tl = tmp_path / "r.timeline"
    tl.write_text("".join("%s\n" % v for v in timeline))
    fl = tmp_path / "r.faults"
    fl.write_text(FAULTS_HEADER + "\n" + "".join(r + "\n" for r in faults_rows))
    au = None
    if audit_rows is not None:
        au = tmp_path / "r.audit"
        au.write_text("".join("%s,%s\n" % r for r in audit_rows))
    return tl, fl, au


# WARMUP 10 s, AT 20 s: fault at timeline second 30, run phase started at
# epoch 1_000_000 s, so the injection is at epoch 1_000_030 s.
F1_ROWS = [
    "2026-10-30T10:00:00Z,999999000,,,F1:node4@20,netem_rtt,node1-node4,egress delay",
    "2026-10-30T10:00:10Z,1000000000,-10.000,0.000,F1:node4@20,run_phase_start,,ycsbbench run phase",
    "2026-10-30T10:00:40Z,1000030200,20.200,30.200,F1:node4@20,docker_stop,node4,containers node4=abc",
    "2026-10-30T10:00:40Z,1000031100,21.100,31.100,F1:node4@20,stopped,node4,docker stop returned",
]


def test_load_run_metrics(tmp_path):
    # index 30 is the mixed second of the fault (30.2): not counted
    timeline = [5] * 10 + [10] * 20 + [0, 0, 0, 6, 9, 10, 10] + [10] * 3
    audit = [
        (1000000 + 28.5, ""),            # before the fault
        (1000000 + 31.0, "down:node4"),  # node stopped: unreachable, not divergent
        (1000000 + 33.25, "node3+down:node4"),
        (1000000 + 34.0, "node3+down:node4"),
    ]
    tl, fl, au = write_run(tmp_path, timeline, F1_ROWS, audit)
    m = rq3.load_run(tl, fl, au)
    assert m["fault"] == "F1:node4@20"
    assert m["fault_at_s"] == pytest.approx(30.2)
    assert m["warmup_s"] == 10
    assert m["pre_mean_tps"] == 10  # seconds 10..29: warmup excluded
    assert m["stall_s"] == 2  # timeline[31], [32]
    assert m["recovery_s"] == pytest.approx(34 - 30.2)  # first second >= 9 tps is index 34
    assert m["unreachable_s"] == pytest.approx(31.0 - 30.2)
    assert m["detect_s"] == pytest.approx(33.25 - 30.2)


def test_load_run_without_audit(tmp_path):
    tl, fl, _ = write_run(tmp_path, [10] * 40, F1_ROWS)
    m = rq3.load_run(tl, fl, None)
    assert m["detect_s"] is None and m["unreachable_s"] is None
    assert m["detect_status"] == "no_audit" and m["audit_ticks"] is None
    # throughput never fell: explicit "no dip", not a bogus recovery of ~0 s
    assert m["recovery_s"] is None and m["recovery_status"] == "no_dip"
    assert m["dip_start_s"] is None and m["stall_s"] == 0
    assert m["min_after_fault_tps"] == 10


def test_load_run_audit_never_divergent(tmp_path):
    tl, fl, au = write_run(tmp_path, [10] * 40, F1_ROWS, [(1000031.0, ""), (1000032.0, "")])
    m = rq3.load_run(tl, fl, au)
    assert m["detect_s"] is None
    # the auditor stopped 8 s before the end of the run: "undetected" is not proven
    assert m["detect_status"].startswith("invalid: the auditor stopped")
    assert m["audit_ticks"] == 2 and m["audit_last_s"] == pytest.approx(32 - 30.2)


def test_load_run_undetected_when_auditor_ran_to_the_end(tmp_path):
    ticks = [(1000000.0 + s, "") for s in range(31, 40)]
    tl, fl, au = write_run(tmp_path, [10] * 40, F1_ROWS, ticks)
    m = rq3.load_run(tl, fl, au)
    assert m["detect_s"] is None and m["detect_status"] == "undetected"
    assert m["audit_div_pre"] == 0


def test_detect_dead_auditor_has_no_post_fault_ticks(tmp_path):
    ticks = [(1000000.0 + s, "") for s in range(1, 20)]  # died before the fault
    tl, fl, au = write_run(tmp_path, [10] * 40, F1_ROWS, ticks)
    m = rq3.load_run(tl, fl, au)
    assert m["detect_s"] is None
    assert m["detect_status"].startswith("invalid: no audit tick after the fault")
    assert m["audit_ticks"] == 19 and m["audit_last_s"] < 0


def test_detect_one_tick_lag_blip_is_invalid(tmp_path):
    ticks = [(1000000.0 + 12, "node2"), (1000000.0 + 20, ""), (1000000.0 + 33, "node3"), (1000000.0 + 34, ""),
             (1000000.0 + 39, "")]
    tl, fl, au = write_run(tmp_path, [10] * 40, F1_ROWS, ticks)
    m = rq3.load_run(tl, fl, au)
    assert m["audit_div_pre"] == 1
    assert m["detect_s"] is None
    assert m["detect_status"].startswith("invalid: one-tick divergence")


def test_detect_persistent_divergence_is_valid_despite_pre_fault_blip(tmp_path):
    ticks = [(1000000.0 + 12, "node2"), (1000000.0 + 20, ""), (1000000.0 + 33, "node3"),
             (1000000.0 + 34, "node3+node1"), (1000000.0 + 39, "node3")]
    tl, fl, au = write_run(tmp_path, [10] * 40, F1_ROWS, ticks)
    m = rq3.load_run(tl, fl, au)
    assert m["audit_div_pre"] == 1
    assert m["detect_status"] == "detected"
    assert m["detect_s"] == pytest.approx(33 - 30.2)


def test_detect_pre_fault_divergence_not_counted_as_detection():
    rows = [(10, "node2"), (31, ""), (39, "")]
    s = rq3.audit_summary(rows, 30, 40)
    assert s["audit_div_pre"] == 1 and s["detect_s"] is None
    assert s["detect_status"] == "undetected"


def test_detect_unreachable_only_is_not_detected():
    rows = [(31, "down:node4"), (32, "down:node4"), (39, "down:node4")]
    s = rq3.audit_summary(rows, 30, 40)
    assert s["detect_status"] == "undetected" and s["detect_s"] is None


def test_clock_skew_warning(tmp_path):
    ok_ticks = [(1000000.5, ""), (1000031.0, "")]
    tl, fl, au = write_run(tmp_path, [10] * 40, F1_ROWS, ok_ticks)
    assert rq3.load_run(tl, fl, au)["warnings"] == ""
    skewed = [(1000000.0 + 9.0, ""), (1000031.0, "")]
    tl, fl, au = write_run(tmp_path, [10] * 40, F1_ROWS, skewed)
    assert "clock skew" in rq3.load_run(tl, fl, au)["warnings"]
    early = [(999999.0, "")]
    tl, fl, au = write_run(tmp_path, [10] * 40, F1_ROWS, early)
    assert "clock skew" in rq3.load_run(tl, fl, au)["warnings"]


# ---- dip-based recovery / stall ---------------------------------------------

def test_dip_start_index():
    tl = [10] * 60 + [10, 10, 4, 0, 9, 10]
    assert rq3.dip_start_index(tl, 60, 10) == 62
    assert rq3.dip_start_index([10] * 70, 60, 10) is None
    assert rq3.dip_start_index(tl, 60, None) is None


def test_recovery_after_dip_skips_the_lagging_prefix():
    tl = [10] * 60 + [10, 10, 4, 0, 9, 10]
    assert recovery_time(tl, 60, 10) == 0  # literal brief semantics
    assert recovery_time(tl, 60, 10, after_dip=True) == 4  # 9 tps at index 64
    assert recovery_time([10] * 70, 60, 10, after_dip=True) is None  # no dip
    assert recovery_time([10] * 60 + [0] * 5, 60, 10, after_dip=True) is None


def test_stall_from_first_zero():
    tl = [10] * 60 + [10, 10, 0, 0, 0, 7]
    assert stall_seconds(tl, 60) == 0
    assert stall_seconds(tl, 60, from_first_zero=True) == 3
    assert stall_seconds([10] * 70, 60, from_first_zero=True) == 0
    assert stall_seconds([10] * 62 + [0, 0, 5, 0], 60, from_first_zero=True) == 2


def test_load_run_lagging_effect(tmp_path):
    # fault at 30.2; throughput stays 10 until 33, then 0,0, then recovers
    timeline = [5] * 10 + [10] * 23 + [0, 0, 4, 9, 10, 10] + [10]
    tl, fl, _ = write_run(tmp_path, timeline, F1_ROWS)
    m = rq3.load_run(tl, fl, None)
    assert m["dip_start_s"] == pytest.approx(33 - 30.2)
    assert m["min_after_fault_tps"] == 0
    assert m["stall_s"] == 2
    assert m["recovery_s"] == pytest.approx(36 - 30.2)
    assert m["recovery_from_dip_s"] == pytest.approx(36 - 33)
    assert m["recovery_status"] == "recovered"


def test_load_run_never_recovers(tmp_path):
    timeline = [5] * 10 + [10] * 20 + [10, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    tl, fl, _ = write_run(tmp_path, timeline, F1_ROWS)
    m = rq3.load_run(tl, fl, None)
    assert m["recovery_s"] is None and m["recovery_status"] == "not_recovered"
    assert m["dip_start_s"] == pytest.approx(31 - 30.2)
    assert m["stall_s"] == 9


# ---- fault kinds ------------------------------------------------------------

def test_unknown_fault_kind_is_an_input_error(tmp_path):
    rows = [
        "u,1000000000,-10.000,0.000,F9:node4@20,run_phase_start,,x",
        "u,1000030200,20.200,30.200,F9:node4@20,f9_on,node4,d",
    ]
    tl, fl, _ = write_run(tmp_path, [10] * 40, rows)
    with pytest.raises(rq3.InputError, match="unknown fault kind F9"):
        rq3.load_run(tl, fl, None)


# Task 15: FaultPolicy faults are injected by SIGUSR1 (faults.sh fault_signal).
def test_load_run_f4_corrupt_on_is_the_injection(tmp_path):
    rows = [
        "u,1000000000,-10.000,0.000,F4:node4@20,run_phase_start,,x",
        "u,1000030200,20.200,30.200,F4:node4@20,corrupt_on,node4,SIGUSR1 to veritas (--fault=corrupt)",
        "u,1000030500,20.500,30.500,F4:node4@20,corrupt_applied,node4,kill returned",
    ]
    tl, fl, au = write_run(tmp_path, [10] * 40, rows, [(1000029.0, ""), (1000032.0, "node4"), (1000033.0, "node4")])
    m = rq3.load_run(tl, fl, au)
    assert m["fault_at_s"] == pytest.approx(30.2)
    assert m["detect_s"] == pytest.approx(32.0 - 30.2)


def test_load_run_kafka_f2_silent_on_is_the_injection(tmp_path):
    rows = [
        "u,1000000000,-10.000,0.000,F2:node4@20,run_phase_start,,x",
        "u,1000030300,20.300,30.300,F2:node4@20,silent_on,node4,SIGUSR1 to veritas (--fault=silent)",
        "u,1000030600,20.600,30.600,F2:node4@20,silent_applied,node4,kill returned",
    ]
    tl, fl, _ = write_run(tmp_path, [10] * 31 + [0] * 9, rows)
    m = rq3.load_run(tl, fl, None)
    assert m["fault_at_s"] == pytest.approx(30.3)
    assert m["recovery_s"] is None


@pytest.mark.parametrize("kind,action", [("F5-F4", "corrupt_on"), ("F5-F2", "iptables_drop")])
def test_load_run_f5_kinds(tmp_path, kind, action):
    spec = "%s:node3+node4@20" % kind
    rows = [
        "u,1000000000,-10.000,0.000,%s,run_phase_start,,x" % spec,
        "u,1000030300,20.300,30.300,%s,%s,node3,d" % (spec, action),
        "u,1000030700,20.700,30.700,%s,%s,node4,d" % (spec, action),
    ]
    tl, fl, _ = write_run(tmp_path, [10] * 40, rows)
    assert rq3.load_run(tl, fl, None)["fault_at_s"] == pytest.approx(30.3)


def test_failed_signal_is_not_an_injection(tmp_path):
    rows = [
        "u,1000000000,-10.000,0.000,F4:node4@20,run_phase_start,,x",
        "u,1000030300,20.300,30.300,F4:node4@20,corrupt_on_failed,node4,d",
    ]
    tl, fl, _ = write_run(tmp_path, [10] * 40, rows)
    with pytest.raises(rq3.InputError, match="injection"):
        rq3.load_run(tl, fl, None)


def test_load_run_uses_first_injection_of_a_multi_node_fault(tmp_path):
    rows = [
        "u,1000000000,-10.000,0.000,F3:node1+node2@20,run_phase_start,,x",
        "u,1000030300,20.300,30.300,F3:node1+node2@20,netem_delay,node1,d",
        "u,1000030700,20.700,30.700,F3:node1+node2@20,netem_delay,node2,d",
    ]
    tl, fl, _ = write_run(tmp_path, [10] * 40, rows)
    assert rq3.load_run(tl, fl, None)["fault_at_s"] == pytest.approx(30.3)


def test_load_run_fault_at_zero_has_no_baseline(tmp_path):
    rows = [
        "u,1000000000,-10.000,0.000,F1:node4@0,run_phase_start,,x",
        "u,1000010100,0.100,10.100,F1:node4@0,docker_stop,node4,d",
    ]
    tl, fl, _ = write_run(tmp_path, [10] * 30, rows)
    m = rq3.load_run(tl, fl, None)
    assert m["pre_mean_tps"] is None and m["recovery_s"] is None


def test_load_run_without_run_phase_row_aligns_on_injection_row(tmp_path):
    rows = [F1_ROWS[2]]
    tl, fl, au = write_run(tmp_path, [10] * 40, rows, [(1000033.0, "node3")])
    assert rq3.load_run(tl, fl, au)["detect_s"] == pytest.approx(33.0 - 30.2)


# ---- input errors -----------------------------------------------------------

def test_no_injection_row_is_an_error(tmp_path):
    tl, fl, _ = write_run(tmp_path, [10] * 40, [F1_ROWS[0], F1_ROWS[1]])
    with pytest.raises(rq3.InputError, match="injection"):
        rq3.load_run(tl, fl, None)


def test_failed_injection_is_not_an_injection(tmp_path):
    rows = [F1_ROWS[1], "u,1000030300,20.300,30.300,F1:node4@20,docker_stop_failed,node4,d"]
    tl, fl, _ = write_run(tmp_path, [10] * 40, rows)
    with pytest.raises(rq3.InputError, match="injection"):
        rq3.load_run(tl, fl, None)


def test_missing_file(tmp_path):
    with pytest.raises(rq3.InputError, match="no such file"):
        rq3.read_timeline(tmp_path / "nope.timeline")


def test_bad_timeline_line(tmp_path):
    p = tmp_path / "t"
    p.write_text("1\nabc\n")
    with pytest.raises(rq3.InputError, match=":2:"):
        rq3.read_timeline(p)


def test_bad_faults_header(tmp_path):
    p = tmp_path / "f"
    p.write_text("a,b,c\n")
    with pytest.raises(rq3.InputError, match="header"):
        rq3.read_faults(p)


def test_bad_audit_line(tmp_path):
    p = tmp_path / "a"
    p.write_text("1.000,\nnot-a-line\n")
    with pytest.raises(rq3.InputError, match=":2:"):
        rq3.read_audit(p)
    p.write_text("x,node1\n")
    with pytest.raises(rq3.InputError, match=":1:"):
        rq3.read_audit(p)


def test_read_audit_empty_file_and_blank_lines(tmp_path):
    p = tmp_path / "a"
    p.write_text("")
    assert rq3.read_audit(p) == []
    p.write_text("1.500,\n\n2.500,node3+down:node4\n")
    assert rq3.read_audit(p) == [(1.5, ""), (2.5, "node3+down:node4")]


def test_main_prints_metrics_and_rejects_bad_input(tmp_path, capsys):
    tl, fl, au = write_run(tmp_path, [10] * 40, F1_ROWS, [(1000033.0, "node3")])
    assert rq3.main([str(tmp_path / "r")]) == 0
    out = capsys.readouterr().out
    assert "fault_at_s=30.2" in out and "detect_s=2.8" in out
    assert rq3.main([str(tmp_path / "missing")]) == 2
    assert "no such file" in capsys.readouterr().err
