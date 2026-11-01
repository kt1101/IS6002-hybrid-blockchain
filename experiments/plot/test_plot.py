"""Tests for experiments/plot/plot.py.

Run: python3 -m pytest experiments/plot -q
Never writes into the repo: everything goes to tmp_path.
"""
import math
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import pytest

import plot

FIXTURE = Path(__file__).parent / "fixtures" / "small.csv"
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


@pytest.fixture(scope="module")
def raw():
    return plot.load_csvs([FIXTURE])


@pytest.fixture(scope="module")
def agg(raw):
    return plot.aggregate(raw)


def pick(df, **cfg):
    """Exactly one row of an aggregated frame matching the given config."""
    m = pd.Series(True, index=df.index)
    for k, v in cfg.items():
        m &= df[k] == v
    rows = df[m]
    assert len(rows) == 1, "expected 1 row for %r, got %d" % (cfg, len(rows))
    return rows.iloc[0]


def flagged_clients(rr, exp="e1", backend="kafka-orig"):
    sub = rr[(rr["exp"] == exp) & (rr["backend"] == backend)]
    return set(sub["clients"])


def make_df(tps, saturated=None, **cfg):
    """Raw-format frame: one config, one row per rep."""
    saturated = saturated or [0] * len(tps)
    rows = []
    for i, (v, s) in enumerate(zip(tps, saturated), 1):
        row = dict(ts="2026-10-29T10:00:00Z", commit="abc", exp="e", backend="b",
                   n=4, f=0, clients=4, block_size=100, workload="a", rtt_ms=0,
                   fault="none", rep=i, commit_mode="async", tps=float(v),
                   lat_mean_ms=1.0, lat_p50_ms=0.5, lat_p99_ms=5.0, errors=0,
                   wrong_gets=0, host_cpu_max_pct=10.0, saturated=s,
                   net_bytes_per_block=float("nan"))
        row.update(cfg)
        rows.append(row)
    return pd.DataFrame(rows)


def test_aggregate_mean_std():
    # 3 reps with tps 90,100,110 -> mean 100, sample std (ddof=1) 10
    a = plot.aggregate(make_df([90, 100, 110]))
    assert len(a) == 1
    row = a.iloc[0]
    assert row["reps"] == 3
    assert row["tps_mean"] == pytest.approx(100.0)
    assert row["tps_std"] == pytest.approx(10.0)
    assert row["cv"] == pytest.approx(0.1)
    assert row["lat_p50_ms_mean"] == pytest.approx(0.5)
    assert row["lat_p99_ms_mean"] == pytest.approx(5.0)


def test_aggregate_mean_std_from_fixture(agg):
    row = pick(agg, exp="e1", backend="kafka-orig", clients=4)  # 95,100,105
    assert row["reps"] == 3
    assert row["tps_mean"] == pytest.approx(100.0)
    assert row["tps_std"] == pytest.approx(5.0)
    assert row["cv"] == pytest.approx(0.05)


def test_aggregate_groups_ignore_commit_and_rep(agg):
    # one rep carries "-dirty" in commit; it must not split the group
    e1_kafka = agg[(agg["exp"] == "e1") & (agg["backend"] == "kafka-orig")]
    assert sorted(e1_kafka["clients"]) == [4, 16, 64, 256]
    for col in ("commit", "ts", "rep"):
        assert col not in agg.columns
    assert len(agg) == 8


def test_aggregate_empty_net_bytes_is_nan(agg):
    row = pick(agg, exp="e1", backend="kafka-orig", clients=16)
    assert math.isnan(row["net_bytes_per_block_mean"])
    # one of two reps has a value; the empty one is skipped
    row = pick(agg, exp="e1", backend="kafka-orig", clients=64)
    assert row["net_bytes_per_block_mean"] == pytest.approx(1234.5)


def test_single_rep_std_is_nan_and_flagged(raw, tmp_path):
    one = raw[(raw["backend"] == "tm-orig") & (raw["clients"] == 4) & (raw["rep"] == 1)]
    assert len(one) == 1
    a = plot.aggregate(one)
    assert a.iloc[0]["reps"] == 1
    assert math.isnan(a.iloc[0]["tps_std"])
    assert math.isnan(a.iloc[0]["cv"])
    rr = plot.rerun_list(a, tmp_path / "rerun.txt")
    assert len(rr) == 1
    assert "reps" in rr.iloc[0]["reason"]


def test_rerun_flags_high_cv(agg, tmp_path):
    # tps 80,100,120 -> cv 0.2
    rr = plot.rerun_list(agg, tmp_path / "rerun.txt")
    assert 16 in flagged_clients(rr)
    row = pick(rr, exp="e1", backend="kafka-orig", clients=16)
    assert row["cv"] == pytest.approx(0.2)
    assert "cv" in row["reason"]


def test_rerun_flags_missing_reps(agg, tmp_path):
    # clients=64 has only 2 reps (and cv 0)
    rr = plot.rerun_list(agg, tmp_path / "rerun.txt")
    assert 64 in flagged_clients(rr)
    row = pick(rr, exp="e1", backend="kafka-orig", clients=64)
    assert row["reps"] == 2
    assert "reps" in row["reason"]


def test_rerun_does_not_flag_stable_configs(agg, tmp_path):
    rr = plot.rerun_list(agg, tmp_path / "rerun.txt")
    assert 256 in set(agg[agg["backend"] == "kafka-orig"]["clients"])
    assert 256 not in flagged_clients(rr)  # 3 reps, cv ~0.02
    assert len(rr[rr["backend"] == "tm-orig"]) == 0
    assert len(rr[rr["exp"] == "e2"]) == 0
    assert len(rr) == 2


def test_rerun_writes_file(agg, tmp_path):
    path = tmp_path / "sub" / "rerun.txt"
    plot.rerun_list(agg, path)
    lines = path.read_text().splitlines()
    assert len(lines) == 2
    assert any("clients=16 " in l and "cv>" in l for l in lines)
    assert any("clients=64 " in l and "reps<3" in l for l in lines)


def test_rerun_empty_file_when_all_ok(agg, tmp_path):
    ok = agg[agg["exp"] == "e2"]
    path = tmp_path / "rerun.txt"
    rr = plot.rerun_list(ok, path)
    assert len(rr) == 0
    assert path.read_text() == ""


def test_saturated_propagates_inline():
    a = plot.aggregate(make_df([100, 100, 100], saturated=[0, 1, 0]))
    assert a.iloc[0]["saturated"] == True  # noqa: E712
    a = plot.aggregate(make_df([100, 100, 100], saturated=[0, 0, 0]))
    assert a.iloc[0]["saturated"] == False  # noqa: E712


def test_saturated_unknown_is_nan_not_zero():
    # empty / non-numeric values are unknown, never silently 0
    a = plot.aggregate(make_df([100, 100, 100], saturated=[0, "", 0]))
    assert pd.isna(a.iloc[0]["saturated"])
    a = plot.aggregate(make_df([100, 100, 100], saturated=["x", "x", "x"]))
    assert pd.isna(a.iloc[0]["saturated"])
    # but one known-saturated rep still wins
    a = plot.aggregate(make_df([100, 100, 100], saturated=[1, "", 0]))
    assert a.iloc[0]["saturated"] == True  # noqa: E712


def test_saturated_propagates(agg):
    # clients=256: only rep 2 saturated -> whole config saturated
    assert pick(agg, exp="e1", backend="kafka-orig", clients=256)["saturated"] == True  # noqa: E712
    assert pick(agg, exp="e1", backend="kafka-orig", clients=4)["saturated"] == False  # noqa: E712
    assert sum(bool(v) for v in agg["saturated"] if pd.notna(v)) == 1


def series(ax):
    return [l for l in ax.get_lines() if l.get_gid() == "series"]


def legend_texts(ax):
    leg = ax.get_legend()
    return [t.get_text() for t in leg.get_texts()] if leg else []


def test_plot_exp_writes_png_and_style(agg, tmp_path):
    out = tmp_path / "figs" / "e1_tps_vs_clients.png"
    fig = plot.plot_exp(agg, "e1", "clients", out)
    try:
        data = out.read_bytes()
        assert data.startswith(PNG_MAGIC)
        assert len(data) > 1000
        ax = fig.axes[0]
        assert ax.get_yscale() == "log"
        # one line per backend (kafka-orig, tm-orig)
        lines = series(ax)
        assert len(lines) == 2
        assert sorted(l.get_label() for l in lines) == ["kafka-orig", "tm-orig"]
        # clients=256 of kafka-orig is saturated -> exactly one hollow marker
        hollow = [l for l in ax.get_lines() if l.get_gid() == "hollow"]
        assert len(hollow) == 1
        assert list(hollow[0].get_xdata()) == [256]
        assert hollow[0].get_markerfacecolor() == "white"
        # the other points are filled markers
        filled = [l for l in ax.get_lines() if l.get_gid() == "filled"]
        assert len(filled) == 2
        assert all(l.get_markerfacecolor() != "white" for l in filled)
        assert any("saturated" in t for t in legend_texts(ax))
    finally:
        plt.close(fig)


def test_plot_exp_n_sweep_is_one_line_per_backend(agg, tmp_path):
    # regression: f is a function of n (n=4,f=0; n=7,f=2); it must not split
    # the backend into one-point lines
    out = tmp_path / "e2_tps_vs_n.png"
    fig = plot.plot_exp(agg, "e2", "n", out)
    try:
        assert out.read_bytes().startswith(PNG_MAGIC)
        ax = fig.axes[0]
        assert ax.get_yscale() == "log"
        lines = series(ax)
        assert len(lines) == 1
        assert len(lines[0].get_xdata()) == 2
        assert lines[0].get_label() == "kafka-orig"
        # nothing saturated in e2: no hollow markers and no legend note
        assert not [l for l in ax.get_lines() if l.get_gid() == "hollow"]
        assert not any("saturated" in t for t in legend_texts(ax))
    finally:
        plt.close(fig)


def test_plot_exp_splits_on_column_varying_at_fixed_x(tmp_path):
    # rtt_ms genuinely varies at the same clients -> separate lines
    df = pd.concat([make_df([100, 100, 100], clients=c, rtt_ms=r)
                    for c in (4, 16) for r in (0, 50)], ignore_index=True)
    fig = plot.plot_exp(plot.aggregate(df), "e", "clients", tmp_path / "s.png")
    try:
        lines = series(fig.axes[0])
        assert len(lines) == 2
        assert all(len(l.get_xdata()) == 2 for l in lines)
    finally:
        plt.close(fig)


def test_plot_exp_unknown_saturation_is_not_hollow(tmp_path):
    df = pd.concat([make_df([100, 100, 100], saturated=[0, "", 0], clients=c)
                    for c in (4, 16)], ignore_index=True)
    fig = plot.plot_exp(plot.aggregate(df), "e", "clients", tmp_path / "u.png")
    try:
        ax = fig.axes[0]
        assert not [l for l in ax.get_lines() if l.get_gid() == "hollow"]
        assert not any("saturated" in t for t in legend_texts(ax))
    finally:
        plt.close(fig)


def test_plot_exp_unknown_exp_raises(agg, tmp_path):
    with pytest.raises(ValueError):
        plot.plot_exp(agg, "nope", "clients", tmp_path / "x.png")


def zero_tps_agg():
    df = pd.concat([make_df([0, 0, 0], clients=4),
                    make_df([100, 100, 100], clients=16),
                    make_df([200, 200, 200], clients=64)], ignore_index=True)
    return plot.aggregate(df)


def test_rerun_flags_zero_throughput(tmp_path):
    a = zero_tps_agg()
    path = tmp_path / "rerun.txt"
    rr = plot.rerun_list(a, path)
    assert list(rr["clients"]) == [4]
    assert "tps=0" in rr.iloc[0]["reason"]
    assert "reps" not in rr.iloc[0]["reason"]
    assert "tps=0" in path.read_text()


def test_plot_exp_skips_zero_throughput(tmp_path):
    fig = plot.plot_exp(zero_tps_agg(), "e", "clients", tmp_path / "z.png")
    try:
        ax = fig.axes[0]
        lines = series(ax)
        assert len(lines) == 1
        assert list(lines[0].get_xdata()) == [16, 64]
        assert any("not shown" in t for t in legend_texts(ax))
        lo, hi = ax.get_ylim()
        assert lo > 0 and hi / lo < 1e3  # sane log range
    finally:
        plt.close(fig)


def test_plot_exp_all_zero_does_not_degenerate(tmp_path):
    a = plot.aggregate(make_df([0, 0, 0]))
    fig = plot.plot_exp(a, "e", "clients", tmp_path / "z0.png")
    try:
        assert (tmp_path / "z0.png").read_bytes().startswith(PNG_MAGIC)
        lo, hi = fig.axes[0].get_ylim()
        assert 0 < lo < hi
    finally:
        plt.close(fig)


def test_load_csvs_rejects_bad_header_naming_file(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("ts,commit,exp\n1,2,3\n")
    with pytest.raises(plot.InputError) as ei:
        plot.load_csvs([bad])
    assert "bad.csv" in str(ei.value)
    assert "header" in str(ei.value)


def test_load_csvs_columns_match_fixture():
    assert list(plot.load_csvs([FIXTURE]).columns) == plot.EXPECTED_COLUMNS


def run_main(capsys, argv):
    rc = plot.main(argv)
    cap = capsys.readouterr()
    return rc, cap.out, cap.err


def test_main_missing_file_exit_2(tmp_path, capsys):
    rc, out, err = run_main(capsys, [str(tmp_path / "nope*.csv"), "--out", str(tmp_path / "figs")])
    assert rc == 2
    assert err.startswith("plot.py: error:")
    assert "nope*.csv" in err
    assert len(err.strip().splitlines()) == 1
    assert not (tmp_path / "figs").exists()


def test_main_empty_csv_exit_2(tmp_path, capsys):
    empty = tmp_path / "empty.csv"
    empty.write_text("")
    rc, out, err = run_main(capsys, [str(empty), "--out", str(tmp_path / "figs")])
    assert rc == 2
    assert "empty.csv" in err and "empty" in err
    assert "Traceback" not in err


def test_main_header_only_csv_exit_2(tmp_path, capsys):
    hdr = tmp_path / "hdr.csv"
    hdr.write_text(",".join(plot.EXPECTED_COLUMNS) + "\n")
    rc, out, err = run_main(capsys, [str(hdr), "--out", str(tmp_path / "figs")])
    assert rc == 2
    assert "no data rows" in err


def test_main_bad_header_exit_2(tmp_path, capsys):
    bad = tmp_path / "bad.csv"
    bad.write_text("a,b,c\n1,2,3\n")
    rc, out, err = run_main(capsys, [str(bad), "--out", str(tmp_path / "figs")])
    assert rc == 2
    assert "bad.csv" in err
    assert len(err.strip().splitlines()) == 1


def test_main_writes_figs_and_rerun(tmp_path, capsys):
    out = tmp_path / "results" / "figs"
    rc, stdout, err = run_main(capsys, [str(FIXTURE), "--out", str(out)])
    assert rc == 0
    assert (out / "e1_tps_vs_clients.png").read_bytes().startswith(PNG_MAGIC)
    assert (out / "e2_tps_vs_n.png").read_bytes().startswith(PNG_MAGIC)
    # only axes that vary are plotted
    assert not (out / "e1_tps_vs_n.png").exists()
    assert not (out / "e2_tps_vs_clients.png").exists()
    assert "no figure produced" not in stdout
    # default rerun path is <out>/../rerun.txt
    rerun = tmp_path / "results" / "rerun.txt"
    assert len(rerun.read_text().splitlines()) == 2


def test_main_notes_exp_without_varying_axis(tmp_path, capsys):
    csv = tmp_path / "flat.csv"
    make_df([100, 100, 100], exp="flat")[plot.EXPECTED_COLUMNS].to_csv(csv, index=False)
    out = tmp_path / "results" / "figs"
    rc, stdout, err = run_main(capsys, [str(csv), "--out", str(out)])
    assert rc == 0
    assert "exp flat: none of clients, n, block_size, workload, rtt_ms varies, no figure produced" in stdout
    assert list(out.glob("*.png")) == []


# ---- write latency from the ops sidecars (final-review C1) ----------------

OPS_HEADER = "op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted"


def write_ops(path, write_line):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(OPS_HEADER + "\nread,300,150.00,1.000,0.700,2.000,0,0\n" + write_line + "\n")


def test_run_tag_matches_run_one():
    row = {"backend": "kafka", "n": 7.0, "f": 2, "clients": 64, "block_size": 100, "workload": "a",
           "rtt_ms": 0, "fault": "F5-F4:node7+node6+node5@60", "commit_mode": "sync", "rep": 1}
    assert plot.run_tag(row) == "kafka_n7_f2_c64_b100_wa_rtt0_F5-F4_node7_node6_node5_60_sync_r1"


def test_read_write_latency(tmp_path):
    p = tmp_path / "x.ops.csv"
    write_ops(p, "write,200,100.00,510.000,450.000,990.000,1,3")
    assert plot.read_write_latency(p) == (510.0, 450.0, 990.0)
    write_ops(p, "write,0,0.00,0.000,0.000,0.000,5,0")
    assert plot.read_write_latency(p) is None  # no successful write
    p.write_text("junk\n")
    assert plot.read_write_latency(p) is None
    assert plot.read_write_latency(tmp_path / "missing.ops.csv") is None


def test_apply_write_latency_uses_sidecar_else_csv(tmp_path):
    df = make_df([100, 100], exp="e1", backend="raft", clients=16)
    df["rep"] = [1, 2]
    df["lat_p50_ms"] = [3.0, 3.0]
    tag1 = plot.run_tag(df.iloc[0])
    write_ops(tmp_path / "e1" / (tag1 + ".ops.csv"), "write,200,100.00,510.000,450.000,990.000,0,0")
    out = plot.apply_write_latency(df, [str(tmp_path)])
    assert list(out["lat_source"]) == ["write", "csv"]
    assert out.iloc[0]["lat_p50_ms"] == 450.0 and out.iloc[0]["lat_p99_ms"] == 990.0
    assert out.iloc[1]["lat_p50_ms"] == 3.0
    assert "lat_source" not in plot.config_columns(out)
    # a non-default BATCH_FLUSH tag suffix is found too
    tag2 = plot.run_tag(df.iloc[1])
    write_ops(tmp_path / "e1" / (tag2 + "_bf100ms.ops.csv"), "write,10,5.00,90.000,80.000,150.000,0,0")
    out = plot.apply_write_latency(df, [str(tmp_path)])
    assert list(out["lat_source"]) == ["write", "write"] and out.iloc[1]["lat_p50_ms"] == 80.0


def test_main_latency_figures_use_write_latency(tmp_path, capsys):
    raw_dir = tmp_path / "results" / "raw"
    raw_dir.mkdir(parents=True)
    csv = raw_dir / "e1.csv"
    frames = []
    for c in (4, 16):
        d = make_df([100, 110, 105], exp="e1", backend="raft", clients=c)
        d["rep"] = [1, 2, 3]
        frames.append(d)
    df = pd.concat(frames, ignore_index=True)
    df[plot.EXPECTED_COLUMNS].to_csv(csv, index=False)
    for _, row in df.iterrows():
        write_ops(raw_dir / "e1" / (plot.run_tag(row) + ".ops.csv"), "write,200,100.00,510.000,450.000,990.000,0,0")
    out = tmp_path / "results" / "figs"
    rc, stdout, err = run_main(capsys, [str(csv), "--out", str(out)])
    assert rc == 0
    assert (out / "e1_lat_vs_clients.png").read_bytes().startswith(PNG_MAGIC)
    assert "write latency from ops sidecars for 6 of 6 rows" in stdout
    raw = plot.apply_write_latency(plot.load_csvs([csv]), [str(raw_dir)])
    agg = plot.aggregate(raw)
    assert set(agg["lat_p50_ms_mean"]) == {450.0}
    fig = plot.plot_latency(agg, "e1", "clients", out / "again.png")
    assert len([l for l in fig.axes[0].get_lines() if l.get_gid() == "p50"]) == 1
    plt.close(fig)


# ---- x axis per experiment (task 19a) ---------------------------------------

def sweep(col, values, **cfg):
    """Raw frame: one backend over `values` of configuration column `col`."""
    df = pd.concat([make_df([100, 110, 105], **{col: v}, **cfg) for v in values], ignore_index=True)
    return df


@pytest.mark.parametrize("col,values", [
    ("clients", [4, 16, 64]), ("n", [4, 7]), ("block_size", [10, 100, 1000]),
    ("workload", ["a", "b", "c"]), ("rtt_ms", [0, 20, 60])])
def test_varying_axes_picks_the_varying_column(col, values):
    a = plot.aggregate(sweep(col, values))
    assert plot.varying_axes(a) == [col]


def test_varying_axes_none_and_several():
    assert plot.varying_axes(plot.aggregate(make_df([1, 2, 3]))) == []
    df = pd.concat([sweep("clients", [4, 16], rtt_ms=0), sweep("clients", [4, 16], rtt_ms=20)])
    assert plot.varying_axes(plot.aggregate(df)) == ["clients", "rtt_ms"]


def test_plot_exp_block_size_log_axis_with_ticks_at_values(tmp_path):
    fig = plot.plot_exp(plot.aggregate(sweep("block_size", [10, 100, 1000])), "e", "block_size", tmp_path / "b.png")
    try:
        ax = fig.axes[0]
        assert ax.get_xscale() == "log"
        assert list(ax.get_xticks()) == [10, 100, 1000]
        assert [t.get_text() for t in ax.get_xticklabels()] == ["10", "100", "1000"]
        assert len(series(ax)) == 1
    finally:
        plt.close(fig)


def test_plot_exp_workload_is_categorical(tmp_path):
    agg = plot.aggregate(sweep("workload", ["c", "a", "b"]))
    fig = plot.plot_exp(agg, "e", "workload", tmp_path / "w.png")
    fig2 = plot.plot_latency(agg, "e", "workload", tmp_path / "wl.png")
    try:
        ax = fig.axes[0]
        assert ax.get_xscale() == "linear"
        assert [t.get_text() for t in ax.get_xticklabels()] == ["a", "b", "c"]
        assert list(series(ax)[0].get_xdata()) == [0, 1, 2]
        assert [t.get_text() for t in fig2.axes[0].get_xticklabels()] == ["a", "b", "c"]
    finally:
        plt.close(fig)
        plt.close(fig2)


def test_plot_exp_rtt_linear_axis(tmp_path):
    fig = plot.plot_exp(plot.aggregate(sweep("rtt_ms", [0, 20, 60])), "e", "rtt_ms", tmp_path / "r.png")
    try:
        ax = fig.axes[0]
        assert ax.get_xscale() == "linear"
        assert list(ax.get_xticks()) == [0, 20, 60]
    finally:
        plt.close(fig)


def test_main_draws_one_figure_pair_per_varying_axis(tmp_path, capsys):
    csv = tmp_path / "in.csv"
    frames = [sweep("block_size", [10, 100], exp="e3"), sweep("workload", ["a", "b"], exp="e4"),
              sweep("rtt_ms", [0, 20], exp="e5")]
    pd.concat(frames, ignore_index=True)[plot.EXPECTED_COLUMNS].to_csv(csv, index=False)
    out = tmp_path / "results" / "figs"
    rc, stdout, err = run_main(capsys, [str(csv), "--out", str(out)])
    assert rc == 0
    for name in ("e3_tps_vs_block_size", "e3_lat_vs_block_size", "e4_tps_vs_workload",
                 "e4_lat_vs_workload", "e5_tps_vs_rtt_ms", "e5_lat_vs_rtt_ms"):
        assert (out / (name + ".png")).read_bytes().startswith(PNG_MAGIC)
    assert "no figure produced" not in stdout


# ---- E6 bars per fault scenario (task 19a) ----------------------------------

def test_fault_scenario_strips_nodes_and_time():
    assert plot.fault_scenario("F1:node4@60") == "F1"
    assert plot.fault_scenario("F5-F2:node7+node6+node5@60") == "F5-F2"
    assert plot.fault_scenario("none") == "none"


def fault_frame(spec):
    """Raw E6 frame from {(backend, n, fault): tps}; three reps (tps-5, tps, tps+5)."""
    frames = [make_df([t - 5, t, t + 5], exp="e6", backend=b, n=n, f=1 if n == 4 else 2, clients=256,
                      fault=fault) for (b, n, fault), t in spec.items()]
    return pd.concat(frames, ignore_index=True)


def test_fault_labels_strip_node_list():
    a = plot.aggregate(fault_frame({("bft2", 4, "F1:node4@60"): 100, ("raft", 4, "F1:node4@60"): 200,
                                    ("bft2", 4, "F5-F2:node4+node3@60"): 90}))
    assert plot.fault_labels(a) == {"F1:node4@60": "F1", "F5-F2:node4+node3@60": "F5-F2"}


def test_fault_labels_two_variants_of_one_backend_get_node_lists():
    a = plot.aggregate(fault_frame({("kafka", 4, "F1:node1@60"): 100, ("kafka", 4, "F1:node4@60"): 110,
                                    ("tendermint", 4, "F1:node4@60"): 50, ("kafka", 4, "F2:node4@60"): 90}))
    assert plot.fault_labels(a) == {"F1:node1@60": "F1 (node1)", "F1:node4@60": "F1 (node4)",
                                    "F2:node4@60": "F2"}


def test_fault_labels_list_input_uses_one_backend():
    got = plot.fault_labels(["F1:node1@60", "F1:node7+node6@60", "F2:node7+node6@60"])
    assert got == {"F1:node1@60": "F1 (node1)", "F1:node7+node6@60": "F1 (node7+node6)", "F2:node7+node6@60": "F2"}


def test_nofault_reference_matches_the_e2_row_only():
    e2 = make_df([300, 300, 300], exp="e2", backend="raft", n=4, clients=256)
    other_clients = make_df([999, 999, 999], exp="e2", backend="raft", n=4, clients=64)
    other_n = make_df([777, 777, 777], exp="e2", backend="raft", n=7, clients=256)
    e6 = fault_frame({("raft", 4, "F1:node4@60"): 100})
    a = plot.aggregate(pd.concat([e2, other_clients, other_n, e6], ignore_index=True))
    assert plot.nofault_reference(a, 4, "tps") == {"raft": 300.0}
    assert plot.nofault_reference(a, 7, "tps") == {"raft": 777.0}
    assert plot.nofault_reference(plot.aggregate(e6), 4, "tps") == {}


def test_plot_fault_bars_groups_bars_and_reference(tmp_path):
    spec = {("kafka", 4, "F1:node1@60"): 100, ("kafka", 4, "F1:node4@60"): 110, ("kafka", 4, "F2:node4@60"): 90,
            ("tendermint", 4, "F1:node4@60"): 50, ("tendermint", 4, "F2:node4@60"): 40}
    df = pd.concat([fault_frame(spec), fault_frame({("kafka", 7, "F1:node7+node6@60"): 70}),
                    make_df([300, 300, 300], exp="e2", backend="kafka", n=4, clients=256)], ignore_index=True)
    a = plot.aggregate(df)
    out = tmp_path / "e6_tps_by_fault_n4.png"
    fig = plot.plot_fault_bars(a, "e6", 4, "tps", out, plot.nofault_reference(a, 4, "tps"))
    try:
        assert out.read_bytes().startswith(PNG_MAGIC)
        ax = fig.axes[0]
        assert [t.get_text() for t in ax.get_xticklabels()] == ["F1\n(node1)", "F1\n(node4)", "F2"]
        assert not any("n=7" in t.get_text() for t in ax.get_xticklabels())
        heights = sorted(p.get_height() for p in ax.patches)
        assert heights == [40, 50, 90, 100, 110]
        # one dashed reference tick per kafka bar (3), none for tendermint
        refs = [l for l in ax.get_lines() if l.get_gid() == "reference"]
        assert len(refs) == 3 and all(list(l.get_ydata()) == [300, 300] for l in refs)
        assert any("no fault" in t for t in legend_texts(ax))
    finally:
        plt.close(fig)
    # without an E2 row there is no reference and no legend entry
    fig = plot.plot_fault_bars(a, "e6", 7, "lat_p99_ms", tmp_path / "l.png")
    try:
        assert not [l for l in fig.axes[0].get_lines() if l.get_gid() == "reference"]
        assert not any("no fault" in t for t in legend_texts(fig.axes[0]))
        assert [p.get_height() for p in fig.axes[0].patches] == [5.0]  # p99 of make_df, one n=7 bar
    finally:
        plt.close(fig)


def test_plot_fault_bars_errors(tmp_path):
    a = plot.aggregate(fault_frame({("raft", 4, "F1:node4@60"): 100}))
    with pytest.raises(ValueError):
        plot.plot_fault_bars(a, "e6", 7, "tps", tmp_path / "x.png")
    with pytest.raises(ValueError):
        plot.plot_fault_bars(a, "e6", 4, "bogus", tmp_path / "x.png")


def test_main_draws_e6_bars_per_n_and_not_lines(tmp_path, capsys):
    csv = tmp_path / "in.csv"
    spec = {(b, n, "F1:node%d@60" % n): 100 for b in ("raft", "bft2") for n in (4, 7)}
    spec.update({(b, n, "F4:node%d@60" % n): 90 for b in ("raft", "bft2") for n in (4, 7)})
    df = pd.concat([fault_frame(spec), make_df([300, 300, 300], exp="e2", backend="raft", n=4, clients=256)],
                   ignore_index=True)
    df[plot.EXPECTED_COLUMNS].to_csv(csv, index=False)
    out = tmp_path / "results" / "figs"
    rc, stdout, err = run_main(capsys, [str(csv), "--out", str(out)])
    assert rc == 0, err
    for n in (4, 7):
        assert (out / ("e6_tps_by_fault_n%d.png" % n)).read_bytes().startswith(PNG_MAGIC)
        assert (out / ("e6_lat_by_fault_n%d.png" % n)).read_bytes().startswith(PNG_MAGIC)
    assert not (out / "e6_tps_vs_n.png").exists()
    # e2 has no varying axis here
    assert "exp e2: none of" in stdout
