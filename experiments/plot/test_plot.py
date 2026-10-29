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
    assert "exp flat: neither clients nor n varies, no figure produced" in stdout
    assert list(out.glob("*.png")) == []
