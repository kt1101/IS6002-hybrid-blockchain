#!/usr/bin/env python3
"""Aggregate benchmark CSVs, flag configurations to re-run, draw figures.

Usage:
    python3 experiments/plot/plot.py results/raw/*.csv --out results/figs

Dependencies: pandas, matplotlib (Agg backend, works headless).

Conventions
-----------
* Configuration = every CSV column except ts, commit, rep and the measured
  columns (tps, lat_*, errors, wrong_gets, host_cpu_max_pct, saturated,
  net_bytes_per_block).  `commit` (which may carry a "-dirty" suffix) is
  therefore never part of the grouping key.
* std is the SAMPLE standard deviation (ddof=1).  With a single rep it is NaN.
* cv = tps_std / tps_mean.
* Re-run rule (plan): a configuration needs a re-run if cv > 0.10 or it has
  fewer than 3 reps.  A configuration with 1 rep has cv = NaN (comparison is
  False) and is flagged by the reps rule only.  Additionally a configuration
  with tps_mean <= 0 is flagged (reason `tps=0`; NaN mean: `tps=nan`).
* saturated (aggregated) is tri-state: True if any rep is 1, False if all reps
  are 0, NaN (unknown) otherwise; unknown is never drawn hollow.
* Every input CSV must have the fixed 22-column header; CLI input errors print
  one line to stderr and exit with status 2.
* net_bytes_per_block may be empty in the CSV; it is parsed as NaN and never
  makes anything fail.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless; must precede the pyplot import

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.ticker import NullLocator  # noqa: E402

# Columns that are not part of the configuration key.
NON_CONFIG_COLS = (
    "ts", "commit", "rep",
    "tps", "lat_mean_ms", "lat_p50_ms", "lat_p99_ms",
    "errors", "wrong_gets", "host_cpu_max_pct", "saturated",
    "net_bytes_per_block",
)
# Measured columns aggregate() needs (net_bytes_per_block is optional).
REQUIRED_MEASURED = ("tps", "lat_p50_ms", "lat_p99_ms", "saturated")

# Columns produced by aggregate() besides the configuration columns.
STAT_COLS = (
    "reps",
    "tps_mean", "tps_std",
    "lat_p50_ms_mean", "lat_p50_ms_std",
    "lat_p99_ms_mean", "lat_p99_ms_std",
    "net_bytes_per_block_mean",
    "cv", "saturated",
)

CV_THRESHOLD = 0.10
MIN_REPS = 3
DEFAULT_RERUN_PATH = "results/rerun.txt"


EXPECTED_COLUMNS = [
    "ts", "commit", "exp", "backend", "n", "f", "clients", "block_size",
    "workload", "rtt_ms", "fault", "rep", "commit_mode", "tps", "lat_mean_ms",
    "lat_p50_ms", "lat_p99_ms", "errors", "wrong_gets", "host_cpu_max_pct",
    "saturated", "net_bytes_per_block",
]  # same as HBDB_CSV_HEADER in experiments/lib.sh


class InputError(ValueError):
    """Unusable input file (missing, empty, wrong header, unparsable)."""


def _read_one(path) -> pd.DataFrame:
    p = Path(path)
    try:
        with open(p, newline="", encoding="utf-8-sig") as fh:
            first = fh.readline().strip()
    except FileNotFoundError:
        raise InputError("%s: no such file (unmatched glob?)" % p)
    except OSError as e:
        raise InputError("%s: cannot read (%s)" % (p, e.strerror or e))
    except UnicodeDecodeError:
        raise InputError("%s: not a UTF-8 CSV" % p)
    if not first:
        raise InputError("%s: empty CSV" % p)
    header = first.split(",")
    if header != EXPECTED_COLUMNS:
        raise InputError(
            "%s: unexpected CSV header (expected %d columns starting %s,... ending ...,%s; got %d columns: %s)"
            % (p, len(EXPECTED_COLUMNS), EXPECTED_COLUMNS[0], EXPECTED_COLUMNS[-1],
               len(header), first))
    try:
        return pd.read_csv(p, keep_default_na=False, na_values=[""], encoding="utf-8-sig")
    except (pd.errors.ParserError, UnicodeDecodeError) as e:
        raise InputError("%s: cannot parse CSV (%s)" % (p, e))


def load_csvs(paths) -> pd.DataFrame:
    """Read and concatenate result CSVs.

    Every file must have exactly the fixed 22-column header (EXPECTED_COLUMNS);
    otherwise InputError names the file.  Only the empty string counts as
    missing (so a fault named "none" stays a string); empty numeric cells, e.g.
    net_bytes_per_block, become NaN.
    """
    frames = [_read_one(p) for p in paths]
    if not frames:
        raise InputError("no input CSV files")
    return pd.concat(frames, ignore_index=True)


def config_columns(df: pd.DataFrame) -> list:
    """Configuration (grouping) columns of a raw result frame."""
    return [c for c in df.columns if c not in NON_CONFIG_COLS]


def _agg_config_columns(agg: pd.DataFrame) -> list:
    return [c for c in agg.columns if c not in STAT_COLS]


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """Group reps of one configuration and compute mean/std.

    Returns one row per configuration: the configuration columns, then
    reps, tps/lat_p50_ms/lat_p99_ms mean+std (sample std, ddof=1),
    net_bytes_per_block_mean, cv = tps_std / tps_mean and saturated.

    saturated is tri-state (object column): True if ANY rep has saturated >= 1,
    False if EVERY rep has saturated == 0, otherwise NaN (unknown: empty or
    non-numeric values and no rep known to be saturated).  Unknown is never
    treated as 0.
    """
    missing = [c for c in REQUIRED_MEASURED if c not in df.columns]
    if missing:
        raise ValueError("missing column(s): " + ", ".join(missing))

    df = df.copy()
    cfg = config_columns(df)
    if not cfg:
        raise ValueError("no configuration columns found")
    if "net_bytes_per_block" not in df.columns:
        df["net_bytes_per_block"] = np.nan
    for c in ("tps", "lat_p50_ms", "lat_p99_ms", "net_bytes_per_block"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    sat = pd.to_numeric(df["saturated"], errors="coerce")
    df["_sat1"] = (sat >= 1).astype(int)
    df["_sat0"] = (sat == 0).astype(int)  # NaN == 0 is False

    out = df.groupby(cfg, dropna=False, sort=True, as_index=False).agg(
        reps=("tps", "size"),
        tps_mean=("tps", "mean"),
        tps_std=("tps", "std"),
        lat_p50_ms_mean=("lat_p50_ms", "mean"),
        lat_p50_ms_std=("lat_p50_ms", "std"),
        lat_p99_ms_mean=("lat_p99_ms", "mean"),
        lat_p99_ms_std=("lat_p99_ms", "std"),
        net_bytes_per_block_mean=("net_bytes_per_block", "mean"),
        _sat1=("_sat1", "max"),
        _sat0=("_sat0", "sum"),
    )
    out["saturated"] = pd.Series(
        [True if a > 0 else (False if z == r else np.nan)
         for a, z, r in zip(out["_sat1"], out["_sat0"], out["reps"])],
        index=out.index, dtype=object)
    out["cv"] = out["tps_std"] / out["tps_mean"]
    return out[cfg + list(STAT_COLS)].reset_index(drop=True)


def rerun_list(agg: pd.DataFrame, out_path=DEFAULT_RERUN_PATH) -> pd.DataFrame:
    """Configurations that need re-running.

    Flagged when cv > 0.10, or fewer than 3 reps, or zero throughput
    (tps_mean <= 0, reason `tps=0`; a missing/NaN mean gets `tps=nan`).
    Returns the flagged rows with an extra `reason` column (comma-joined).
    Unless out_path is None, also writes them to out_path (default
    results/rerun.txt), one line per configuration:
    `key=value ... reps=R cv=C reason=...`.  The file is empty when nothing
    needs re-running.
    """
    few = agg["reps"] < MIN_REPS
    noisy = agg["cv"] > CV_THRESHOLD  # NaN compares False
    bad_tps = ~(agg["tps_mean"] > 0)  # zero, negative or NaN
    flagged = agg[few | noisy | bad_tps].copy()

    def reason(row) -> str:
        parts = []
        if row["reps"] < MIN_REPS:
            parts.append("reps<%d" % MIN_REPS)
        if row["cv"] > CV_THRESHOLD:
            parts.append("cv>%.2f" % CV_THRESHOLD)
        if pd.isna(row["tps_mean"]):
            parts.append("tps=nan")
        elif row["tps_mean"] <= 0:
            parts.append("tps=0")
        return ",".join(parts)

    flagged["reason"] = [reason(r) for r in flagged.to_dict("records")]
    flagged = flagged.reset_index(drop=True)

    if out_path is not None:
        cfg = _agg_config_columns(agg)
        lines = []
        for row in flagged.to_dict("records"):
            kv = " ".join("%s=%s" % (c, row[c]) for c in cfg)
            lines.append("%s reps=%d cv=%.3f reason=%s"
                         % (kv, row["reps"], row["cv"], row["reason"]))
        path = Path(out_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(line + "\n" for line in lines))
    return flagged


def plot_exp(agg: pd.DataFrame, exp: str, x: str, out_png):
    """Throughput (log y) vs configuration column `x` for one experiment.

    Writes the PNG and returns the matplotlib Figure; the CALLER must close it
    (plt.close(fig)).

    One line per backend.  Another configuration column splits a backend into
    several lines only if it varies at a FIXED x within that backend; a column
    determined by x (e.g. f as a function of n) never splits lines.  Error bars
    are the sample std over reps.  Points where any rep was saturated are drawn
    with hollow (white-filled) markers and a legend entry says so (only when
    such a point exists; unknown saturation is drawn filled).  Points with
    tps_mean <= 0 cannot be shown on a log axis: they are skipped and counted in
    the legend.

    Artist tags (gid): "series" = the line of one series, "filled"/"hollow" =
    its marker-only artists.
    """
    if x not in agg.columns:
        raise ValueError("unknown x column: %s" % x)
    sub = agg[agg["exp"] == exp].dropna(subset=[x])
    if sub.empty:
        raise ValueError("no data for exp=%r with x=%r" % (exp, x))

    cfg = _agg_config_columns(agg)
    extra = [c for c in cfg
             if c not in ("exp", "backend", x)
             and sub.groupby(["backend", x])[c].nunique(dropna=False).max() > 1]

    fig, ax = plt.subplots(figsize=(6.4, 4.2))
    skipped = 0
    any_sat = False
    for i, (key, g) in enumerate(sub.groupby(["backend"] + extra, dropna=False, sort=True)):
        if not isinstance(key, tuple):
            key = (key,)
        label = str(key[0]) + "".join(" %s=%s" % (c, v) for c, v in zip(extra, key[1:]))
        g = g.sort_values(x)
        color = "C%d" % (i % 10)
        xs = g[x].to_numpy(dtype=float)
        mean = g["tps_mean"].to_numpy(dtype=float)
        std = g["tps_std"].fillna(0).to_numpy(dtype=float)
        sat = g["saturated"].map(lambda v: False if pd.isna(v) else bool(v)).to_numpy(dtype=bool)

        pos = mean > 0  # False for zero, negative and NaN
        skipped += int((~pos).sum())
        xs, mean, std, sat = xs[pos], mean[pos], std[pos], sat[pos]
        # keep the lower error bar positive so it is drawable on a log axis
        lower = np.clip(np.minimum(std, 0.99 * mean), 0, None)

        ax.plot(xs, mean, "-", color=color, label=label, gid="series")
        if len(xs):
            ax.errorbar(xs, mean, yerr=[lower, std], fmt="none", ecolor=color,
                        capsize=3, elinewidth=1)
        if (~sat).any():
            ax.plot(xs[~sat], mean[~sat], "o", color=color, zorder=3, gid="filled")
        if sat.any():
            any_sat = True
            ax.plot(xs[sat], mean[sat], "o", markerfacecolor="white",
                    markeredgecolor=color, markeredgewidth=1.5, zorder=3, gid="hollow")

    ax.set_yscale("log")
    if skipped == len(sub):
        ax.set_ylim(1, 10)  # nothing drawable; avoid a degenerate range
    xs_all = sorted(sub[x].dropna().unique())
    if x == "clients" and min(xs_all) > 0:
        ax.set_xscale("log", base=2)
    ax.set_xticks(xs_all)
    ax.set_xticklabels(["%g" % v for v in xs_all])
    ax.xaxis.set_minor_locator(NullLocator())
    ax.set_xlabel(x)
    ax.set_ylabel("throughput (tx/s, log scale)")
    ax.set_title("%s: throughput vs %s" % (exp, x))
    ax.grid(True, which="both", alpha=0.3)

    handles, labels = ax.get_legend_handles_labels()
    if any_sat:
        handles.append(Line2D([], [], linestyle="", marker="o", markerfacecolor="white",
                              markeredgecolor="black"))
        labels.append("hollow = saturated (any rep)")
    if skipped:
        handles.append(Line2D([], [], linestyle=""))
        labels.append("%d point(s) with tps<=0 not shown (log axis)" % skipped)
    if handles:
        ax.legend(handles, labels, fontsize=8)

    out_png = Path(out_png)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    return fig


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("csv", nargs="+", help="result CSV files (results/raw/*.csv)")
    ap.add_argument("--out", default="results/figs", help="figure output directory")
    ap.add_argument("--rerun", default=None,
                    help="re-run list path (default: <out>/../rerun.txt, i.e. results/rerun.txt)")
    args = ap.parse_args(argv)

    try:
        raw = load_csvs(args.csv)
        if raw.empty:
            raise InputError("no data rows in: %s" % " ".join(args.csv))
    except InputError as e:
        print("plot.py: error: %s" % e, file=sys.stderr)
        return 2

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    rerun_path = Path(args.rerun) if args.rerun else out_dir.parent / "rerun.txt"

    agg = aggregate(raw)
    flagged = rerun_list(agg, rerun_path)

    written = []
    notes = []
    for exp in sorted(agg["exp"].unique()):
        sub = agg[agg["exp"] == exp]
        made = False
        for x in ("clients", "n"):
            if x in sub.columns and sub[x].nunique() > 1:
                png = out_dir / ("%s_tps_vs_%s.png" % (exp, x))
                plt.close(plot_exp(agg, exp, x, png))
                written.append(png)
                made = True
        if not made:
            notes.append("exp %s: neither clients nor n varies, no figure produced" % exp)

    print("%d configurations, %d need re-run -> %s" % (len(agg), len(flagged), rerun_path))
    for p in written:
        print("wrote %s" % p)
    for note in notes:
        print(note)
    return 0


if __name__ == "__main__":
    sys.exit(main())
