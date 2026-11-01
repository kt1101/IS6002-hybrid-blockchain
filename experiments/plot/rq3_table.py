#!/usr/bin/env python3
"""RQ3 summary table: the rq3.py metrics of every E6 run, aggregated over reps.

Usage:
    python3 experiments/plot/rq3_table.py results/raw/e6 [results/raw/e6-rerun ...] \\
        --out results/figs

Writes <out>/rq3_summary.csv with one row per configuration (backend, n, f,
fault: the fault column of the .faults file, e.g. F1:node4@60, so the kafka/raft
leader and follower variants of a scenario are separate rows).  Pure standard
library; the metrics come from rq3.load_run (see rq3.py for their meaning), this
file only collects and aggregates.

Inputs
------
Every <tag>.timeline of the given run directories (written by
experiments/run_one.sh) is one run; its <tag>.faults and optional <tag>.audit
sit next to it, and N comes from the `_n<N>_` part of the tag, as in rq3.py's
CLI.  backend, n and f are read from the tag (<backend>_n<N>_f<F>_c..._r<REP>);
runs differing only in the rep number are the reps of one configuration.  If
two directories hold the same tag, the later directory wins (a re-run replaces
the run it repeats, it is not a further rep).

Columns
-------
backend, n, f, fault, reps (= usable runs), then for every numeric metric M of
rq3.load_run (METRICS) the columns M_mean and M_std (sample std, ddof=1; empty
with fewer than 2 values).  A metric that can be None in a run (NULLABLE: no
dip, not recovered, not detected, no audit) is averaged over the runs where it
exists, and M_n says how many that are.  recovery_status and detect_status
become count strings such as "recovered:2;not_recovered:1" (an "invalid: why"
detect_status counts as "invalid"), and warned_reps counts the runs with an
rq3 warning.  Numbers are rounded to 3 decimals.

A run whose metrics are unusable (rq3.InputError: missing or malformed
.faults/.timeline, no injection row, unparsable tag) is skipped with one line
on stderr and never aborts the table; the last stdout line is `skipped N`.
Exit status 2 and one stderr line for a missing run directory or when no run
was usable (then no CSV is written).
"""
from __future__ import annotations

import argparse
import csv
import re
import statistics
import sys
from pathlib import Path

import rq3

KEY_COLS = ("backend", "n", "f", "fault")
# Numeric metrics of rq3.load_run (its "n" is a key column, not listed here).
METRICS = (
    "fault_at_s", "warmup_s", "pre_mean_tps", "faulty_nodes", "threshold_share",
    "recovery_threshold_tps", "dip_start_s", "min_after_fault_tps", "recovery_s",
    "recovery_from_dip_s", "stall_s", "audit_ticks", "audit_last_s", "audit_div_pre",
    "detect_s", "unreachable_s",
)
# Metrics that are None in a run without dip / recovery / detection / audit.
NULLABLE = ("pre_mean_tps", "recovery_threshold_tps", "dip_start_s", "min_after_fault_tps",
            "recovery_s", "recovery_from_dip_s", "audit_ticks", "audit_last_s",
            "audit_div_pre", "detect_s", "unreachable_s")
STATUS_COLS = ("recovery_status", "detect_status")

TAG_RE = re.compile(r"^(?P<backend>.+?)_n(?P<n>[0-9]+)_f(?P<f>[0-9]+)_.*_r(?P<rep>[0-9]+)(?:_bf[A-Za-z0-9.]+)?$")


def parse_tag(tag: str):
    """(backend, n, f, rep) of a run tag like raft_n4_f1_c256_..._sync_r2;
    rq3.InputError if it does not look like one."""
    m = TAG_RE.match(tag)
    if not m:
        raise rq3.InputError("%s: not a run tag (<backend>_n<N>_f<F>_..._r<REP>)" % tag)
    return m["backend"], int(m["n"]), int(m["f"]), int(m["rep"])


def collect_runs(dirs):
    """{tag: (path prefix, metrics dict)} of the usable runs of the run
    directories, plus the list of skip messages.  A later directory replaces
    an earlier run of the same tag.  Raises rq3.InputError for a directory that
    does not exist."""
    runs = {}
    skipped = []
    for d in dirs:
        d = Path(d)
        if not d.is_dir():
            raise rq3.InputError("%s: no such run directory" % d)
        for tl in sorted(d.glob("*.timeline")):
            tag = tl.name[:-len(".timeline")]
            prefix = d / tag
            try:
                parse_tag(tag)
                audit = Path(str(prefix) + ".audit")
                m = rq3.load_run(str(prefix) + ".timeline", str(prefix) + ".faults",
                                 str(audit) if audit.exists() else None,
                                 n=rq3.n_from_tag(prefix))
            except rq3.InputError as e:
                skipped.append("%s: skipped: %s" % (prefix, e))
                runs.pop(tag, None)  # a broken re-run does not keep the old run
                continue
            runs[tag] = (prefix, m)
    return runs, skipped


def _round(v):
    return "" if v is None else round(v, 3)


def _count_string(values):
    counts = {}
    for v in values:
        head = str(v).split(":")[0].strip() or "none"  # "invalid: why" -> invalid
        counts[head] = counts.get(head, 0) + 1
    return ";".join("%s:%d" % (k, counts[k]) for k in sorted(counts))


def summarize(runs):
    """Rows (dicts keyed by header()) of the configurations of collect_runs()'s runs."""
    groups = {}
    for tag, (_, m) in runs.items():
        backend, n, f, _rep = parse_tag(tag)
        groups.setdefault((backend, n, f, m["fault"]), []).append(m)
    rows = []
    for key in sorted(groups):
        ms = groups[key]
        row = dict(zip(KEY_COLS, key))
        row["reps"] = len(ms)
        for c in METRICS:
            vals = [float(m[c]) for m in ms if m.get(c) is not None]
            row[c + "_mean"] = _round(statistics.fmean(vals)) if vals else ""
            row[c + "_std"] = _round(statistics.stdev(vals)) if len(vals) > 1 else ""
            if c in NULLABLE:
                row[c + "_n"] = len(vals)
        for c in STATUS_COLS:
            row[c] = _count_string(m[c] for m in ms)
        row["warned_reps"] = sum(1 for m in ms if m["warnings"])
        rows.append(row)
    return rows


def header():
    cols = list(KEY_COLS) + ["reps"]
    for c in METRICS:
        cols += [c + "_mean", c + "_std"] + ([c + "_n"] if c in NULLABLE else [])
    return cols + list(STATUS_COLS) + ["warned_reps"]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("dirs", nargs="+", help="run directories (results/raw/e6 ...)")
    ap.add_argument("--out", default="results/figs", help="output directory of rq3_summary.csv")
    args = ap.parse_args(argv)
    try:
        runs, skipped = collect_runs(args.dirs)
    except rq3.InputError as e:
        print("rq3_table: error: %s" % e, file=sys.stderr)
        return 2
    for line in skipped:
        print(line, file=sys.stderr)
    if not runs:
        print("rq3_table: error: no usable run in %s" % " ".join(args.dirs), file=sys.stderr)
        print("skipped %d" % len(skipped))
        return 2
    rows = summarize(runs)
    out = Path(args.out) / "rq3_summary.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=header())
        w.writeheader()
        w.writerows(rows)
    print("wrote %s (%d configurations from %d runs)" % (out, len(rows), len(runs)))
    print("skipped %d" % len(skipped))
    return 0


if __name__ == "__main__":
    sys.exit(main())
