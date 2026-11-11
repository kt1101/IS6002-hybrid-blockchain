#!/usr/bin/env python3
"""E10 (phase 3, fault-free read-path cost) latency per read mode, workload and op.

Usage:
    python3 experiments/plot/e10_latency.py results/raw/e10-*.csv --raw-dir results/raw --out results/e1x

E10 EXPs (experiments/exp/e10_readcost.sh) are e10-<read mode>. For every CSV
row the read and write lines of its <tag>.ops.csv sidecar (written by
ycsbbench through run_one.sh) give lat_p50_ms, lat_p99_ms and lat_mean_ms of
that op over the measurement window. Rows are grouped by (backend, read mode,
workload, op); each group reports the median over its reps and the min-max
across reps. An op with count 0 (writes of workload c) is left out.
A row without an ops sidecar is listed with a note and skipped.

Output: <out>/e10_latency.csv
    backend, read_mode, workload, op, reps, p50_ms, p50_min_ms, p50_max_ms,
    p99_ms, p99_min_ms, p99_max_ms, mean_ms, tps
(medians over reps; tps = median op throughput). Standard library only.
"""

import argparse
import statistics
import sys
from pathlib import Path

import rq3
import rq6

COLUMNS = ["backend", "read_mode", "workload", "op", "reps", "p50_ms", "p50_min_ms", "p50_max_ms",
           "p99_ms", "p99_min_ms", "p99_max_ms", "mean_ms", "tps"]
OPS = ("read", "write")


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _r(v, nd=3):
    return "" if v is None else round(v, nd)


def read_mode_of(exp):
    """e10-<mode> -> <mode>; anything else -> None."""
    exp = str(exp)
    return exp[len("e10-"):] if exp.startswith("e10-") and len(exp) > 4 else None


def analyse(csv_paths, raw_dir):
    """(rows of e10_latency.csv, notes)."""
    groups, notes = {}, []
    for row in rq6.load_rows(csv_paths):
        mode = read_mode_of(row["exp"])
        if mode is None:
            notes.append("exp %s is not an E10 EXP (e10-<read mode>); row skipped" % row["exp"])
            continue
        ops = rq6.run_sidecar(raw_dir, row, ".ops.csv")
        if ops is None:
            notes.append("%s %s rep %s: no .ops.csv sidecar; row skipped" % (row["exp"], row["backend"], row["rep"]))
            continue
        for op in OPS:
            line = rq6.read_ops_line(ops, op)
            if not line or not (_f(line.get("count")) or 0) > 0:
                continue
            key = (row["backend"], mode, row["workload"], op)
            groups.setdefault(key, []).append(line)
    out = []
    for key in sorted(groups):
        lines = groups[key]

        def col(c):
            return [v for v in (_f(l.get(c)) for l in lines) if v is not None]

        p50, p99, mean, tps = col("lat_p50_ms"), col("lat_p99_ms"), col("lat_mean_ms"), col("tps")
        med = lambda v: statistics.median(v) if v else None
        out.append(dict(zip(COLUMNS, key + (len(lines),))) | {
            "p50_ms": _r(med(p50)), "p50_min_ms": _r(min(p50) if p50 else None), "p50_max_ms": _r(max(p50) if p50 else None),
            "p99_ms": _r(med(p99)), "p99_min_ms": _r(min(p99) if p99 else None), "p99_max_ms": _r(max(p99) if p99 else None),
            "mean_ms": _r(med(mean)), "tps": _r(med(tps), 2)})
    return out, notes


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("csv", nargs="+")
    p.add_argument("--raw-dir", required=True, help="dir holding <exp>/<tag>.* (results/raw)")
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    try:
        rows, notes = analyse(a.csv, a.raw_dir)
    except (rq6.InputError, rq3.InputError) as e:
        print("e10_latency.py: error: %s" % e, file=sys.stderr)
        return 2
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rq6._write(out / "e10_latency.csv", COLUMNS, rows)
    print("  ".join(COLUMNS))
    for r in rows:
        print("  ".join(str(r[c]) for c in COLUMNS))
    for n in notes:
        print("note: " + n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
