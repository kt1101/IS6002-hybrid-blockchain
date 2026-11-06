#!/usr/bin/env python3
"""Saturation throughput of every backend from an open-loop rate sweep (E7).

Usage:
    python3 experiments/plot/saturation.py results/raw/e7*.csv --ops-dir results/raw --out results/e7

A rate R of a (backend, config) is sustained when, averaged over its reps,
tps >= 0.95 * R and the write p99 (from the run's .ops.csv sidecar,
<ops-dir>/<exp>/<tag>[_bf..][_kc..][_tc..]_rate<R>.ops.csv) is <= 2000 ms,
and no rep dropped an operation (the sidecar's `dropped` column, 0 in
every rep). The saturation throughput sat_rps is the largest sustained R
(spec A1). config is the tag suffix that is not part of the CSV row
(kc<cpus>-<mem>, tc<timeout>; "default" for none), so E7 and E7f runs of one
backend are told apart.

Writes <out>/saturation.csv (backend,config,sat_rps,tps_at_sat,
write_p99_ms_at_sat,reps; sat_rps empty when no rate is sustained) and
<out>/saturation_points.csv (every (backend, config, rate) with its means),
prints both tables and the notes. Exit 0, or 2 on unusable input.
"""
import argparse
import math
import re
import sys
from pathlib import Path

import plot

TPS_FRACTION = 0.95
MAX_WRITE_P99_MS = 2000.0

SAT_COLUMNS = ["backend", "config", "sat_rps", "tps_at_sat", "write_p99_ms_at_sat", "reps"]
POINT_COLUMNS = ["backend", "config", "offered_rps", "reps", "tps_mean", "write_p99_ms_mean",
                 "dropped", "sustained"]


class Result:
    def __init__(self):
        self.sat = []
        self.points = []
        self.notes = []


def read_ops(path):
    """(write_p99_ms, dropped) of an ops sidecar, or None."""
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return None
    if not lines:
        return None
    header = lines[0].split(",")
    if header[:len(plot.OPS_HEADER_PREFIX)] != plot.OPS_HEADER_PREFIX:
        return None
    di = header.index("dropped") if "dropped" in header else None
    for line in lines[1:]:
        f = line.split(",")
        if f and f[0] == "write":
            try:
                p99 = float(f[5])
                dropped = int(f[di]) if di is not None and di < len(f) else 0
            except (ValueError, IndexError):
                return None
            return p99, dropped
    return None


def config_of(sidecar, row):
    """The tag suffixes of a sidecar name that are not in the CSV row."""
    rest = sidecar.name[len(plot.run_tag(row)):-len(".ops.csv")]
    parts = [p for p in rest.split("_") if p]
    keep = [p for p in parts if not p.startswith("bf") and not p.startswith("rate")]
    return "_".join(keep) or "default"


def _num(v):
    f = float(v)
    return int(f) if f.is_integer() else f


def analyse(csv_paths, ops_dir):
    res = Result()
    df = plot.load_csvs(csv_paths)
    df = df[df["offered_rps"].astype(float) > 0]
    if df.empty:
        res.notes.append("no open-loop rows (offered_rps > 0)")
        return res
    groups = {}
    for _, row in df.iterrows():
        side = plot.run_sidecar(ops_dir, row, ".ops.csv")
        if side is None:
            res.notes.append("%s rep %s rate %s: no ops sidecar; row ignored"
                             % (row["backend"], row["rep"], row["offered_rps"]))
            continue
        ops = read_ops(side)
        if ops is None:
            res.notes.append("%s: unreadable or no write line; row ignored" % side)
            continue
        key = (row["backend"], config_of(side, row), float(row["offered_rps"]))
        groups.setdefault(key, []).append((float(row["tps"]), ops[0], ops[1]))

    by_cfg = {}
    for (b, cfg, rate), reps in sorted(groups.items()):
        tps = math.fsum(r[0] for r in reps) / len(reps)
        p99 = math.fsum(r[1] for r in reps) / len(reps)
        dropped = sum(r[2] for r in reps)
        ok = tps >= TPS_FRACTION * rate and p99 <= MAX_WRITE_P99_MS and dropped == 0
        pt = {"backend": b, "config": cfg, "offered_rps": _num(rate), "reps": len(reps),
              "tps_mean": round(tps, 2), "write_p99_ms_mean": round(p99, 3),
              "dropped": dropped, "sustained": int(ok)}
        res.points.append(pt)
        by_cfg.setdefault((b, cfg), []).append(pt)

    for (b, cfg), pts in sorted(by_cfg.items()):
        good = [p for p in pts if p["sustained"]]
        if not good:
            res.sat.append({"backend": b, "config": cfg, "sat_rps": "", "tps_at_sat": "",
                            "write_p99_ms_at_sat": "", "reps": ""})
            res.notes.append("%s/%s: no rate sustained (lowest rate %s already fails); sweep lower rates"
                             % (b, cfg, pts[0]["offered_rps"]))
            continue
        best = max(good, key=lambda p: p["offered_rps"])
        res.sat.append({"backend": b, "config": cfg, "sat_rps": best["offered_rps"],
                        "tps_at_sat": _num(best["tps_mean"]),
                        "write_p99_ms_at_sat": _num(best["write_p99_ms_mean"]),
                        "reps": best["reps"]})
        higher = [p for p in pts if p["offered_rps"] > best["offered_rps"] and p["sustained"]]
        if not higher and best is pts[-1]:
            res.notes.append("%s/%s: the highest rate %s is sustained; saturation may be higher"
                             % (b, cfg, best["offered_rps"]))
    return res


def _write(path, cols, rows):
    with open(path, "w") as fh:
        fh.write(",".join(cols) + "\n")
        for r in rows:
            fh.write(",".join(str(r[c]) for c in cols) + "\n")


def _table(cols, rows):
    out = ["  ".join(cols)]
    for r in rows:
        out.append("  ".join(str(r[c]) for c in cols))
    return "\n".join(out)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("csv", nargs="+")
    p.add_argument("--ops-dir", required=True, help="dir holding <exp>/<tag>.ops.csv (results/raw)")
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    try:
        res = analyse(a.csv, Path(a.ops_dir))
    except plot.InputError as e:
        print("saturation.py: error: %s" % e, file=sys.stderr)
        return 2
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    _write(out / "saturation.csv", SAT_COLUMNS, res.sat)
    _write(out / "saturation_points.csv", POINT_COLUMNS, res.points)
    print(_table(SAT_COLUMNS, res.sat))
    print()
    print(_table(POINT_COLUMNS, res.points))
    for n in res.notes:
        print("note: " + n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
