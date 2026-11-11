#!/usr/bin/env python3
"""E9b (fault-free, interleaved attestation control): paired per-pass differences.

Usage:
    python3 experiments/plot/e9b_paired.py results/e9b/rq5_runs.csv --raw-dir results/raw --out results/e9b

E9b runs e9_attest.sh with E9_PREFIX=e9b, E9_FAULTS=none and REPS=5. The
driver interleaves the EXPs pass by pass (experiments/exp/interleave.sh), so
rep k of every configuration ran in the same pass, close in time. This script
reads the per-run rows of rq5.py (write_p50_ms, write_mean_ms, write_p99_ms,
read_p50_ms) and adds queue_block_ms: the mean, over the nodes, of
(queue_ns / queue_blocks) between the first and the last breakdown snapshot of
the run (<raw-dir>/<exp>/<tag>.breakdown/breakdown-node<i>.snap{1,2}.json):
how long a block waited for the apply loop during the measurement window.

Outputs:
    <out>/e9b_runs.csv     exp, backend, config (<attest>-<read>), rep, the
                           four latencies and queue_block_ms
    <out>/e9b_paired.csv   backend, metric, config, baseline, d_rep1..d_repN
                           (config minus baseline in the same pass), median,
                           positive (count of d > 0), reps
Pairs: a1-plain and a10-plain against a0-plain (the attestation itself), and
a1-certified / a10-certified against the plain run at the same attest level
(the certified reads). Standard library only.
"""

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path

METRICS = ("write_p50_ms", "write_mean_ms", "write_p99_ms", "read_p50_ms", "queue_block_ms")
PAIRS = (("1-plain", "0-plain"), ("10-plain", "0-plain"),
         ("1-certified", "1-plain"), ("10-certified", "10-plain"))
RUN_COLUMNS = ["exp", "backend", "config", "rep"] + list(METRICS)


class InputError(ValueError):
    pass


def queue_block_ms(bdir):
    """Mean over nodes of the queue time per block (ms) between snap1 and snap2, or None."""
    vals = []
    for s1 in sorted(Path(bdir).glob("breakdown-node*.snap1.json")):
        s2 = s1.with_name(s1.name.replace(".snap1.", ".snap2."))
        try:
            a = json.loads(s1.read_text())
            z = json.loads(s2.read_text())
        except (OSError, ValueError):
            continue
        nb = z.get("queue_blocks", 0) - a.get("queue_blocks", 0)
        if nb > 0:
            vals.append((z["queue_ns"] - a["queue_ns"]) / nb / 1e6)
    return statistics.mean(vals) if vals else None


def breakdown_dir(raw_dir, exp, backend, rep):
    hits = sorted((Path(raw_dir) / exp).glob("%s_*_r%s_*.breakdown" % (backend, rep)))
    hits += sorted((Path(raw_dir) / exp).glob("%s_*_r%s.breakdown" % (backend, rep)))
    return hits[0] if len(hits) == 1 else None


def load_runs(path, raw_dir):
    try:
        rows = list(csv.DictReader(open(path, newline="")))
    except FileNotFoundError:
        raise InputError("%s: no such file" % path)
    out, notes = [], []
    for r in rows:
        if r.get("fault_kind", "none") not in ("none", ""):
            continue
        cfg = "%s-%s" % (r["attest_every"], r["read_mode"])
        d = {"exp": r["exp"], "backend": r["backend"], "config": cfg, "rep": int(r["rep"])}
        for m in METRICS[:4]:
            try:
                d[m] = float(r[m])
            except (KeyError, ValueError):
                d[m] = None
        bd = breakdown_dir(raw_dir, r["exp"], r["backend"], r["rep"])
        d["queue_block_ms"] = queue_block_ms(bd) if bd else None
        if bd is None:
            notes.append("%s %s rep %s: no single .breakdown dir; queue_block_ms empty" % (r["exp"], r["backend"], r["rep"]))
        out.append(d)
    return out, notes


def paired(runs):
    by = {(r["backend"], r["config"], r["rep"]): r for r in runs}
    reps = sorted({r["rep"] for r in runs})
    out = []
    for b in sorted({r["backend"] for r in runs}):
        for m in METRICS:
            for cfg, base in PAIRS:
                ds = []
                for k in reps:
                    a, z = by.get((b, base, k)), by.get((b, cfg, k))
                    if a is None or z is None or a[m] is None or z[m] is None:
                        ds.append(None)
                    else:
                        ds.append(z[m] - a[m])
                got = [d for d in ds if d is not None]
                if not got:
                    continue
                row = {"backend": b, "metric": m, "config": cfg, "baseline": base}
                for k, d in zip(reps, ds):
                    row["d_rep%d" % k] = "" if d is None else round(d, 3)
                row.update(median=round(statistics.median(got), 3), positive=sum(1 for d in got if d > 0), reps=len(got))
                out.append(row)
    return out, ["d_rep%d" % k for k in reps]


def _write(path, cols, rows):
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: ("" if r.get(c) is None else (round(r[c], 3) if isinstance(r[c], float) else r[c])) for c in cols})


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("runs", help="rq5_runs.csv of the E9b EXPs")
    p.add_argument("--raw-dir", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    try:
        runs, notes = load_runs(a.runs, a.raw_dir)
    except InputError as e:
        print("e9b_paired.py: error: %s" % e, file=sys.stderr)
        return 2
    pr, dcols = paired(runs)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    _write(out / "e9b_runs.csv", RUN_COLUMNS, sorted(runs, key=lambda r: (r["backend"], r["config"], r["rep"])))
    cols = ["backend", "metric", "config", "baseline"] + dcols + ["median", "positive", "reps"]
    _write(out / "e9b_paired.csv", cols, pr)
    for r in pr:
        print("  ".join(str(r.get(c, "")) for c in cols))
    for n in notes:
        print("note: " + n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
