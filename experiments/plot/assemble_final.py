#!/usr/bin/env python3
"""Assemble the final data set (Task 19) from the campaign, re-run and r2 CSVs.

Usage:
    python3 experiments/plot/assemble_final.py results/raw results/final

Rules (rulings in experiments/NOTES.md, "Final data"):
* e1..e6: the campaign rows of results/raw/<exp>.csv.  For every
  configuration in results/raw/<exp>-rerun.csv (one fresh 3-rep pass of the
  configurations plot.py flagged): if the re-run's own CV of tps is <= 0.10
  its rows REPLACE the campaign rows of that configuration ("rerun"),
  otherwise they are ADDED to them ("pooled", reps renumbered after the
  campaign's last rep so run tags stay unique).
* e1-bf100ms: the whole sweep is results/raw/e1-bf100ms-r2.csv (re-measured
  in one block; the campaign sweep is off by up to 2x at 256 clients).
* Rows keep their CSV values; `exp` is rewritten to the final experiment
  name.  Each row's run sidecars (<tag>.ops.csv, .timeline, .faults, .audit,
  .wtimeline) are copied to <out>/<exp>/ under the row's final tag, so
  plot.py (write latency) and rq3_table.py read <out> like results/raw.
* <out>/assembly.txt lists every re-run decision.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plot import run_tag  # noqa: E402

KEYS = ["backend", "n", "f", "clients", "block_size", "workload", "rtt_ms", "fault", "commit_mode"]
SIDECARS = (".ops.csv", ".timeline", ".faults", ".audit", ".wtimeline")
CV_MAX = 0.10


def cv(s: pd.Series) -> float:
    s = s.astype(float)
    return float(s.std() / s.mean())


def _sidecar_base(raw: Path, row) -> Path | None:
    """Path prefix (without suffix) of the row's run files in raw/<exp>/."""
    d = raw / str(row["exp"])
    tag = run_tag({**row, "rep": row["_src_rep"]})
    if (d / (tag + ".timeline")).exists():
        return d / tag
    bf = sorted(d.glob(tag + "_bf*.timeline"))
    return d / bf[0].name[: -len(".timeline")] if len(bf) == 1 else None


def merge_rerun(orig: pd.DataFrame, rerun: pd.DataFrame, log: list) -> pd.DataFrame:
    out = orig.copy()
    for k, g in rerun.groupby(KEYS, sort=True):
        same = (out[KEYS] == pd.Series(k, index=KEYS)).all(axis=1)
        c = cv(g["tps"])
        if c <= CV_MAX:
            out = pd.concat([out[~same], g])
            how = "rerun"
        else:
            g = g.copy()
            last = out.loc[same, "rep"].astype(int).max()
            g["rep"] = (g["rep"].astype(int) + (0 if pd.isna(last) else last)).astype(str)
            out = pd.concat([out, g])
            how = "pooled"
        log.append("%s: rerun cv %.3f -> %s" % (" ".join("%s=%s" % kv for kv in zip(KEYS, k)), c, how))
    return out


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2:
        print("usage: assemble_final.py <raw dir> <out dir>", file=sys.stderr)
        return 2
    raw, out = Path(argv[0]), Path(argv[1])
    out.mkdir(parents=True, exist_ok=True)
    log = []
    plan = {e: [e, e + "-rerun"] for e in ("e1", "e2", "e3", "e4", "e5", "e6")}
    plan["e1-bf100ms"] = ["e1-bf100ms-r2"]
    for exp, srcs in plan.items():
        frames = [pd.read_csv(raw / (s + ".csv"), dtype=str) for s in srcs]
        for f in frames:
            f["_src_rep"] = f["rep"]
        log.append("== %s from %s" % (exp, ", ".join(s + ".csv" for s in srcs)))
        df = merge_rerun(frames[0], frames[1], log) if len(frames) == 2 else frames[0].copy()
        df = df.sort_values("ts")
        d = out / exp
        d.mkdir(exist_ok=True)
        missing = 0
        for _, row in df.iterrows():
            row = row.to_dict()
            src = _sidecar_base(raw, row)
            final = dict(row)
            final["exp"] = exp
            if src is None:
                missing += 1
                continue
            dst = d / (run_tag(final) + src.name[len(run_tag({**row, "rep": row["_src_rep"]})):])
            for suf in SIDECARS:
                p = Path(str(src) + suf)
                if p.exists():
                    shutil.copyfile(p, Path(str(dst) + suf))
        df["exp"] = exp
        df = df.drop(columns="_src_rep")
        df.to_csv(out / (exp + ".csv"), index=False)
        log.append("%s: %d rows, %d without sidecars" % (exp, len(df), missing))
    (out / "assembly.txt").write_text("\n".join(log) + "\n")
    print("\n".join(log))
    return 0


if __name__ == "__main__":
    sys.exit(main())
