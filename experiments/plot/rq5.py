#!/usr/bin/env python3
"""RQ5 (in-protocol root attestations, E9) metrics of every run.

Usage:
    python3 experiments/plot/rq5.py results/raw/e9-*.csv --raw-dir results/raw --out results/e9

E9 EXPs are named <prefix>-a<K>-<read> (K = ATTEST_EVERY, read = READ_MODE;
experiments/exp/e9_attest.sh). For every CSV row, from its run sidecars
(<raw-dir>/<exp>/<tag>[_bf..][_rate..].<ext>, written by run_one.sh):

* detect_protocol_s: epoch of the first "core: diverged at height" line of
  <tag>.diverged (docker log timestamp) minus the epoch of the fault
  injection (the first injection row of <tag>.faults, e.g. corrupt_on);
  empty, with a note, when there is no .diverged line.
* detect_auditor_s: seconds from the injection to the first <tag>.audit
  tick naming a divergent node (rq3.detect_latency); empty when none.
* wrong_gets: the CSV column (reads the client accepted with a corrupt value).
* rejected_gets: proof_errors + cert_errors + dataloss_errors of <tag>.ops.csv
  (reads the client refused, or a diverged node refused).
* tps (CSV) and write_p99_ms (the ops write line).

Writes <out>/rq5_runs.csv (one line per run) and <out>/rq5_summary.csv
(means per backend, attest_every, read_mode and fault kind), prints both
and the notes. Exit 0, or 2 on unusable input. Uses plot.py and rq3.py.
"""
import argparse
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import plot
import rq3

E9_EXP = re.compile(r"^(?P<prefix>.+)-a(?P<k>[0-9]+)-(?P<read>plain|verified|certified)$")
DOCKER_TS = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?Z")
DIVERGED = "core: diverged at height"

RUN_COLUMNS = ["exp", "backend", "attest_every", "read_mode", "fault", "rep", "fault_kind",
               "detect_protocol_s", "detect_auditor_s", "wrong_gets", "rejected_gets", "tps", "write_p99_ms"]
SUMMARY_COLUMNS = ["backend", "attest_every", "read_mode", "fault_kind", "runs", "detected_runs",
                   "detect_protocol_s", "detect_auditor_s", "wrong_gets", "rejected_gets", "tps", "write_p99_ms"]


class Result:
    def __init__(self):
        self.runs = []
        self.summary = []
        self.notes = []


def docker_epoch(line):
    """Epoch seconds of the first docker log timestamp in line, or None."""
    m = DOCKER_TS.search(line)
    if not m:
        return None
    frac = (m.group(2) or ".0")[1:7].ljust(6, "0")
    dt = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.timestamp() + int(frac) / 1e6


def first_diverged(path):
    """Epoch seconds of the first diverged line of a .diverged file, or None."""
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError, TypeError):
        return None
    ts = [docker_epoch(l) for l in lines if DIVERGED in l]
    ts = [t for t in ts if t is not None]
    return min(ts) if ts else None


def injection_epoch(path, fault):
    """Epoch seconds of the first injection row of a .faults file, or None."""
    if path is None:
        return None
    kind = fault.split(":", 1)[0]
    actions = rq3.INJECT_ACTIONS.get(kind, ())
    try:
        rows = rq3.read_faults(path)
    except rq3.InputError:
        return None
    for r in rows:
        if r["action"] in actions:
            try:
                return float(r["epoch_ms"]) / 1000.0
            except ValueError:
                return None
    return None


def _round(v, nd=3):
    return "" if v is None else round(v, nd)


def analyse(csv_paths, raw_dir):
    res = Result()
    df = plot.load_csvs(csv_paths)
    raw_dir = Path(raw_dir)
    for _, row in df.iterrows():
        m = E9_EXP.match(str(row["exp"]))
        if not m:
            res.notes.append("exp %s is not an E9 EXP (<prefix>-a<K>-<read>); row skipped" % row["exp"])
            continue
        fault = str(row["fault"])
        kind = fault.split(":", 1)[0] if fault != "none" else "none"
        ops = plot.read_ops(plot.run_sidecar(raw_dir, row, ".ops.csv"))
        rd, wr = ops.get("read", {}), ops.get("write", {})
        rejected = int(sum(rd.get(c, 0) for c in ("proof_errors", "cert_errors", "dataloss_errors")))
        out = {"exp": row["exp"], "backend": row["backend"], "attest_every": int(m.group("k")),
               "read_mode": m.group("read"), "fault": fault, "rep": row["rep"], "fault_kind": kind,
               "detect_protocol_s": "", "detect_auditor_s": "", "wrong_gets": int(row["wrong_gets"]),
               "rejected_gets": rejected, "tps": float(row["tps"]),
               "write_p99_ms": wr.get("lat_p99_ms", float("nan"))}
        if kind != "none":
            label = "%s %s rep %s" % (row["exp"], fault, row["rep"])
            inj = injection_epoch(plot.run_sidecar(raw_dir, row, ".faults"), fault)
            if inj is None:
                res.notes.append("%s: no injection row in .faults" % label)
            else:
                div = first_diverged(plot.run_sidecar(raw_dir, row, ".diverged"))
                if div is None:
                    if out["attest_every"] > 0:
                        res.notes.append("%s: no .diverged line (no in-protocol detection)" % label)
                else:
                    out["detect_protocol_s"] = _round(div - inj)
                audit = plot.run_sidecar(raw_dir, row, ".audit")
                if audit is not None:
                    try:
                        out["detect_auditor_s"] = _round(rq3.detect_latency(rq3.read_audit(audit), inj))
                    except rq3.InputError as e:
                        res.notes.append("%s: %s" % (label, e))
        res.runs.append(out)

    groups = {}
    for r in res.runs:
        groups.setdefault((r["backend"], r["attest_every"], r["read_mode"], r["fault_kind"]), []).append(r)

    def mean(rs, col):
        vals = [float(r[col]) for r in rs if r[col] != "" and not math.isnan(float(r[col]))]
        return round(sum(vals) / len(vals), 3) if vals else ""

    for (b, k, rdm, kind), rs in sorted(groups.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        res.summary.append({"backend": b, "attest_every": k, "read_mode": rdm, "fault_kind": kind,
                            "runs": len(rs), "detected_runs": sum(r["detect_protocol_s"] != "" for r in rs),
                            "detect_protocol_s": mean(rs, "detect_protocol_s"),
                            "detect_auditor_s": mean(rs, "detect_auditor_s"),
                            "wrong_gets": mean(rs, "wrong_gets"), "rejected_gets": mean(rs, "rejected_gets"),
                            "tps": mean(rs, "tps"), "write_p99_ms": mean(rs, "write_p99_ms")})
    return res


def _write(path, cols, rows):
    with open(path, "w") as fh:
        fh.write(",".join(cols) + "\n")
        for r in rows:
            fh.write(",".join(str(r[c]) for c in cols) + "\n")


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("csv", nargs="+")
    p.add_argument("--raw-dir", required=True, help="dir holding <exp>/<tag>.* (results/raw)")
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    try:
        res = analyse(a.csv, a.raw_dir)
    except plot.InputError as e:
        print("rq5.py: error: %s" % e, file=sys.stderr)
        return 2
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    _write(out / "rq5_runs.csv", RUN_COLUMNS, res.runs)
    _write(out / "rq5_summary.csv", SUMMARY_COLUMNS, res.summary)
    for cols, rows in ((SUMMARY_COLUMNS, res.summary), (RUN_COLUMNS, res.runs)):
        print("  ".join(cols))
        for r in rows:
            print("  ".join(str(r[c]) for c in cols))
        print()
    for n in res.notes:
        print("note: " + n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
