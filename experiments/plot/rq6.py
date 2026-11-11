#!/usr/bin/env python3
"""RQ6/RQ7 (phase 3: accountability, read-path faults, attestation quorum) metrics of every run.

Usage:
    python3 experiments/plot/rq6.py results/raw/e1[012]-*.csv --raw-dir results/raw --out results/e1x

E10-E12 EXPs (experiments/exp/e10_readcost.sh, e11_account.sh,
e12_quorum.sh) encode their configuration in the EXP name; the summary groups
by EXP, backend and fault kind. For every CSV row, from its run sidecars
(<raw-dir>/<exp>/<tag>[_bf..][_rate..].<ext>, written by run_one.sh):

* detect_evidence_s: first "core: evicted node" line of <tag>.evict (docker
  log timestamp, any node) minus the injection (first injection row of
  <tag>.faults); empty, with a note, when there is none.
* evict_all_s: injection -> the time every honest node (N from the row,
  faulty nodes from the FAULT column) has logged the eviction of every
  faulty node; empty when that never happens.
* same_height: True when every node logged each culprit's eviction at the
  same height (the eviction is decided in the apply loop, so it must be).
* failover_s: first_failover_unix_ms of the read line of <tag>.ops.csv (the
  client's first move of any reader to another node) minus the injection;
  empty when the run had no failover or no injection, or (with a note) when
  the first failover came before the injection, or when the sidecar has no
  such column (an older ycsbbench) although the run failed over.
* wrong_gets: the CSV column (corrupt values the client accepted).
* rejected_reads, evidence_sent, evidence_failed, cert_timeouts: the read line of
  <tag>.ops.csv (rejected_reads falls back to proof_errors + cert_errors +
  dataloss_errors of phase 2; a missing column is left empty).
* injection_race: 1 for an F5 run at quorum 2f+1 (EXP with "-q2f1-", or
  an EXP of E11 at the default quorum) in which some but not all of the f+1
  faulty nodes were evicted: quorum 2f+1 certifies a height only if at most
  f nodes were faulty when it was attested, so this means the injections
  took effect at different heights (SIGUSR1 is not synchronised by height).
  Such runs are listed with a note and left out of the summary (race_runs
  counts them). All f+1 faulty nodes evicted at quorum 2f+1 is not a race:
  it gets a WARNING note (check the Certifier) and stays in the summary.
* honest_evicted: distinct nodes outside FAULT that some node evicted.
* wrong_root_heights: distinct heights at which an honest node was evicted
  for divergent_root, i.e. heights whose certified root was a faulty one
  (a culprit's root that differs from a certified honest root evicts the
  culprit instead); certs_heights: distinct heights of <tag>.certs.

Writes <out>/rq6_runs.csv and <out>/rq6_summary.csv (means per EXP, backend
and fault kind), prints both and the notes. Exit 0, or 2 on unusable input.

Standard library only (plus rq3.py), so it runs on the experiment host
without pandas; run_tag / run_sidecar mirror plot.py's.
"""
import argparse
import csv
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import rq3

EXPECTED_COLUMNS = [
    "ts", "commit", "exp", "backend", "n", "f", "clients", "block_size",
    "workload", "rtt_ms", "fault", "rep", "commit_mode", "tps", "lat_mean_ms",
    "lat_p50_ms", "lat_p99_ms", "errors", "wrong_gets", "host_cpu_max_pct",
    "saturated", "net_bytes_per_block", "offered_rps",
]  # plot.EXPECTED_COLUMNS
OLD_COLUMNS = EXPECTED_COLUMNS[:-1]
DOCKER_TS = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?Z")
EVICT = re.compile(r"core: evicted node (\d+) at height (\d+) \(([a-z_]+)\)")
CERT = re.compile(r"core: certified height (\d+) root ([0-9a-f]+)")
FAULT_NODES = re.compile(r"^[A-Z0-9-]+:([^@]+)@")

RUN_COLUMNS = ["exp", "backend", "n", "fault", "rep", "fault_kind", "injection_race", "detect_evidence_s", "evict_all_s",
               "same_height", "failover_s", "wrong_gets", "rejected_reads", "evidence_sent", "evidence_failed",
               "cert_timeouts", "honest_evicted", "wrong_root_heights", "certs_heights", "tps"]
SUMMARY_COLUMNS = ["exp", "backend", "fault_kind", "runs", "race_runs", "detected_runs", "evicted_all_runs",
                   "detect_evidence_s", "evict_all_s", "failover_s", "wrong_gets", "rejected_reads",
                   "evidence_sent", "evidence_failed", "cert_timeouts", "honest_evicted", "wrong_root_heights",
                   "tps"]


class InputError(ValueError):
    """Unusable input file (missing, empty, wrong header)."""


class Result:
    def __init__(self):
        self.runs = []
        self.summary = []
        self.notes = []


# ---- input -----------------------------------------------------------------

def load_rows(paths):
    """Rows (dicts) of the result CSVs; 22-column phase-1 files get offered_rps 0."""
    rows = []
    for p in paths:
        try:
            with open(p, newline="", encoding="utf-8-sig") as fh:
                rd = csv.reader(fh)
                header = next(rd, None)
                if header is None:
                    raise InputError("%s: empty CSV" % p)
                if header not in (EXPECTED_COLUMNS, OLD_COLUMNS):
                    raise InputError("%s: unexpected CSV header (%d columns)" % (p, len(header)))
                for rec in rd:
                    if not rec:
                        continue
                    if len(rec) != len(header):
                        raise InputError("%s: a row has %d fields, want %d" % (p, len(rec), len(header)))
                    row = dict(zip(header, rec))
                    row.setdefault("offered_rps", "0")
                    rows.append(row)
        except FileNotFoundError:
            raise InputError("%s: no such file (unmatched glob?)" % p)
    if not rows and not paths:
        raise InputError("no input CSV files")
    return rows


def _cell(v):
    try:
        f = float(v)
        if f.is_integer():
            return str(int(f))
    except (TypeError, ValueError):
        pass
    return str(v)


def run_tag(row):
    """run_one.sh's run tag of a CSV row (plot.run_tag)."""
    tag = "%s_n%s_f%s_c%s_b%s_w%s_rtt%s_%s_%s_r%s" % tuple(
        _cell(row[c]) for c in ("backend", "n", "f", "clients", "block_size", "workload",
                                "rtt_ms", "fault", "commit_mode", "rep"))
    return re.sub(r"[^A-Za-z0-9._-]", "_", tag)


def rate_suffix(row):
    """The "_rate<RATE>" tag suffix of an open-loop row (plot.rate_suffix)."""
    try:
        r = float(row.get("offered_rps", 0) or 0)
    except (TypeError, ValueError):
        return ""
    if r <= 0 or math.isnan(r):
        return ""
    return "_rate" + _cell(r)


def run_sidecar(base, row, ext):
    """<base>/<exp>/<tag>[_*]<rate><ext> of a row, or None (plot.run_sidecar)."""
    d = Path(base) / str(row["exp"])
    tag = run_tag(row)
    rs = rate_suffix(row)
    p = d / (tag + rs + ext)
    if p.exists():
        return p
    hits = sorted(d.glob(tag + "_*" + rs + ext))
    if not rs:
        hits = [h for h in hits if "_rate" not in h.name[len(tag):]]
    return hits[0] if len(hits) == 1 else None


def read_ops_line(path, op):
    """{column: str} of one line (op = read or write) of an ops sidecar, {} when missing."""
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError, TypeError):
        return {}
    if not lines:
        return {}
    head = lines[0].split(",")
    for line in lines[1:]:
        f = line.split(",")
        if f and f[0] == op and len(f) == len(head):
            return dict(zip(head, f))
    return {}


def docker_epoch(line):
    """Epoch seconds of the first docker log timestamp in line, or None."""
    m = DOCKER_TS.search(line)
    if not m:
        return None
    frac = (m.group(2) or ".0")[1:7].ljust(6, "0")
    dt = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.timestamp() + int(frac) / 1e6


def read_evictions(path):
    """[(reporter node, epoch, culprit node, height, reason)] of an .evict file."""
    out = []
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError, TypeError):
        return out
    for line in lines:
        m = EVICT.search(line)
        ts = docker_epoch(line)
        if not m or ts is None or not line.split():
            continue
        out.append((line.split()[0], ts, "node" + m.group(1), int(m.group(2)), m.group(3)))
    return out


def read_cert_heights(path):
    out = set()
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError, TypeError):
        return out
    for line in lines:
        m = CERT.search(line)
        if m:
            out.add(int(m.group(1)))
    return out


def injection_epoch(path, fault):
    """Epoch seconds of the first injection row of a .faults file, or None."""
    if path is None:
        return None
    actions = rq3.INJECT_ACTIONS.get(fault.split(":", 1)[0], ())
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


def faulty_nodes(fault):
    """Nodes named by a FAULT value ("F6:node4@60", "F5-F4:node4+node3@60")."""
    m = FAULT_NODES.match(fault)
    if not m:
        return []
    return [x for x in re.split(r"[+,]", m.group(1)) if x]


# ---- analysis ----------------------------------------------------------------

def _round(v, nd=3):
    return "" if v is None else round(v, nd)


def _int_or_empty(d, col):
    try:
        return int(float(d[col]))
    except (KeyError, ValueError):
        return ""


def eviction_metrics(evictions, n, culprits, inj):
    """(detect_evidence_s, evict_all_s, same_height, honest_evicted, wrong_root_heights)."""
    nodes = ["node%d" % i for i in range(1, n + 1)]
    honest = [x for x in nodes if x not in culprits]
    detect = None
    if evictions and inj is not None:
        detect = min(ts for _, ts, _, _, _ in evictions) - inj
    evict_all = None
    if culprits and inj is not None:
        first = {}  # (reporter, culprit) -> earliest ts
        for rep, ts, cul, _, _ in evictions:
            k = (rep, cul)
            first[k] = min(ts, first.get(k, ts))
        need = [(h, c) for h in honest for c in culprits]
        if need and all(k in first for k in need):
            evict_all = max(first[k] for k in need) - inj
    heights = {}
    for _, _, cul, h, _ in evictions:
        heights.setdefault(cul, set()).add(h)
    same = all(len(hs) == 1 for hs in heights.values()) if heights else ""
    honest_evicted = len({cul for _, _, cul, _, _ in evictions if cul not in culprits})
    wrong_heights = len({h for _, _, cul, h, why in evictions if cul not in culprits and why == "divergent_root"})
    return detect, evict_all, same, honest_evicted, wrong_heights


def analyse(csv_paths, raw_dir):
    res = Result()
    raw_dir = Path(raw_dir)
    for row in load_rows(csv_paths):
        fault = str(row["fault"])
        kind = fault.split(":", 1)[0] if fault != "none" else "none"
        try:
            n = int(float(row["n"]))
        except ValueError:
            raise InputError("%s: bad n %r" % (row["exp"], row["n"]))
        label = "%s %s %s rep %s" % (row["exp"], row["backend"], fault, row["rep"])
        culprits = faulty_nodes(fault) if kind != "none" else []
        rd = read_ops_line(run_sidecar(raw_dir, row, ".ops.csv"), "read")
        if "rejected_reads" in rd:
            rejected = _int_or_empty(rd, "rejected_reads")
        elif rd:
            rejected = sum(int(float(rd.get(c) or 0)) for c in ("proof_errors", "cert_errors", "dataloss_errors"))
        else:
            rejected = ""
        inj = None
        if kind != "none":
            inj = injection_epoch(run_sidecar(raw_dir, row, ".faults"), fault)
            if inj is None:
                res.notes.append("%s: no injection row in .faults" % label)
        ev_path = run_sidecar(raw_dir, row, ".evict")
        evictions = read_evictions(ev_path)
        detect, evict_all, same, honest_ev, wrong_h = eviction_metrics(evictions, n, culprits, inj)
        if kind != "none" and not evictions:
            res.notes.append("%s: no .evict line (no node was evicted)" % label)
        race = 0
        if kind.startswith("F5") and "-qf1-" not in str(row["exp"]):
            caught = {cul for _, _, cul, _, _ in evictions if cul in culprits}
            if caught and caught == set(culprits):
                res.notes.append("WARNING: %s: all f+1 faulty nodes (%s) evicted at quorum 2f+1: a height was "
                                 "certified against f+1 faulty nodes, which quorum 2f+1 must prevent; check the "
                                 "Certifier (kept in the summary)" % (label, "+".join(sorted(caught))))
            elif caught:
                race = 1
                res.notes.append("%s: only %s of the f+1 faulty nodes evicted at quorum 2f+1: the injections took "
                                 "effect at different heights (injection race); left out of the summary"
                                 % (label, "+".join(sorted(caught))))
        failover = None
        first_fo = _int_or_empty(rd, "first_failover_unix_ms")
        if first_fo and inj is not None:
            failover = first_fo / 1000.0 - inj
            if failover < 0:
                res.notes.append("%s: first failover %.3f s before the injection; failover_s left empty"
                                 % (label, -failover))
                failover = None
        elif first_fo == "" and _int_or_empty(rd, "failovers"):
            res.notes.append("%s: failover_s not recorded (ops sidecar without first_failover_unix_ms)" % label)
        res.runs.append({
            "exp": row["exp"], "backend": row["backend"], "n": n, "fault": fault, "rep": row["rep"],
            "fault_kind": kind, "injection_race": race, "detect_evidence_s": _round(detect), "evict_all_s": _round(evict_all),
            "same_height": same, "failover_s": _round(failover), "wrong_gets": int(float(row["wrong_gets"] or 0)),
            "rejected_reads": rejected, "evidence_sent": _int_or_empty(rd, "evidence_sent"),
            "evidence_failed": _int_or_empty(rd, "evidence_failed"),
            "cert_timeouts": _int_or_empty(rd, "cert_timeouts"), "honest_evicted": honest_ev,
            "wrong_root_heights": wrong_h, "certs_heights": len(read_cert_heights(run_sidecar(raw_dir, row, ".certs"))),
            "tps": float(row["tps"] or "nan"),
        })

    groups = {}
    for r in res.runs:
        groups.setdefault((r["exp"], r["backend"], r["fault_kind"]), []).append(r)

    def mean(rs, col):
        vals = []
        for r in rs:
            try:
                v = float(r[col])
            except (TypeError, ValueError):
                continue
            if not math.isnan(v):
                vals.append(v)
        return round(sum(vals) / len(vals), 3) if vals else ""

    for (exp, b, kind), all_rs in sorted(groups.items()):
        rs = [r for r in all_rs if not r["injection_race"]]
        res.summary.append({
            "exp": exp, "backend": b, "fault_kind": kind, "runs": len(rs), "race_runs": len(all_rs) - len(rs),
            "detected_runs": sum(r["detect_evidence_s"] != "" for r in rs),
            "evicted_all_runs": sum(r["evict_all_s"] != "" for r in rs),
            **{c: mean(rs, c) for c in ("detect_evidence_s", "evict_all_s", "failover_s", "wrong_gets", "rejected_reads",
                                       "evidence_sent", "evidence_failed", "cert_timeouts", "honest_evicted",
                                       "wrong_root_heights", "tps")},
        })
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
    except (InputError, rq3.InputError) as e:
        print("rq6.py: error: %s" % e, file=sys.stderr)
        return 2
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    _write(out / "rq6_runs.csv", RUN_COLUMNS, res.runs)
    _write(out / "rq6_summary.csv", SUMMARY_COLUMNS, res.summary)
    for cols, rows in ((SUMMARY_COLUMNS, res.summary), (RUN_COLUMNS, res.runs)):
        print("  ".join(cols))
        for r in rows:
            print("  ".join(str(r[c]) for c in cols))
        print()
    for note in res.notes:
        print("note: " + note)
    return 0


if __name__ == "__main__":
    sys.exit(main())
