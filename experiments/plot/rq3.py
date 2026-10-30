#!/usr/bin/env python3
"""RQ3 (behaviour under faults) metrics of one run.

Usage:
    python3 experiments/plot/rq3.py results/raw/<EXP>/<tag>

prints key=value lines for <tag>.timeline, <tag>.faults and (when present)
<tag>.audit; exit status 2 and one line on stderr for unusable input.
Pure standard library (no pandas).

Inputs (all written by experiments/run_one.sh)
----------------------------------------------
* <tag>.timeline: ycsbbench --timeline, one number per line = successful
  requests in that second, line 0 = the first second of the run phase
  (INCLUDING the warmup).  Metrics here use only this, never the CSV `errors`
  column: clients bound to a stopped node fail fast in a loop, which inflates
  `errors` (about 190k in a 40 s F1 run) without saying anything about the
  cluster.
* <tag>.faults: CSV `utc,epoch_ms,t_s,timeline_s,fault,action,node,detail`
  (experiments/faults.sh).  The fault time is `timeline_s` of the first
  injection row (action docker_stop, iptables_drop or netem_delay: logged just
  BEFORE the command runs).  The `run_phase_start` row (timeline_s 0) gives the
  epoch of timeline second 0.
* <tag>.audit: `unix_ts,nodes` per second (cmd/auditor --interval): `nodes` is
  empty (consistent), or '+'-joined `nodeK` (divergent) and `down:nodeK`
  (unreachable, never divergent).  unix_ts is on the clock of the client
  container, the .faults epoch_ms on the clock of the host that runs
  run_one.sh; both are read as the same clock (a skew shifts detect_s).

Conventions
-----------
* All times are seconds on the timeline axis (0 = start of the run phase, so
  the measurement window starts at warmup_s).  fault_at may be fractional:
  second `i` of the timeline covers [i, i+1), so seconds before ceil(fault_at)
  mix pre-fault and post-fault traffic and are skipped by recovery_time and
  stall_seconds.
* pre_mean = mean throughput of the full seconds between the end of the warmup
  and the fault; None when there are none (AT=0), then recovery_s is None.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

RECOVERY_FRACTION = 0.9  # recovered = throughput >= 0.9 x pre-fault mean
FAULTS_COLUMNS = ["utc", "epoch_ms", "t_s", "timeline_s", "fault", "action", "node", "detail"]
# faults.sh actions logged right before a fault is applied (the *_failed and
# the "applied"/"stopped" completion rows are not injections).
INJECT_ACTIONS = ("docker_stop", "iptables_drop", "netem_delay")
RUN_START_ACTION = "run_phase_start"
DOWN_PREFIX = "down:"  # unreachable-node token of the audit file


class InputError(ValueError):
    """Unusable input file (missing, wrong header, unparsable)."""


# ---- the metrics ------------------------------------------------------------

def recovery_time(timeline, fault_at, pre_mean):
    """Seconds after the fault until throughput is back to >= 0.9 x pre_mean.

    Looks at the full seconds from ceil(fault_at) on; returns
    (index of the first second with throughput >= 0.9 * pre_mean) - fault_at,
    or None if there is none (or pre_mean is None or not positive).  A fault
    that never lowers throughput gives a value below 1 (0 for an integer
    fault_at).
    """
    if pre_mean is None or not pre_mean > 0:
        return None
    need = RECOVERY_FRACTION * pre_mean - 1e-9
    for i in range(max(int(math.ceil(fault_at)), 0), len(timeline)):
        if timeline[i] >= need:
            return float(i - fault_at)
    return None


def stall_seconds(timeline, fault_at):
    """Number of consecutive seconds with throughput 0 from ceil(fault_at)."""
    n = 0
    for i in range(max(int(math.ceil(fault_at)), 0), len(timeline)):
        if timeline[i] != 0:
            break
        n += 1
    return n


def divergent_nodes(field):
    """Divergent nodes of an audit `nodes` field (the down: ones excluded)."""
    return [t for t in field.split("+") if t and not t.startswith(DOWN_PREFIX)]


def unreachable_nodes(field):
    """Unreachable nodes of an audit `nodes` field, without the down: prefix."""
    return [t[len(DOWN_PREFIX):] for t in field.split("+") if t.startswith(DOWN_PREFIX)]


def detect_latency(audit_rows, fault_at):
    """Seconds from the fault to the first audit tick that lists a divergent node.

    audit_rows: iterable of (ts, nodes) on the same time axis as fault_at;
    only rows with ts >= fault_at count, and an unreachable node (down:) is not
    a divergent one.  None if no tick after the fault reports a divergence.
    """
    for ts, nodes in audit_rows:
        if ts >= fault_at and divergent_nodes(nodes):
            return ts - fault_at
    return None


def unreachable_latency(audit_rows, fault_at):
    for ts, nodes in audit_rows:
        if ts >= fault_at and unreachable_nodes(nodes):
            return ts - fault_at
    return None


# ---- readers ----------------------------------------------------------------

def _read_text(path):
    p = Path(path)
    try:
        return p.read_text(encoding="utf-8-sig")
    except FileNotFoundError:
        raise InputError("%s: no such file" % p)
    except OSError as e:
        raise InputError("%s: cannot read (%s)" % (p, e.strerror or e))
    except UnicodeDecodeError:
        raise InputError("%s: not a UTF-8 file" % p)


def read_timeline(path):
    """Throughput per second: one number per line (blank lines ignored)."""
    out = []
    for no, line in enumerate(_read_text(path).splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(float(line))
        except ValueError:
            raise InputError("%s:%d: not a number: %r" % (path, no, line))
    return out


def read_faults(path):
    """Rows of a .faults file as dicts keyed by FAULTS_COLUMNS."""
    text = _read_text(path)
    lines = text.splitlines()
    if not lines or lines[0].strip().split(",") != FAULTS_COLUMNS:
        raise InputError("%s: unexpected header (want %s)" % (path, ",".join(FAULTS_COLUMNS)))
    rows = []
    for no, rec in enumerate(csv.reader(lines[1:]), 2):
        if not rec:
            continue
        if len(rec) != len(FAULTS_COLUMNS):
            raise InputError("%s:%d: %d fields, want %d" % (path, no, len(rec), len(FAULTS_COLUMNS)))
        rows.append(dict(zip(FAULTS_COLUMNS, rec)))
    return rows


def read_audit(path):
    """Rows of an .audit file as (unix_ts float, nodes str); blank lines skipped."""
    out = []
    for no, line in enumerate(_read_text(path).splitlines(), 1):
        if not line.strip():
            continue
        parts = line.strip().split(",")
        if len(parts) != 2:
            raise InputError("%s:%d: want unix_ts,nodes, got %r" % (path, no, line))
        try:
            out.append((float(parts[0]), parts[1]))
        except ValueError:
            raise InputError("%s:%d: bad unix_ts %r" % (path, no, parts[0]))
    return out


# ---- one run ----------------------------------------------------------------

def _num(row, col, path):
    try:
        return float(row[col])
    except ValueError:
        raise InputError("%s: bad %s %r in the %s row" % (path, col, row[col], row["action"]))


def load_run(timeline_path, faults_path, audit_path=None):
    """RQ3 metrics of one run (a dict, see main for the keys).

    fault_at_s = timeline_s of the first (earliest) injection row of the .faults
    file; the audit timestamps are moved onto the timeline axis with the epoch
    of timeline second 0 (epoch_ms of the run_phase_start row, else derived
    from the injection row: epoch_ms - timeline_s * 1000).
    """
    timeline = read_timeline(timeline_path)
    rows = read_faults(faults_path)
    inject = [r for r in rows if r["action"] in INJECT_ACTIONS and r["timeline_s"] != ""]
    if not inject:
        raise InputError("%s: no injection row (%s) with a timeline_s" % (faults_path, "/".join(INJECT_ACTIONS)))
    first = min(inject, key=lambda r: _num(r, "timeline_s", faults_path))
    fault_at = _num(first, "timeline_s", faults_path)
    warmup = int(round(fault_at - _num(first, "t_s", faults_path)))
    start = [r for r in rows if r["action"] == RUN_START_ACTION and r["epoch_ms"] != ""]
    if start:
        origin_s = _num(start[0], "epoch_ms", faults_path) / 1000.0 - (
            _num(start[0], "timeline_s", faults_path) if start[0]["timeline_s"] != "" else 0.0)
    else:
        origin_s = (_num(first, "epoch_ms", faults_path) - fault_at * 1000.0) / 1000.0

    window = timeline[warmup:int(math.floor(fault_at))]
    pre_mean = sum(window) / len(window) if window else None
    audit = None
    if audit_path is not None:
        audit = [(ts - origin_s, nodes) for ts, nodes in read_audit(audit_path)]

    def r3(v):
        return None if v is None else round(v, 3)

    return {
        "fault": first["fault"],
        "fault_at_s": r3(fault_at),
        "warmup_s": warmup,
        "pre_mean_tps": r3(pre_mean),
        "recovery_s": r3(recovery_time(timeline, fault_at, pre_mean)),
        "stall_s": stall_seconds(timeline, fault_at),
        "detect_s": None if audit is None else r3(detect_latency(audit, fault_at)),
        "unreachable_s": None if audit is None else r3(unreachable_latency(audit, fault_at)),
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="RQ3 metrics of one run (see the module docstring).")
    ap.add_argument("prefix", help="results/raw/<EXP>/<tag>: reads <prefix>.timeline, .faults and .audit")
    args = ap.parse_args(argv)
    audit = Path(args.prefix + ".audit")
    try:
        m = load_run(args.prefix + ".timeline", args.prefix + ".faults", audit if audit.exists() else None)
    except InputError as e:
        print("rq3: %s" % e, file=sys.stderr)
        return 2
    for k, v in m.items():
        print("%s=%s" % (k, "" if v is None else v))
    return 0


if __name__ == "__main__":
    sys.exit(main())
