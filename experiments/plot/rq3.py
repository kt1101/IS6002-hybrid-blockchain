#!/usr/bin/env python3
"""RQ3 (behaviour under faults) metrics of one run.

Usage:
    python3 experiments/plot/rq3.py results/raw/<EXP>/<tag>

prints key=value lines for <tag>.timeline, <tag>.faults and (when present)
<tag>.wtimeline and <tag>.audit; exit status 2 and one line on stderr for unusable input.
Pure standard library (no pandas).

Inputs (all written by experiments/run_one.sh)
----------------------------------------------
* <tag>.timeline: ycsbbench --timeline, one number per line = successful
  requests in that second, line 0 = the first second of the run phase
  (INCLUDING the warmup).  Metrics here use only this, never the CSV `errors`
  column: clients bound to a stopped node fail fast in a loop, which inflates
  `errors` (about 190k in a 40 s F1 run) without saying anything about the
  cluster.
* <tag>.wtimeline: ycsbbench --wtimeline, the same for successful Sets
  (writes) only, same seconds as .timeline.  Reads are served by each node's
  local store and keep succeeding after consensus has stopped (a client with a
  pending write still completes reads between two write timeouts), so the
  total timeline hides a loss of liveness.  When .wtimeline is usable (it
  exists, parses, is not empty and has as many seconds as .timeline) EVERY
  throughput / liveness metric below is computed on it and `timeline_source`
  is `write`; otherwise on .timeline and `timeline_source` is `total` (a
  warning says why when the file exists but is unusable).  The key names stay
  (pre_mean_tps, min_after_fault_tps, ...): "tps" then means successful
  writes per second, so the two sources are not comparable in absolute tps.
* <tag>.faults: CSV `utc,epoch_ms,t_s,timeline_s,fault,action,node,detail`
  (experiments/faults.sh).  The fault time is `timeline_s` of the first
  injection row (INJECT_ACTIONS, e.g. docker_stop: logged just BEFORE the
  command runs).  The `run_phase_start` row (timeline_s 0) gives the epoch of
  timeline second 0; it is written when the host SEES the marker in the bench
  log (0.1 s poll + log relay), so timeline_s/fault_at_s are up to ~0.3 s too
  large against the true timeline (below the 1 s resolution; inherited from
  faults.sh).
* <tag>.audit: `unix_ts,nodes` per second (cmd/auditor --interval): `nodes` is
  empty (consistent), or '+'-joined `nodeK` (divergent) and `down:nodeK`
  (unreachable, never divergent).  unix_ts is on the clock of the client
  container, the .faults epoch_ms on the clock of the host that runs
  run_one.sh; both are read as the same clock (a skew shifts detect_s).
  load_run warns when the first tick is not within [-0.1, 5] s of the
  run_phase_start epoch (the auditor starts ~0.3-1 s after the marker), which
  is how a Docker Desktop clock skew shows up.

Conventions
-----------
* All times are seconds on the timeline axis (0 = start of the run phase, so
  the measurement window starts at warmup_s).  fault_at may be fractional:
  second `i` of the timeline covers [i, i+1), so seconds before ceil(fault_at)
  mix pre-fault and post-fault traffic and are skipped by recovery_time and
  stall_seconds.
* "timeline" below is the series picked by timeline_source (write or total).
* pre_mean = mean throughput of the full seconds between the end of the warmup
  and the fault; None when there are none (AT=0), then recovery_s is None.

Loader output (load_run / CLI), all times in seconds relative to fault_at_s
unless named otherwise
------------------------------------------------------------------------
* timeline_source: write | total, the series all throughput metrics come from.
* Recovery threshold: 0.9 x pre_mean x share, share = (N - k) / N for a
  fault that takes k of the N nodes out of the cluster (F1 `docker_stop`,
  network F2 / F5-F2 `iptables_drop`; k = the distinct nodes of those rows),
  else share = 1.  The closed-loop clients are bound statically to nodes
  (driver j -> node j mod N), so the clients of a stopped or partitioned node
  produce ~0 tps whatever the protocol does, and post-fault throughput is
  capped near share x pre_mean in the latency-bound regime; without the
  factor `recovered` would be unreachable by construction.  Kafka F2
  (silent_on: votes withheld, the node keeps serving) and F3/F4 keep
  share = 1.  N comes from the `_n<N>_` part of the run tag (main) or the n
  argument of load_run; unknown N -> share = 1.  Reported as
  recovery_threshold_tps (with threshold_share, faulty_nodes, n).
* The effect of a fault can lag the injection row (a graceful `docker stop`,
  client timeouts), so the headline numbers are measured from the DIP, the
  first full second after the fault below the recovery threshold:
    dip_start_s            that second minus fault_at (None: no dip)
    min_after_fault_tps    lowest per-second throughput after the fault
    recovery_s             first second >= the threshold AFTER the dip, minus
                           fault_at; recovery_from_dip_s = recovery_s - dip_start_s
    recovery_status        recovered | no_dip | not_recovered | no_baseline
                           (no_dip: throughput never fell below the threshold,
                           so there is nothing to recover from)
    stall_s                consecutive zero seconds starting at the first zero
                           second after the fault (0: no second with 0 tps)
  recovery_time()/stall_seconds() keep their plain "from the fault" meaning
  (the brief's), selectable per call with after_dip / from_first_zero.
* detect_s needs a guard because the audited digest can lag a block on a
  healthy node (a one-tick false divergence) and the auditor can die early:
    audit_ticks    number of audit lines
    audit_last_s   last tick minus fault_at
    audit_div_pre  ticks BEFORE the fault that listed a divergent node
    detect_status  detected | undetected | no_audit | invalid: <reason>
  detect_s is set only for `detected`: the first post-fault tick with a
  divergent node, and either audit_div_pre == 0, or the divergence persists
  (a divergent node of that tick is divergent again in the next tick).
  `undetected` (a finding for F4) needs the auditor to have run until within
  AUDIT_END_SLACK_S of the end of the timeline, else it is `invalid`.
* warnings: ';'-joined text (empty = none), also on stderr in the CLI.
"""
from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from pathlib import Path

RECOVERY_FRACTION = 0.9  # recovered = throughput >= 0.9 x pre-fault mean (x share)
# Injection actions that take their nodes out of the cluster (their clients
# then produce ~0 tps): F1 docker stop, network-level F2 / F5-F2.
OUT_ACTIONS = ("docker_stop", "iptables_drop")
FAULTS_COLUMNS = ["utc", "epoch_ms", "t_s", "timeline_s", "fault", "action", "node", "detail"]
# faults.sh actions logged right before a fault is applied, per fault kind (the
# *_failed and the "applied"/"stopped" completion rows are not injections).
# silent_on / corrupt_on: the SIGUSR1 that activates veritas --fault=silent
# (F2 on kafka) or --fault=corrupt (F4, F5-F4) at AT (faults.sh fault_signal).
# The key is the kind as written in the fault column (F5-F2, F5-F4).  A fault
# kind that is not listed is reported as an InputError, not a crash.
INJECT_ACTIONS = {
    "F1": ("docker_stop",),
    "F2": ("iptables_drop", "silent_on"),
    "F3": ("netem_delay",),
    "F4": ("corrupt_on",),
    "F5-F2": ("iptables_drop",),
    "F5-F4": ("corrupt_on",),
}
AUDIT_END_SLACK_S = 3  # the auditor may stop this long before the timeline ends
FIRST_TICK_WINDOW_S = (-0.1, 5.0)  # expected first audit tick vs run_phase_start
RUN_START_ACTION = "run_phase_start"
DOWN_PREFIX = "down:"  # unreachable-node token of the audit file


class InputError(ValueError):
    """Unusable input file (missing, wrong header, unparsable)."""


# ---- the metrics ------------------------------------------------------------

def _first_full_second(fault_at):
    return max(int(math.ceil(fault_at)), 0)


def dip_start_index(timeline, fault_at, pre_mean, share=1.0):
    """Index of the first full second after the fault below 0.9 x pre_mean x
    share, or None (no dip, or pre_mean is None or not positive)."""
    if pre_mean is None or not pre_mean > 0:
        return None
    need = RECOVERY_FRACTION * pre_mean * share - 1e-9
    for i in range(_first_full_second(fault_at), len(timeline)):
        if timeline[i] < need:
            return i
    return None


def recovery_time(timeline, fault_at, pre_mean, after_dip=False, share=1.0):
    """Seconds after the fault until throughput is back to >= 0.9 x pre_mean.

    Looks at the full seconds from ceil(fault_at) on; returns
    (index of the first second with throughput >= 0.9 * pre_mean) - fault_at,
    or None if there is none (or pre_mean is None or not positive).  A fault
    that never lowers throughput gives a value below 1 (0 for an integer
    fault_at).

    after_dip=True (used by load_run): only seconds after the first
    sub-threshold second count, so a fault whose effect lags the injection is
    not "recovered" at once; None if there is no dip at all.

    share (0 < share <= 1) scales the threshold to 0.9 * pre_mean * share
    (see "Recovery threshold" in the module doc).
    """
    if pre_mean is None or not pre_mean > 0:
        return None
    first = _first_full_second(fault_at)
    if after_dip:
        dip = dip_start_index(timeline, fault_at, pre_mean, share)
        if dip is None:
            return None
        first = dip + 1
    need = RECOVERY_FRACTION * pre_mean * share - 1e-9
    for i in range(first, len(timeline)):
        if timeline[i] >= need:
            return float(i - fault_at)
    return None


def stall_seconds(timeline, fault_at, from_first_zero=False):
    """Number of consecutive seconds with throughput 0 from ceil(fault_at).

    from_first_zero=True (used by load_run): the run starts at the first zero
    second after the fault instead of exactly at ceil(fault_at), so a stall
    that begins a few seconds after the injection is still counted.
    """
    i = _first_full_second(fault_at)
    if from_first_zero:
        while i < len(timeline) and timeline[i] != 0:
            i += 1
    n = 0
    while i < len(timeline) and timeline[i] == 0:
        n += 1
        i += 1
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


def audit_summary(audit_rows, fault_at, end_s):
    """Guarded detection result of a run (see "Loader output" in the module doc).

    audit_rows: (ts, nodes) on the timeline axis, in file order; end_s: length
    of the timeline in seconds.  Returns a dict with audit_ticks, audit_last_s,
    audit_div_pre, detect_s and detect_status.
    """
    rows = list(audit_rows)
    out = {"audit_ticks": len(rows), "audit_last_s": None, "audit_div_pre": 0,
           "detect_s": None, "detect_status": ""}
    if rows:
        out["audit_last_s"] = max(ts for ts, _ in rows) - fault_at
    out["audit_div_pre"] = sum(1 for ts, nodes in rows if ts < fault_at and divergent_nodes(nodes))
    if not any(ts >= fault_at for ts, _ in rows):
        out["detect_status"] = "invalid: no audit tick after the fault (auditor dead or not started)"
        return out
    k = next((i for i, (ts, nodes) in enumerate(rows) if ts >= fault_at and divergent_nodes(nodes)), None)
    if k is None:
        if max(ts for ts, _ in rows) < end_s - AUDIT_END_SLACK_S:
            out["detect_status"] = "invalid: the auditor stopped %.1f s before the end of the run" % (
                end_s - max(ts for ts, _ in rows))
        else:
            out["detect_status"] = "undetected"
        return out
    persists = k + 1 < len(rows) and bool(
        set(divergent_nodes(rows[k][1])) & set(divergent_nodes(rows[k + 1][1])))
    if out["audit_div_pre"] == 0 or persists:
        out["detect_s"] = rows[k][0] - fault_at
        out["detect_status"] = "detected"
    else:
        out["detect_status"] = (
            "invalid: one-tick divergence with %d divergent tick(s) before the fault (digest lag artefact?)"
            % out["audit_div_pre"])
    return out


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


def faulty_nodes(rows):
    """Distinct nodes that the OUT_ACTIONS rows of a .faults file took out."""
    out = set()
    for r in rows:
        if r["action"] in OUT_ACTIONS:
            out.update(t for t in r["node"].split("+") if t)
    return sorted(out)


def threshold_share(n, k):
    """(n - k) / n, or 1.0 when n is unknown or k is 0 (see the module doc)."""
    if not n or not k or k >= n:
        return 1.0
    return (n - k) / float(n)


def n_from_tag(prefix):
    """N of a run tag like .../raft_n4_f1_c64_... (None if there is none)."""
    m = re.search(r"_n([0-9]+)_", Path(str(prefix)).name)
    return int(m.group(1)) if m else None


def pick_timeline(timeline_path, wtimeline_path=None):
    """(per-second series, "write" | "total", warning or "") of one run.

    The write timeline when wtimeline_path is given and usable (exists,
    parses, is not empty, same number of seconds as the total timeline), else
    the total timeline.  A missing file is silent (older runs have none); an
    existing but unusable one gives a warning.
    """
    total = read_timeline(timeline_path)
    if wtimeline_path is None or not Path(wtimeline_path).exists():
        return total, "total", ""
    try:
        write = read_timeline(wtimeline_path)
    except InputError as e:
        why = str(e)
    else:
        if not write:
            why = "%s: empty" % wtimeline_path
        elif len(write) != len(total):
            why = "%s: %d seconds, but the timeline has %d" % (wtimeline_path, len(write), len(total))
        else:
            return write, "write", ""
    return total, "total", "write timeline unusable, using the total timeline (%s)" % why


def load_run(timeline_path, faults_path, audit_path=None, n=None, wtimeline_path=None):
    """RQ3 metrics of one run (a dict, see main for the keys).

    The throughput metrics come from pick_timeline(timeline_path,
    wtimeline_path): the write timeline when usable, else the total one.

    fault_at_s = timeline_s of the first (earliest) injection row of the .faults
    file; the audit timestamps are moved onto the timeline axis with the epoch
    of timeline second 0 (epoch_ms of the run_phase_start row, else derived
    from the injection row: epoch_ms - timeline_s * 1000).
    """
    timeline, source, source_warning = pick_timeline(timeline_path, wtimeline_path)
    rows = read_faults(faults_path)
    actions = {a for acts in INJECT_ACTIONS.values() for a in acts}
    inject = [r for r in rows if r["action"] in actions and r["timeline_s"] != ""]
    if not inject:
        kinds = sorted({r["fault"].split(":")[0] for r in rows if r["fault"]})
        unknown = [k for k in kinds if k not in INJECT_ACTIONS]
        if unknown:
            raise InputError(
                "%s: unknown fault kind %s: no injection action is known for it (INJECT_ACTIONS in rq3.py lists %s; "
                "add the action names faults.sh logs for it)"
                % (faults_path, "/".join(unknown), ", ".join(sorted(INJECT_ACTIONS))))
        raise InputError("%s: no injection row (%s) with a timeline_s" % (faults_path, "/".join(sorted(actions))))
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
    out_nodes = faulty_nodes(rows)
    share = threshold_share(n, len(out_nodes))
    warnings = [source_warning] if source_warning else []
    audit = None
    if audit_path is not None:
        raw = read_audit(audit_path)
        audit = [(ts - origin_s, nodes) for ts, nodes in raw]
        if audit and not FIRST_TICK_WINDOW_S[0] <= audit[0][0] <= FIRST_TICK_WINDOW_S[1]:
            warnings.append(
                "first audit tick is %.2f s after the run_phase_start epoch (expected within [%g, %g] s): "
                "clock skew between the client container and the host, or a late auditor"
                % ((audit[0][0],) + FIRST_TICK_WINDOW_S))

    def r3(v):
        return None if v is None else round(v, 3)

    first_full = _first_full_second(fault_at)
    after = timeline[first_full:]
    dip = dip_start_index(timeline, fault_at, pre_mean, share)
    recovery = recovery_time(timeline, fault_at, pre_mean, after_dip=True, share=share)
    if pre_mean is None or not pre_mean > 0:
        status = "no_baseline"
    elif dip is None:
        status = "no_dip"
    elif recovery is None:
        status = "not_recovered"
    else:
        status = "recovered"
    dip_s = None if dip is None else dip - fault_at

    res = {
        "fault": first["fault"],
        "timeline_source": source,
        "fault_at_s": r3(fault_at),
        "warmup_s": warmup,
        "pre_mean_tps": r3(pre_mean),
        "n": n,
        "faulty_nodes": len(out_nodes),
        "threshold_share": r3(share),
        "recovery_threshold_tps": r3(None if pre_mean is None else RECOVERY_FRACTION * pre_mean * share),
        "dip_start_s": r3(dip_s),
        "min_after_fault_tps": r3(min(after)) if after else None,
        "recovery_s": r3(recovery),
        "recovery_from_dip_s": r3(None if recovery is None else recovery - dip_s),
        "recovery_status": status,
        "stall_s": stall_seconds(timeline, fault_at, from_first_zero=True),
    }
    if audit is None:
        res.update({"audit_ticks": None, "audit_last_s": None, "audit_div_pre": None,
                    "detect_s": None, "detect_status": "no_audit", "unreachable_s": None})
    else:
        summ = audit_summary(audit, fault_at, len(timeline))
        res.update({"audit_ticks": summ["audit_ticks"], "audit_last_s": r3(summ["audit_last_s"]),
                    "audit_div_pre": summ["audit_div_pre"], "detect_s": r3(summ["detect_s"]),
                    "detect_status": summ["detect_status"],
                    "unreachable_s": r3(unreachable_latency(audit, fault_at))})
    res["warnings"] = "; ".join(warnings)
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description="RQ3 metrics of one run (see the module docstring).")
    ap.add_argument("prefix", help="results/raw/<EXP>/<tag>: reads <prefix>.timeline, .wtimeline, .faults and .audit")
    args = ap.parse_args(argv)
    audit = Path(args.prefix + ".audit")
    try:
        m = load_run(args.prefix + ".timeline", args.prefix + ".faults", audit if audit.exists() else None,
                     n=n_from_tag(args.prefix), wtimeline_path=args.prefix + ".wtimeline")
    except InputError as e:
        print("rq3: %s" % e, file=sys.stderr)
        return 2
    for k, v in m.items():
        print("%s=%s" % (k, "" if v is None else v))
    if m["warnings"]:
        print("rq3: warning: %s" % m["warnings"], file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
