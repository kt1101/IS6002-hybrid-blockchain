#!/usr/bin/env python3
"""Build the offline results dashboard: one self-contained HTML file.

Usage:
    python3 experiments/plot/build_dashboard.py \\
        --config experiments/plot/dashboard_datasets.json \\
        --out results/dashboard.html --out demo/web/static/overview.html

Stdlib only. The config (paths relative to the current directory, normally
the repository root) lists the datasets (one per machine / campaign: final
experiment CSVs with the fixed 22-column header, RQ3 summaries, optional
per-run sidecar directories and Kafka operation counts) plus the P0 baseline
CSV. Every configuration is aggregated (mean, sample std, reps) and the
result is embedded as JSON in dashboard_template.html (next to this script).
Every --out gets the same bytes; the page needs no network (inline CSS/JS,
inline SVG charts).

Per-run sidecars (when a dataset names a sidecar directory) add, per row:
* write latency (mean, p99) from <tag>.ops.csv (the `write` line), so the
  page can show the same latency as the figures in results/figs;
* for E6, the write ratio = mean successful writes/s from 10 s after the
  fault to the end of the run, divided by the mean over the measured seconds
  before the fault (.wtimeline and .faults, read with rq3.py).
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rq3  # noqa: E402  (stdlib only)

GROUP_KEYS = ("backend", "n", "f", "clients", "block_size", "workload",
              "rtt_ms", "fault", "commit_mode")
INT_KEYS = ("n", "f", "clients", "block_size", "rtt_ms")
METRICS = ("tps", "lat_mean_ms", "lat_p99_ms")
OPTIONAL_METRICS = ("net_bytes_per_block", "host_cpu_max_pct")
DATA_MARKER = "/*__DATA__*/null"
TEMPLATE = Path(__file__).resolve().parent / "dashboard_template.html"
CONFIG = Path(__file__).resolve().parent / "dashboard_datasets.json"
OPS_HEADER_PREFIX = ["op", "count", "tps", "lat_mean_ms", "lat_p50_ms", "lat_p99_ms"]
POST_FAULT_SKIP_S = 10  # write ratio: the first 10 s after the fault are left out


def load_rows(path) -> list[dict]:
    """Rows of a CSV file as dicts of strings."""
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def _stats(values: list[float]):
    """{mean, std} (sample std, None for one value); None for no values."""
    if not values:
        return None
    return {
        "mean": statistics.mean(values),
        "std": statistics.stdev(values) if len(values) > 1 else None,
    }


def _float(value):
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _key_value(key: str, value: str):
    if key in INT_KEYS:
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _cell(v) -> str:
    """A CSV value as run_one.sh wrote it into the run tag (4.0 -> 4)."""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def run_tag(row) -> str:
    """run_one.sh's run tag of a CSV row (same as plot.run_tag, without the
    _bf<BATCH_FLUSH> suffix)."""
    tag = "%s_n%s_f%s_c%s_b%s_w%s_rtt%s_%s_%s_r%s" % tuple(
        _cell(row[c]) for c in ("backend", "n", "f", "clients", "block_size", "workload",
                                "rtt_ms", "fault", "commit_mode", "rep"))
    return re.sub(r"[^A-Za-z0-9._-]", "_", tag)


def read_write_latency(path):
    """(mean_ms, p99_ms) of the `write` line of an ops sidecar, or None
    (missing/unreadable file, other header, no successful write)."""
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return None
    if not lines or lines[0].split(",")[:len(OPS_HEADER_PREFIX)] != OPS_HEADER_PREFIX:
        return None
    for line in lines[1:]:
        f = line.split(",")
        if f and f[0] == "write" and len(f) >= 6:
            count, mean_ms, p99_ms = _float(f[1]), _float(f[3]), _float(f[5])
            if not count or mean_ms is None or p99_ms is None:
                return None
            return mean_ms, p99_ms
    return None


def write_ratio(wtimeline_path, faults_path):
    """Successful writes/s from POST_FAULT_SKIP_S after the fault to the end,
    divided by the mean over the measured seconds before the fault; None when
    a file is missing or unusable or there is no pre-fault write."""
    try:
        timeline = rq3.read_timeline(wtimeline_path)
        res = rq3.load_run(str(wtimeline_path), str(faults_path))
    except (OSError, rq3.InputError, ValueError, KeyError):
        return None
    pre = res.get("pre_mean_tps")
    fault_at = res.get("fault_at_s")
    if not pre or fault_at is None:
        return None
    post = timeline[int(math.ceil(fault_at)) + POST_FAULT_SKIP_S:]
    if not post:
        return None
    return round((sum(post) / len(post)) / pre, 3)


class Sidecars:
    """Locates the per-run sidecar files of a dataset's rows."""

    def __init__(self, pattern: str, suffix: str):
        self.pattern, self.suffix = pattern, suffix

    def path(self, raw_exp: str, row: dict, ext: str) -> Path:
        return Path(self.pattern.format(exp=raw_exp)) / (run_tag(row) + self.suffix + ext)


def aggregate(rows, keys, sidecars: Sidecars | None = None, raw_exp: str | None = None) -> list[dict]:
    """One dict per configuration (the values of `keys`), in first-seen order:
    the key columns (INT_KEYS parsed as int), `reps`, {mean, std} of each of
    METRICS (std is the sample std, None for a single rep), {mean, std} or None
    of each of OPTIONAL_METRICS (empty cells skipped), `errors` and
    `wrong_gets` (sums), `saturated` (true when any rep saturated the host).
    With sidecars: `w_lat_mean_ms` / `w_lat_p99_ms` ({mean, std} over the reps
    that have an ops sidecar, else None) and `w_reps`; for fault rows
    `write_ratio` ({min, max, mean, n} over the reps that have one, else None)."""
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        groups.setdefault(tuple(row[k] for k in keys), []).append(row)
    out = []
    for key, members in groups.items():
        g = {k: _key_value(k, v) for k, v in zip(keys, key)}
        g["reps"] = len(members)
        for m in METRICS:
            g[m] = _stats([float(r[m]) for r in members])
        for m in OPTIONAL_METRICS:
            g[m] = _stats([v for v in (_float(r.get(m)) for r in members) if v is not None])
        g["errors"] = sum(int(_float(r.get("errors")) or 0) for r in members)
        g["wrong_gets"] = sum(int(_float(r.get("wrong_gets")) or 0) for r in members)
        g["saturated"] = any(str(r.get("saturated", "")).strip() == "1" for r in members)
        if sidecars is not None and raw_exp is not None:
            lats = [read_write_latency(sidecars.path(raw_exp, r, ".ops.csv")) for r in members]
            lats = [x for x in lats if x]
            g["w_lat_mean_ms"] = _stats([x[0] for x in lats])
            g["w_lat_p99_ms"] = _stats([x[1] for x in lats])
            g["w_reps"] = len(lats)
            if g.get("fault", "none") != "none":
                ratios = [write_ratio(sidecars.path(raw_exp, r, ".wtimeline"),
                                      sidecars.path(raw_exp, r, ".faults")) for r in members]
                ratios = [x for x in ratios if x is not None]
                g["write_ratio"] = ({"min": min(ratios), "max": max(ratios),
                                     "mean": statistics.mean(ratios), "n": len(ratios)}
                                    if ratios else None)
        out.append(g)
    return out


def _parse_cell(value: str):
    """A numeric cell as float, an empty cell as None, anything else as is."""
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return value


def _rq3_rows(path, mem=None) -> list[dict]:
    rows = [{k: (v if k in ("backend", "fault") else _parse_cell(v)) for k, v in row.items()}
            for row in load_rows(path)]
    if mem:
        for r in rows:
            r["mem"] = mem
    return rows


def _kafka_ops(path) -> list[dict]:
    """Kafka operation counts per (n, clients): mean reads and writes over reps."""
    groups: dict[tuple, list[dict]] = {}
    for row in load_rows(path):
        groups.setdefault((row["n"], row["clients"]), []).append(row)
    out = []
    for (n, clients), members in groups.items():
        out.append({"n": int(n), "clients": int(clients), "reps": len(members),
                    "reads": _stats([float(r["reads"]) for r in members]),
                    "writes": _stats([float(r["writes"]) for r in members])})
    return sorted(out, key=lambda r: (r["n"], r["clients"]))


def build_dataset(spec: dict, base=".") -> dict:
    """The embedded data of one dataset of the config (relative paths are
    resolved against base)."""
    base = Path(base)
    final_dir = base / spec["final"]
    side = spec.get("sidecars")
    sidecars = Sidecars(str(base / side["dir"]), side.get("suffix", "")) if side else None
    experiments = {}
    for exp in spec["experiments"]:
        experiments[exp] = aggregate(load_rows(final_dir / (exp + ".csv")), GROUP_KEYS,
                                     sidecars, exp if sidecars else None)
    for extra in spec.get("extra", []):
        rows = aggregate(load_rows(final_dir / (extra["exp"] + ".csv")), GROUP_KEYS,
                         sidecars, extra["exp"] if sidecars else None)
        for r in rows:
            r["mem"] = extra.get("mem")
        experiments.setdefault(extra["into"], []).extend(rows)
    rq3_rows = []
    for item in spec.get("rq3", []):
        rq3_rows.extend(_rq3_rows(base / item["path"], item.get("mem")))
    out = {
        "id": spec["id"],
        "label": spec["label"],
        "machine": spec.get("machine", ""),
        "notes": list(spec.get("notes", [])),
        "missing": dict(spec.get("missing", {})),
        "experiments": experiments,
        "rq3": rq3_rows,
        "write_latency": any(r.get("w_lat_mean_ms") for rows in experiments.values() for r in rows),
        "kafka_ops": _kafka_ops(base / spec["kafka_ops"]) if spec.get("kafka_ops") else [],
        "source": ", ".join([spec["final"]] + [i["path"] for i in spec.get("rq3", [])]),
    }
    return out


def build(config: dict, base=".") -> dict:
    """The embedded data of every dataset of the config plus the P0 baseline
    (relative paths in the config are resolved against base)."""
    datasets = [build_dataset(spec, base) for spec in config["datasets"]]
    p0 = aggregate(load_rows(Path(base) / config["p0"]), GROUP_KEYS) if config.get("p0") else []
    return {
        "generated": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source": "; ".join(d["source"] for d in datasets) + ("; " + config["p0"] if config.get("p0") else ""),
        "datasets": datasets,
        "p0": p0,
    }


def render(data: dict, template: str) -> str:
    """The template with its single DATA_MARKER replaced by the JSON of data
    ("</" escaped so the JSON cannot close its <script> element)."""
    if template.count(DATA_MARKER) != 1:
        raise ValueError("template must contain %s exactly once" % DATA_MARKER)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return template.replace(DATA_MARKER, payload.replace("</", "<\\/"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--config", default=str(CONFIG), help="datasets config (JSON)")
    ap.add_argument("--base", default=".", help="directory the config paths are relative to (default: the current directory)")
    ap.add_argument("--out", action="append", required=True,
                    help="output HTML file (repeatable; every file gets the same bytes)")
    ap.add_argument("--template", default=str(TEMPLATE), help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    try:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        html = render(build(config, args.base), Path(args.template).read_text(encoding="utf-8"))
    except (OSError, KeyError, ValueError) as e:
        print("build_dashboard: %s" % e, file=sys.stderr)
        return 2
    for out in args.out:
        p = Path(out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(html, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
