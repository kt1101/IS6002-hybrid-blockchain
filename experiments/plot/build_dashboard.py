#!/usr/bin/env python3
"""Build the offline results dashboard: one self-contained HTML file.

Usage:
    python3 experiments/plot/build_dashboard.py --final results/final \\
        --rq3 results/figs/rq3_summary.csv \\
        --out results/dashboard.html --out demo/web/static/overview.html

Stdlib only. Reads the final experiment CSVs (<final>/e1.csv, e1-bf100ms.csv,
e2.csv ... e6.csv; fixed 22-column header) and the RQ3 summary, aggregates
each configuration (mean, sample std, reps), and embeds the result as JSON in
dashboard_template.html (next to this script). Every --out gets the same
bytes; the page needs no network (inline CSS/JS, inline SVG charts).
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

EXPERIMENTS = ("e1", "e1-bf100ms", "e2", "e3", "e4", "e5", "e6")
GROUP_KEYS = ("backend", "n", "f", "clients", "block_size", "workload",
              "rtt_ms", "fault", "commit_mode")
INT_KEYS = ("n", "f", "clients", "block_size", "rtt_ms")
METRICS = ("tps", "lat_mean_ms", "lat_p99_ms")
DATA_MARKER = "/*__DATA__*/null"
TEMPLATE = Path(__file__).resolve().parent / "dashboard_template.html"


def load_rows(path) -> list[dict]:
    """Rows of a CSV file as dicts of strings."""
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def _stats(values: list[float]) -> dict:
    return {
        "mean": statistics.mean(values),
        "std": statistics.stdev(values) if len(values) > 1 else None,
    }


def _key_value(key: str, value: str):
    if key in INT_KEYS:
        try:
            return int(value)
        except ValueError:
            return value
    return value


def aggregate(rows, keys) -> list[dict]:
    """One dict per configuration (the values of `keys`), in first-seen order:
    the key columns (INT_KEYS parsed as int), `reps`, {mean, std} of each of
    METRICS (std is the sample std, None for a single rep) and `errors` (sum)."""
    groups: dict[tuple, list[dict]] = {}
    for row in rows:
        groups.setdefault(tuple(row[k] for k in keys), []).append(row)
    out = []
    for key, members in groups.items():
        g = {k: _key_value(k, v) for k, v in zip(keys, key)}
        g["reps"] = len(members)
        for m in METRICS:
            g[m] = _stats([float(r[m]) for r in members])
        g["errors"] = sum(int(float(r["errors"] or 0)) for r in members)
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


def build(final_dir, rq3_csv) -> dict:
    final_dir = Path(final_dir)
    experiments = {
        exp: aggregate(load_rows(final_dir / (exp + ".csv")), GROUP_KEYS)
        for exp in EXPERIMENTS
    }
    rq3 = [{k: (v if k in ("backend", "fault") else _parse_cell(v)) for k, v in row.items()}
           for row in load_rows(rq3_csv)]
    return {
        "generated": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source": "%s/{%s}.csv, %s" % (final_dir.as_posix(), ",".join(EXPERIMENTS),
                                       Path(rq3_csv).as_posix()),
        "experiments": experiments,
        "rq3": rq3,
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
    ap.add_argument("--final", default="results/final", help="directory of e1.csv ... e6.csv")
    ap.add_argument("--rq3", default="results/figs/rq3_summary.csv", help="RQ3 summary CSV")
    ap.add_argument("--out", action="append", required=True,
                    help="output HTML file (repeatable; every file gets the same bytes)")
    ap.add_argument("--template", default=str(TEMPLATE), help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    try:
        html = render(build(args.final, args.rq3),
                      Path(args.template).read_text(encoding="utf-8"))
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
