#!/usr/bin/env python3
"""Build demo/web/static/present/data.js, the data of the presentation pages.

The pages (demo/web/static/present/*.html) show phase 2-3 results that live
on another branch. This script copies them, unchanged, from one commit:

- CSV files (results/e7, results/e9, results/e9b, results/e1x) as lists of string rows,
  exactly as written in the file (no parsing of numbers, no rounding);
- the markdown tables of experiments/NOTES.md that the pages show (E6, E7,
  E9, E9b, E11, E12), cell by cell; E10 comes from results/e1x/e10_latency.csv.

Every dataset carries its file path and the commit, so the page can print
its source next to each table and chart.

Run from the repo root (stdlib only; needs git and the commit):

    python3 experiments/plot/build_present_data.py --ref p10-attest \\
        --out demo/web/static/present/data.js

--check compares instead of writing (exit 1 when the file is stale).
"""
import argparse
import csv
import io
import json
import subprocess
import sys

CSV_FILES = {
    "e7_saturation": "results/e7/saturation.csv",
    "e7_points": "results/e7/saturation_points.csv",
    "e9_summary": "results/e9/rq5_summary.csv",
    "e1x_summary": "results/e1x/rq6_summary.csv",
    "e1x_runs": "results/e1x/rq6_runs.csv",
    "e10_latency": "results/e1x/e10_latency.csv",
    "e9b_summary": "results/e9b/rq5_summary.csv",
    "e9b_paired": "results/e9b/e9b_paired.csv",
    "e9b_runs": "results/e9b/e9b_runs.csv",
}

NOTES = "experiments/NOTES.md"

# (key, heading prefix in NOTES, index of the table under that heading)
NOTES_TABLES = [
    ("e6", "### Đối chiếu giả thuyết F1–F5", 0),
    ("e7", "## Giai đoạn 2/3: E7 quét tốc độ trên M3", 0),
    ("e9_detect", "## Giai đoạn 2/3: E9 attestation trên M3", 0),
    ("e9_latency", "## Giai đoạn 2/3: E9 attestation trên M3", 1),
    ("e9b", "## Giai đoạn 2/3: E9b đối chứng độ trễ của attestation", 0),
    ("e9b_paired", "## Giai đoạn 2/3: E9b đối chứng độ trễ của attestation", 1),
    ("e11", "### E11: truy vết 1 node lỗi", 0),
    ("e12", "### E12: ngưỡng quorum", 0),
]


def git_show(ref, path):
    """Return the text of path at ref (git show ref:path)."""
    out = subprocess.run(["git", "show", "%s:%s" % (ref, path)], check=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return out.stdout.decode("utf-8")


def full_commit(ref):
    out = subprocess.run(["git", "rev-parse", "--verify", ref + "^{commit}"], check=True,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return out.stdout.decode("ascii").strip()


def read_csv(text):
    """Rows of a CSV as dicts of strings, in file order."""
    return [dict(r) for r in csv.DictReader(io.StringIO(text))]


def _cells(line):
    """Cells of one markdown table line ("| a | b |" -> ["a", "b"])."""
    s = line.strip()
    if s.startswith("|"):
        s = s[1:]
    if s.endswith("|"):
        s = s[:-1]
    return [c.strip() for c in s.split("|")]


def _is_rule(cells):
    return all(c and set(c) <= set("-: ") for c in cells)


def section(text, heading):
    """Lines of the section that starts with a line beginning with heading,
    up to the next heading of the same or a higher level."""
    lines = text.split("\n")
    level = len(heading) - len(heading.lstrip("#"))
    for i, line in enumerate(lines):
        if line.startswith(heading):
            out = []
            for nxt in lines[i + 1:]:
                if nxt.startswith("#"):
                    lvl = len(nxt) - len(nxt.lstrip("#"))
                    if lvl <= level:
                        break
                out.append(nxt)
            return out
    raise KeyError("heading not found: %r" % heading)


def tables(lines):
    """Markdown tables in lines, each {"header": [...], "rows": [[...], ...]}."""
    found, cur = [], []
    for line in lines + [""]:
        if line.lstrip().startswith("|"):
            cur.append(_cells(line))
            continue
        if cur:
            header, body = cur[0], cur[1:]
            if body and _is_rule(body[0]):
                body = body[1:]
            found.append({"header": header, "rows": body})
            cur = []
    return found


def notes_table(text, heading, index):
    t = tables(section(text, heading))
    if index >= len(t):
        raise KeyError("table %d not found under %r (found %d)" % (index, heading, len(t)))
    return t[index]


def build(ref):
    commit = full_commit(ref)
    short = commit[:7]
    data = {"commit": commit, "commit_short": short, "ref": ref, "csv": {}, "notes": {}}
    for key, path in CSV_FILES.items():
        data["csv"][key] = {"source": path, "commit": short, "rows": read_csv(git_show(commit, path))}
    notes = git_show(commit, NOTES)
    for key, heading, index in NOTES_TABLES:
        t = notes_table(notes, heading, index)
        t.update({"source": NOTES, "section": heading.lstrip("# ").strip(), "commit": short})
        data["notes"][key] = t
    return data


def render(data):
    body = json.dumps(data, ensure_ascii=False, indent=1, sort_keys=True)
    return ("// Generated by experiments/plot/build_present_data.py --ref %s (commit %s); do not edit.\n"
            "// Values are copied unchanged from the files named in each dataset.\n"
            "window.PRESENT_DATA = %s;\n" % (data["ref"], data["commit_short"], body))


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--ref", required=True, help="commit or tag that holds the results (e.g. p10-attest)")
    p.add_argument("--out", required=True, help="data.js to write")
    p.add_argument("--check", action="store_true", help="compare with --out instead of writing")
    a = p.parse_args(argv)
    out = render(build(a.ref))
    if a.check:
        try:
            with open(a.out, encoding="utf-8") as f:
                cur = f.read()
        except FileNotFoundError:
            cur = None
        if cur != out:
            print("%s is stale: re-run without --check" % a.out, file=sys.stderr)
            return 1
        return 0
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
