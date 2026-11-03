#!/usr/bin/env python3
"""Final data set of the cloud campaign (experiments/cloud/campaign.sh).

Usage:
    python3 experiments/cloud/assemble.py results/cloud-bf100ms results/cloud-bf100ms/final

For every <src>/<prefix>-<eN>-bf100ms.csv (eN in e1..e6) writes <out>/<eN>.csv
with the `exp` column rewritten to eN, and copies the run sidecars of
<src>/<prefix>-<eN>-bf100ms/ to <out>/<eN>/. With plain eN names
experiments/plot/plot.py draws the same figures as for the M3 data (its E6
no-fault reference is the `e2` experiment) and experiments/plot/rq3_table.py
reads <out>/e6. Rows are copied unchanged otherwise (there is no re-run pass
to merge: the campaign's own noise rule already added the extra reps).

Standard library only.
"""
import csv
import re
import shutil
import sys
from pathlib import Path

NAME = re.compile(r"^(?P<prefix>[A-Za-z0-9.-]+?)-(?P<exp>e[1-6])-bf100ms\.csv$")


def main(argv):
    if len(argv) != 3:
        print(__doc__.strip().splitlines()[2].strip(), file=sys.stderr)
        return 2
    src, out = Path(argv[1]), Path(argv[2])
    if not src.is_dir():
        print("assemble.py: %s is not a directory" % src, file=sys.stderr)
        return 2
    out.mkdir(parents=True, exist_ok=True)
    done = []
    for path in sorted(src.glob("*-bf100ms.csv")):
        m = NAME.match(path.name)
        if not m:
            continue
        exp = m.group("exp")
        with open(path, newline="") as fh:
            rows = list(csv.reader(fh))
        if not rows:
            continue
        header = rows[0]
        if "exp" not in header:
            print("assemble.py: %s has no exp column" % path, file=sys.stderr)
            return 2
        i = header.index("exp")
        for r in rows[1:]:
            r[i] = exp
        with open(out / ("%s.csv" % exp), "w", newline="") as fh:
            csv.writer(fh, lineterminator="\n").writerows(rows)
        side = src / path.name[: -len(".csv")]
        if side.is_dir():
            dst = out / exp
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(side, dst)
        done.append("%s: %d rows" % (exp, len(rows) - 1))
    if not done:
        print("assemble.py: no <prefix>-eN-bf100ms.csv in %s" % src, file=sys.stderr)
        return 2
    print("\n".join(done))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
