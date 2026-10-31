#!/usr/bin/env python3
"""Print the best CLIENTS value of one backend from a results CSV.

    best_clients.py CSV BACKEND

Best = the CLIENTS value with the highest mean tps over all its rows (reps,
including extra reps) at N=4 for that backend; on a tie the smaller CLIENTS
wins. Rows of other backends and other N are ignored; rows with an empty or
non-numeric tps are skipped. Columns are found by header name, in any order.
Prints one integer on stdout. Exits 1 with a message on stderr when the file
is missing, lacks a needed column (backend, n, clients, tps), or has no usable
N=4 row for the backend.

Used by experiments/exp/p0_baseline.sh to pick CLIENTS for part (b) and by
experiments/exp/exp_lib.sh (E2-E6) to pick the CLIENTS of each backend from
the E1 rows; cv.py shares read_tps().
Standard library only.
"""

import csv
import math
import sys

BEST_N = "4"
NEEDED = ("backend", "n", "clients", "tps")


def read_tps(path, backend, n, where=None):
    """Return {clients: [tps, ...]} for the rows of backend at N=n.

    where ({column: value}, optional) keeps only the rows whose column (by
    header name, compared as stripped text) equals value, e.g.
    {"block_size": "10", "fault": "none"}; the E3-E6 configurations differ in
    columns other than backend, n and clients.

    Raises ValueError if a needed column (or a column named in where) is
    missing from the header. Rows with a non-integer clients or an
    empty/non-numeric/non-finite tps are skipped.
    """
    out = {}
    where = dict(where or {})
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        missing = [c for c in NEEDED + tuple(sorted(where)) if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError("%s has no column %s (header: %s)" % (
                path, ", ".join("'%s'" % c for c in missing),
                ",".join(reader.fieldnames or []) or "<empty>"))
        for row in reader:
            if row["backend"] != backend or (row["n"] or "").strip() != str(n):
                continue
            if any((row[k] or "").strip() != str(v) for k, v in where.items()):
                continue
            try:
                clients = int(row["clients"])
                tps = float(row["tps"])
            except (TypeError, ValueError):
                continue
            if not math.isfinite(tps):
                continue
            out.setdefault(clients, []).append(tps)
    return out


def best_clients(path, backend):
    """Return the best CLIENTS (int) for backend at N=4, or raise ValueError."""
    tps_by_clients = read_tps(path, backend, BEST_N)
    if not tps_by_clients:
        raise ValueError("no N=%s rows for backend '%s' in %s" % (BEST_N, backend, path))
    # Round the means so that float noise cannot break a tie; the smaller
    # CLIENTS wins ties.
    means = {c: round(math.fsum(v) / len(v), 6) for c, v in tps_by_clients.items()}
    return min(means, key=lambda c: (-means[c], c))


def main(argv):
    if len(argv) != 3:
        sys.stderr.write("usage: best_clients.py CSV BACKEND\n")
        return 1
    try:
        print(best_clients(argv[1], argv[2]))
    except (OSError, ValueError) as e:
        sys.stderr.write("best_clients: %s\n" % e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
