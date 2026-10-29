#!/usr/bin/env python3
"""Coefficient of variation of tps over the reps of one configuration.

    cv.py CSV BACKEND N CLIENTS

Prints "<cv> <rows>" on stdout: cv = sample standard deviation (n-1) / mean of
the tps of every row of (BACKEND, N, CLIENTS), formatted with 4 decimals, and
the number of rows used. cv is "NA" when it is undefined: fewer than 2 rows or
a mean <= 0. Exits 1 with a message on stderr on a missing file or column.

Used by experiments/exp/p0_baseline.sh for the plan rule "std/mean > 10% ->
more reps". Standard library only.
"""

import math
import statistics
import sys

from best_clients import read_tps


def cv_of(values):
    """Sample std / mean of values, or None if undefined."""
    if len(values) < 2:
        return None
    mean = math.fsum(values) / len(values)
    if mean <= 0:
        return None
    return statistics.stdev(values) / mean


def config_cv(path, backend, n, clients):
    """Return (cv or None, number of rows) for one configuration."""
    values = read_tps(path, backend, n).get(clients, [])
    return cv_of(values), len(values)


def main(argv):
    if len(argv) != 5:
        sys.stderr.write("usage: cv.py CSV BACKEND N CLIENTS\n")
        return 1
    try:
        cv, rows = config_cv(argv[1], argv[2], int(argv[3]), int(argv[4]))
    except (OSError, ValueError) as e:
        sys.stderr.write("cv: %s\n" % e)
        return 1
    print("%s %d" % ("NA" if cv is None else "%.4f" % cv, rows))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
