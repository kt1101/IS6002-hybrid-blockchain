#!/usr/bin/env bash
# E4 (RQ1/2): throughput and latency vs the YCSB workload.
#
#   experiments/exp/e4_workload.sh [--dry-run]
#
# For each backend in kafka raft tendermint bft2: N=4, CLIENTS = the backend's
# best from E1 (results/raw/e1.csv, see exp_lib.sh; FIXED_CLIENTS /
# BEST_CLIENTS_* override), block 100, no RTT, no fault, COMMIT_MODE=sync,
# WORKLOAD in {a,b,c} (YCSB workloada/b/c, uniform; the files results/ycsb/
# <w>.load.dat and <w>.run.dat made by experiments/ycsb/gen.sh; the CSV column
# holds a, b or c), REPS=3 reps each: 4 x 3 x 3 = 36 runs, rows in
# results/raw/e4.csv.
#
# Other env: WORKLOADS="a b c", and the common env, resumability, retry, noise
# rule and exit codes described in experiments/exp/exp_lib.sh.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/exp_lib.sh"

WORKLOADS="${WORKLOADS:-a b c}"
exp_begin e4 "$@"
[ -n "$WORKLOADS" ] || die "WORKLOADS must list at least one workload"
for w in $WORKLOADS; do exp_require_workload "$w"; done

exp_plan() {
  for b in $EXP_BACKEND_LIST; do
    exp_resolve_clients "$b" || continue
    c="$EXP_RESOLVED_CLIENTS"
    for w in $WORKLOADS; do
      exp_cfg "$b" 4 0 "$c" 100 "$w" 0 none
    done
  done
}
exp_main
