#!/usr/bin/env bash
# E1 (RQ1/2): throughput and latency vs the number of clients.
#
#   experiments/exp/e1_clients.sh [--dry-run]
#
# For each backend in kafka raft tendermint bft2 (BACKENDS narrows it):
# N=4, block 100, YCSB-A (WORKLOAD=a), no RTT, no fault, COMMIT_MODE=sync,
# CLIENTS in {4,16,64,256}, REPS=3 reps each: 4 x 4 x 3 = 48 runs, rows in
# results/raw/e1.csv. The tendermint backend needs CLIENTS <= 256 in sync
# commit: the largest value of E1 fits. E2-E6 take each backend's best CLIENTS
# from these rows, so the four values are fixed.
#
# Other env: the common env, resumability, retry, noise rule and exit codes
# described in experiments/exp/exp_lib.sh.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/exp_lib.sh"

exp_begin e1 "$@"
exp_plan() {
  for b in $EXP_BACKEND_LIST; do
    for c in $EXP_CLIENTS_E1; do
      exp_cfg "$b" 4 0 "$c" 100 a 0 none
    done
  done
}
exp_main
