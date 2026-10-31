#!/usr/bin/env bash
# E3 (RQ1/2): throughput and latency vs the block size.
#
#   experiments/exp/e3_block.sh [--dry-run]
#
# For each backend in kafka raft tendermint bft2: N=4, CLIENTS = the backend's
# best from E1 (results/raw/e1.csv, see exp_lib.sh; FIXED_CLIENTS /
# BEST_CLIENTS_* override), YCSB-A, no RTT, no fault, COMMIT_MODE=sync,
# BLOCK_SIZE in {10,100,1000}, REPS=3 reps each: 4 x 3 x 3 = 36 runs, rows in
# results/raw/e3.csv. The core batcher still flushes a partial block after
# 1 s (BATCH_FLUSH is not changed), so a block size larger than the pending
# Sets is cut by the timer, not by the size (experiments/NOTES.md).
#
# Other env: BLOCKS="10 100 1000" (block sizes), and the common env,
# resumability, retry, noise rule and exit codes described in
# experiments/exp/exp_lib.sh.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/exp_lib.sh"

BLOCKS="${BLOCKS:-10 100 1000}"
exp_begin e3 "$@"
[ -n "$BLOCKS" ] || die "BLOCKS must list at least one block size"
for blk in $BLOCKS; do hbdb_require_uint BLOCKS "$blk"; done

exp_plan() {
  for b in $EXP_BACKEND_LIST; do
    exp_resolve_clients "$b" || continue
    c="$EXP_RESOLVED_CLIENTS"
    for blk in $BLOCKS; do
      exp_cfg "$b" 4 0 "$c" "$blk" a 0 none
    done
  done
}
exp_main
