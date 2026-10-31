#!/usr/bin/env bash
# E5 (RQ1/2): throughput and latency vs the network round-trip time.
#
#   experiments/exp/e5_rtt.sh [--dry-run]
#
# For each backend in kafka raft tendermint bft2: N=4, CLIENTS = the backend's
# best from E1 (results/raw/e1.csv, see exp_lib.sh; FIXED_CLIENTS /
# BEST_CLIENTS_* override), block 100, YCSB-A, no fault, COMMIT_MODE=sync,
# RTT_MS in {0,20,60}, REPS=3 reps each: 4 x 3 x 3 = 36 runs, rows in
# results/raw/e5.csv. run_one.sh applies the RTT with netem on every node's
# egress (RTT_MS/2 each way) after the health check, so load, settle, warmup
# and measurement all run with it.
#
# Other env: RTTS="0 20 60", and the common env, resumability, retry, noise
# rule and exit codes described in experiments/exp/exp_lib.sh.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/exp_lib.sh"

RTTS="${RTTS:-0 20 60}"
exp_begin e5 "$@"
[ -n "$RTTS" ] || die "RTTS must list at least one RTT"
for rtt in $RTTS; do hbdb_require_uint RTTS "$rtt"; done

for b in $EXP_BACKEND_LIST; do
  exp_resolve_clients "$b" || continue
  c="$EXP_RESOLVED_CLIENTS"
  for rtt in $RTTS; do
    exp_cfg "$b" 4 0 "$c" 100 a "$rtt" none
  done
done
exp_finish
