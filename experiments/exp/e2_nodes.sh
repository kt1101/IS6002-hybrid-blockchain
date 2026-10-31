#!/usr/bin/env bash
# E2 (RQ1/2): throughput and latency vs the cluster size N.
#
#   experiments/exp/e2_nodes.sh [--dry-run]
#   NS="4 7 10 13 16" experiments/exp/e2_nodes.sh     # a bigger machine
#
# For each backend in kafka raft tendermint bft2: CLIENTS = the backend's best
# from E1 (results/raw/e1.csv, see exp_lib.sh; FIXED_CLIENTS / BEST_CLIENTS_*
# override), block 100, YCSB-A, no RTT, no fault, COMMIT_MODE=sync, N in NS,
# REPS=3 reps each. Rows in results/raw/e2.csv. N=4 is run again here (its own
# rows), so the N sweep comes from one experiment.
#
# NS defaults to "4 7": this machine runs at most N=8 (Docker VM 7.65 GiB, see
# "Quyet dinh ve N toi da" in experiments/NOTES.md), and the plan's N=10, 13, 16
# do not fit. The plan's full sweep is NS="4 7 10 13 16" (4 x 5 x 3 = 60 runs)
# on a machine with enough RAM; the default is 4 x 2 x 3 = 24 runs.
#
# Kafka operation counts (Figure 8 of the paper): for BACKEND=kafka every run
# also appends one row to results/raw/e2-kafka-ops.csv, read from the broker
# by experiments/exp/kafka_ops.sh through run_one.sh's PRE_DOWN_HOOK just
# before the cluster is torn down (same logic as scripts/get_kafka_ops.sh:
# CURRENT-OFFSET = reads, LOG-END-OFFSET = writes). With EXP=<other> the file
# is results/raw/<EXP>-kafka-ops.csv. The summary compares the number of
# kafka rows in the CSV with the rows in the ops file.
#
# Other env: the common env, resumability, retry, noise rule and exit codes
# described in experiments/exp/exp_lib.sh.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/exp_lib.sh"

NS="${NS:-4 7}"
exp_begin e2 "$@"
[ -n "$NS" ] || die "NS must list at least one cluster size"
for n in $NS; do
  hbdb_require_uint NS "$n"
  [ "$n" -ge 1 ] || die "NS entries must be >= 1, got '$n'"
done

E2_OPS_CSV="${KAFKA_OPS_CSV:-$HBDB_RESULTS_RAW/${EXP}-kafka-ops.csv}"
export KAFKA_OPS_CSV="$E2_OPS_CSV"

exp_hook_for() {
  if [ "$1" = kafka ]; then
    echo "$EXP_LIB_DIR/kafka_ops.sh"
  fi
}

# Rows of kafka runs in e2.csv vs rows in the ops file (both keyed by n,
# clients, rep).
exp_summary_extra() {
  local rows ops
  [ -s "$EXP_CSV" ] || return 0
  rows="$(awk -F, 'NR > 1 && $4 == "kafka" { c++ } END { print c + 0 }' "$EXP_CSV")"
  ops=0
  if [ -s "$E2_OPS_CSV" ]; then
    ops="$(awk -F, 'NR > 1 && $3 == "kafka" { c++ } END { print c + 0 }' "$E2_OPS_CSV")"
  fi
  echo "kafka ops rows: $ops in $E2_OPS_CSV for $rows kafka rows in $EXP_CSV$([ "$ops" -ge "$rows" ] || echo '  (some runs have no ops row: see the PRE_DOWN_HOOK warnings in the logs)')"
}

for b in $EXP_BACKEND_LIST; do
  exp_resolve_clients "$b" || continue
  c="$EXP_RESOLVED_CLIENTS"
  for n in $NS; do
    exp_cfg "$b" "$n" 0 "$c" 100 a 0 none
  done
done
exp_finish
