#!/usr/bin/env bash
# Cloud campaign: E1-E6 with the batcher flush at 100 ms (BATCH_FLUSH=100ms)
# on one machine, so E2-E6 take their best CLIENTS from an E1 measured on the
# same host. The drivers, run_one.sh and the CSV schema are the ones of the
# M3 campaign; only BATCH_FLUSH and the EXP names differ.
#
#   experiments/cloud/campaign.sh                 # e1 ... e6
#   STEPS="e3 e4" experiments/cloud/campaign.sh   # a subset (E1 rows must exist)
#
# EXP names: <PREFIX>-<step>-bf100ms (PREFIX=cloud), rows in
# results/raw/<EXP>.csv; E1_EXP=<PREFIX>-e1-bf100ms for E2-E6. Resumable like
# every driver (a row that exists is skipped), so the script can simply be
# started again after an interruption.
#
# After every step the step's CSV, .failures, .params, the E2 Kafka-ops file
# and the per-run sidecars needed by plot.py / rq3_table.py (.ops.csv,
# .timeline, .wtimeline, .faults, .audit, .leaders) are copied to
# results/<PREFIX>-bf100ms/ (committed; results/raw is gitignored).
#
# Driver exit codes: 0 ok; 4 = some configurations missing (a backend without
# complete E1 rows is skipped), the campaign goes on; anything else (1 = harness
# or input error, 130 = Ctrl-C) stops it.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PREFIX="${PREFIX:-cloud}"
STEPS="${STEPS:-e1 e2 e3 e4 e5 e6}"
SNAP="$ROOT/results/$PREFIX-bf100ms"
export BATCH_FLUSH=100ms
export E1_EXP="$PREFIX-e1-bf100ms"
export NO_CAFFEINATE=1

log() { printf '%s [campaign] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

snapshot() {
  local exp="$1" raw="$ROOT/results/raw"
  mkdir -p "$SNAP/$exp"
  for f in "$raw/$exp.csv" "$raw/$exp.failures" "$raw/$exp.params" "$raw/$exp-kafka-ops.csv"; do
    [ -f "$f" ] && cp "$f" "$SNAP/"
  done
  if [ -d "$raw/$exp" ]; then
    find "$raw/$exp" -maxdepth 1 -type f \( -name '*.ops.csv' -o -name '*.timeline' \
      -o -name '*.wtimeline' -o -name '*.faults' -o -name '*.audit' -o -name '*.leaders' \) \
      -exec cp {} "$SNAP/$exp/" \;
  fi
}

for step in $STEPS; do
  case "$step" in
    e1) drv=e1_clients ;;
    e2) drv=e2_nodes ;;
    e3) drv=e3_block ;;
    e4) drv=e4_workload ;;
    e5) drv=e5_rtt ;;
    e6) drv=e6_faults ;;
    *) log "unknown step $step"; exit 1 ;;
  esac
  exp="$PREFIX-$step-bf100ms"
  log "start $step (EXP=$exp, BATCH_FLUSH=$BATCH_FLUSH, E1_EXP=$E1_EXP)"
  EXP="$exp" "experiments/exp/$drv.sh"
  rc=$?
  snapshot "$exp"
  log "end $step rc=$rc"
  case "$rc" in
    0 | 4) ;;
    *) log "stopping: $step exited $rc"; exit "$rc" ;;
  esac
done
log "campaign done"
