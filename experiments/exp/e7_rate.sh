#!/usr/bin/env bash
# E7 (phase 2, RQ1/RQ2): open-loop rate sweep for the saturation throughput of
# every backend (docs/superpowers/specs/2026-11-03-phase2-fair-comparison-and-verifiability-design.md, A1).
#
#   EXP=e7 experiments/exp/e7_rate.sh [--dry-run]
#   EXP=e7f-kafka KAFKA_CPUS=0.5 KAFKA_MEM=768m BACKENDS=kafka experiments/exp/e7_rate.sh
#   EXP=e7f-tm100 TM_TIMEOUT_COMMIT=100ms BACKENDS=tendermint experiments/exp/e7_rate.sh
#
# Fixed: N=4, F=0, BLOCK_SIZE=100, WORKLOAD=a, FAULT=none, CLIENTS=256 (gRPC
# drivers the open-loop scheduler spreads the operations over), MEM=1g,
# BATCH_FLUSH=100ms (defaults; another value is recorded by the params guard).
# Env: RATES="250 500 1000 1500 2000 3000 4000" (total req/s, run_one.sh
# RATE), TM_RATES="50 100 200 300 400 600" (tendermint), MAX_INFLIGHT,
# KAFKA_CPUS, KAFKA_MEM, TM_TIMEOUT_COMMIT (run_one.sh; one value per EXP,
# params guard), and the common env of exp_lib.sh (REPS, BACKENDS, ...).
# The rate is part of the configuration (CSV column offered_rps), so the
# rates of one backend share the EXP. experiments/plot/saturation.py reads
# the CSV and the .ops.csv sidecars.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/exp_lib.sh"

RATES="${RATES:-250 500 1000 1500 2000 3000 4000}"
TM_RATES="${TM_RATES:-50 100 200 300 400 600}"
E7_CLIENTS=256
MEM="${MEM:-1g}"
BATCH_FLUSH="${BATCH_FLUSH:-100ms}"
export MEM BATCH_FLUSH
EXP_DEFAULT_BATCH_FLUSH=100ms
exp_begin e7 "$@"
for r in $RATES $TM_RATES; do
  exp_rate_ok "$r" && exp_rate_on "$r" || die "RATES/TM_RATES entries must be positive numbers, got '$r'"
done
[ -n "$RATES" ] && [ -n "$TM_RATES" ] || die "RATES and TM_RATES must not be empty"

exp_plan() {
  local b rates r
  for b in $EXP_BACKEND_LIST; do
    rates="$RATES"
    [ "$b" != tendermint ] || rates="$TM_RATES"
    for r in $rates; do
      exp_cfg "$b" 4 0 "$E7_CLIENTS" 100 a 0 none "$r"
    done
  done
}
exp_main
