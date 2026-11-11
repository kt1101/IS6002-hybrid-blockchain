#!/usr/bin/env bash
# E9 (phase 2, RQ5): in-protocol root attestations against a wrong-state
# node (docs/superpowers/specs/2026-11-03-phase2-fair-comparison-and-verifiability-design.md, B2).
#
#   experiments/exp/e9_attest.sh [--dry-run]
#
# Matrix: BACKENDS (default "raft bft2") x ATTEST_EVERY (E9_ATTEST, default
# "0 1 10") x FAULT (E9_FAULTS, default "none F4 F5-F4": F4 = f=1 node
# corrupt, F5-F4 = f+1 = 2 nodes, bft2 only) x READ_MODE (plain, and
# certified when ATTEST_EVERY > 0), N=4, CLIENTS=256, MEM=1g, flush 100 ms,
# DURATION=E9_DURATION (180s), the fault at second E9_AT (60) of the
# measurement window, REPS=3: raft 10 + bft2 15 configurations = 75 runs.
# The CSV has no column for ATTEST_EVERY / READ_MODE: every pair writes its
# own EXP <E9_PREFIX>-a<K>-<read> (e9-a0-plain, e9-a1-plain, e9-a1-certified,
# e9-a10-plain, e9-a10-certified); this script runs itself once per pair
# (E9_PAIR, internal). Load: open loop at E9_RATE_<BACKEND> (e9_rate of
# saturation.py: 80% of the largest E7 rate with write p99 <= 1 s) or
# E9_RATE; unset = closed loop. With
# ATTEST_EVERY > 0 run_one.sh keeps every "core: diverged at height" line of
# the node logs in <tag>.diverged; experiments/plot/rq5.py reads them with
# the .faults, .audit and .ops.csv sidecars.
#
# Exit: the worst exit of the pairs (0, 4 = missing rows, 1 = aborted).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

E9_ATTEST="${E9_ATTEST:-0 1 10}"
E9_FAULTS="${E9_FAULTS:-none F4 F5-F4}"
E9_PREFIX="${E9_PREFIX:-e9}"

if [ -z "${E9_PAIR:-}" ]; then
  . "$HERE/../lib.sh"
  for a in "$@"; do
    case "$a" in --dry-run | -h | --help) ;; *) die "unknown argument '$a' (only --dry-run and --help)" ;; esac
  done
  for k in $E9_ATTEST; do hbdb_require_uint E9_ATTEST "$k"; done
  for f in $E9_FAULTS; do
    case "$f" in none | F4 | F5-F4) ;; *) die "E9_FAULTS entries must be none, F4 or F5-F4, got '$f'" ;; esac
  done
  . "$HERE/interleave.sh"
  EXP_CHILDREN=""
  for k in $E9_ATTEST; do
    reads=plain
    [ "$k" = 0 ] || reads="plain certified"
    for rd in $reads; do
      EXP_CHILDREN="$EXP_CHILDREN
E9_PAIR=$k:$rd EXP=$E9_PREFIX-a$k-$rd ATTEST_EVERY=$k READ_MODE=$rd"
    done
  done
  rc=0
  exp_interleave "$0" "$@" || rc=$?
  exit "$rc"
fi

. "$HERE/exp_lib.sh"
E9_CLIENTS=256
E9_AT="${E9_AT:-60}"
BACKENDS="${BACKENDS:-raft bft2}"
MEM="${MEM:-1g}"
BATCH_FLUSH="${BATCH_FLUSH:-100ms}"
if [ -n "${DURATION:-}" ] && [ "$DURATION" != "${E9_DURATION:-180s}" ]; then
  log "note: DURATION=$DURATION is ignored: E9 runs DURATION=${E9_DURATION:-180s}; set E9_DURATION to change it"
fi
DURATION="${E9_DURATION:-180s}"
export MEM BATCH_FLUSH ATTEST_EVERY READ_MODE
EXP_DEFAULT_BATCH_FLUSH=100ms
exp_begin "$EXP" "$@"
hbdb_require_uint E9_AT "$E9_AT"

e9_rate() {
  local v
  v="E9_RATE_$(echo "$1" | tr '[:lower:]' '[:upper:]')"
  eval "echo \"\${$v:-\${E9_RATE:-}}\""
}
for b in $EXP_BACKEND_LIST; do
  r="$(e9_rate "$b")"
  if [ -n "$r" ]; then
    exp_rate_ok "$r" && exp_rate_on "$r" || die "E9_RATE / E9_RATE_<BACKEND> for $b must be a positive number, got '$r'"
  fi
done
exp_params_rates $(for b in $EXP_BACKEND_LIST; do echo "$b=$(e9_rate "$b")"; done)

exp_plan() {
  local b kind fault
  for b in $EXP_BACKEND_LIST; do
    for kind in $E9_FAULTS; do
      case "$kind" in
        none) fault=none ;;
        F4) fault="F4:$(fault_default_nodes 4 1)@$E9_AT" ;;
        F5-F4)
          hbdb_in_list "$b" "tendermint bft2" || continue
          fault="F5-F4:$(fault_default_nodes 4 2)@$E9_AT"
          ;;
      esac
      exp_cfg "$b" 4 1 "$E9_CLIENTS" 100 a 0 "$fault" $(e9_rate "$b")
    done
  done
}
exp_main
