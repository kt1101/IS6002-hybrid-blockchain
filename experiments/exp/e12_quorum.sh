#!/usr/bin/env bash
# E12 (phase 3, RQ7): the attestation quorum against f+1 colluding
# wrong-state nodes (F5-F4)
# (docs/superpowers/specs/2026-11-09-phase3-accountability-design.md, section 7).
#
#   experiments/exp/e12_quorum.sh [--dry-run]
#
# Matrix: BACKENDS (default "raft bft2") x ATTEST_QUORUM (E12_QUORUMS,
# default "f+1 2f+1") x read mode (E12_READS, default "certified quorum") x N
# (NS, default "4"; "4 7" only with a Docker VM >= 10 GiB, see NOTES),
# FAULT=F5-F4 on f+1 nodes, CLIENTS=256, ATTEST_EVERY=1, FAILOVER=1,
# MEM=1g, flush 100 ms, DURATION=E12_DURATION (180s), the fault at second
# E12_AT (60), REPS=3: 2 x 2 x 2 x 1 x 3 = 24 runs (48 with NS="4 7").
# One EXP per (quorum, read), <E12_PREFIX>-q<f1|2f1>-<read> (e12-qf1-certified,
# ...); this script runs itself once per pair (E12_PAIR, internal). Load:
# open loop at exp_rate_for E12 <backend>; unset = closed loop.
#
# Exit: the worst exit of the pairs (0, 4 = missing rows, 1 = aborted).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

E12_QUORUMS="${E12_QUORUMS:-f+1 2f+1}"
E12_READS="${E12_READS:-certified quorum}"
E12_PREFIX="${E12_PREFIX:-e12}"

if [ -z "${E12_PAIR:-}" ]; then
  . "$HERE/../lib.sh"
  for a in "$@"; do
    case "$a" in --dry-run | -h | --help) ;; *) die "unknown argument '$a' (only --dry-run and --help)" ;; esac
  done
  for q in $E12_QUORUMS; do
    case "$q" in f+1 | 2f+1) ;; *) die "E12_QUORUMS entries must be f+1 or 2f+1, got '$q'" ;; esac
  done
  for rd in $E12_READS; do
    case "$rd" in certified | quorum) ;; *) die "E12_READS entries must be certified or quorum, got '$rd'" ;; esac
  done
  . "$HERE/interleave.sh"
  EXP_CHILDREN=""
  for q in $E12_QUORUMS; do
    ql="$(echo "$q" | tr -d '+')"
    for rd in $E12_READS; do
      EXP_CHILDREN="$EXP_CHILDREN
E12_PAIR=$q:$rd EXP=$E12_PREFIX-q$ql-$rd ATTEST_EVERY=1 ATTEST_QUORUM=$q READ_MODE=$rd FAILOVER=1"
    done
  done
  rc=0
  exp_interleave "$0" "$@" || rc=$?
  exit "$rc"
fi

. "$HERE/exp_lib.sh"
E12_CLIENTS=256
E12_AT="${E12_AT:-60}"
NS="${NS:-4}"
BACKENDS="${BACKENDS:-raft bft2}"
MEM="${MEM:-1g}"
BATCH_FLUSH="${BATCH_FLUSH:-100ms}"
if [ -n "${DURATION:-}" ] && [ "$DURATION" != "${E12_DURATION:-180s}" ]; then
  log "note: DURATION=$DURATION is ignored: E12 runs DURATION=${E12_DURATION:-180s}; set E12_DURATION to change it"
fi
DURATION="${E12_DURATION:-180s}"
export MEM BATCH_FLUSH ATTEST_EVERY ATTEST_QUORUM READ_MODE FAILOVER
EXP_DEFAULT_BATCH_FLUSH=100ms
exp_begin "$EXP" "$@"
hbdb_require_uint E12_AT "$E12_AT"
for n in $NS; do
  hbdb_require_uint NS "$n"
  [ "$n" -ge 4 ] || die "NS entries must be >= 4 (F5-F4 needs f >= 1), got $n"
done
for b in $EXP_BACKEND_LIST; do
  r="$(exp_rate_for E12 "$b")"
  if [ -n "$r" ]; then
    exp_rate_ok "$r" && exp_rate_on "$r" || die "E12_RATE / E12_RATE_<BACKEND> for $b must be a positive number, got '$r'"
  fi
done

exp_plan() {
  local b n f
  for b in $EXP_BACKEND_LIST; do
    for n in $NS; do
      f=$(((n - 1) / 3))
      exp_cfg "$b" "$n" "$f" "$E12_CLIENTS" 100 a 0 "F5-F4:$(fault_default_nodes "$n" $((f + 1)))@$E12_AT" $(exp_rate_for E12 "$b")
    done
  done
}
exp_main
