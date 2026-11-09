#!/usr/bin/env bash
# E8 (phase 2, RQ4): the cost of real Ed25519 signatures and verified reads
# (docs/superpowers/specs/2026-11-03-phase2-fair-comparison-and-verifiability-design.md, B1).
#
#   experiments/exp/e8_verify.sh [--dry-run]
#
# Matrix: BACKENDS (default "raft bft2": one CFT, one BFT; the read path and
# the signatures do not depend on the backend) x WORKLOAD {a, c} x
# VERIFY_SIGS (E8_SIGS, default "none ingress all") x READ_MODE (E8_READS,
# default "plain verified"), N=4, block 100, CLIENTS=256, MEM=1g, flush
# 100 ms, REPS=3: 2 x 2 x 6 x 3 = 72 runs. The CSV has no column for
# VERIFY_SIGS / READ_MODE, so every pair writes its own EXP
# <E8_PREFIX>-<sigs>-<read> (default e8-none-plain ... e8-all-verified);
# this script runs itself once per pair (E8_PAIR, internal). Load: open
# loop at E8_RATE_<BACKEND> (e.g. 80% of the backend's E7 saturation
# throughput) or E8_RATE for every backend; unset = closed loop.
# ycsbbench signs every Set when VERIFY_SIGS != none (run_one.sh SIGN).
# The .ops.csv sidecars carry read_bytes_mean and verify_cpu_us_mean.
#
# Exit: the worst exit of the pairs (0, 4 = missing rows, 1 = aborted).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

E8_SIGS="${E8_SIGS:-none ingress all}"
E8_READS="${E8_READS:-plain verified}"
E8_PREFIX="${E8_PREFIX:-e8}"

if [ -z "${E8_PAIR:-}" ]; then
  . "$HERE/../lib.sh"
  for a in "$@"; do
    case "$a" in --dry-run | -h | --help) ;; *) die "unknown argument '$a' (only --dry-run and --help)" ;; esac
  done
  for v in $E8_SIGS; do
    case "$v" in none | ingress | all) ;; *) die "E8_SIGS entries must be none, ingress or all, got '$v'" ;; esac
  done
  for v in $E8_READS; do
    case "$v" in plain | verified) ;; *) die "E8_READS entries must be plain or verified, got '$v'" ;; esac
  done
  worst=0
  for sg in $E8_SIGS; do
    for rd in $E8_READS; do
      rc=0
      E8_PAIR="$sg:$rd" EXP="$E8_PREFIX-$sg-$rd" VERIFY_SIGS="$sg" READ_MODE="$rd" "$0" "$@" || rc=$?
      case "$rc" in
        0) ;;
        4) [ "$worst" -ne 0 ] || worst=4 ;;
        *) exit "$rc" ;;
      esac
    done
  done
  exit "$worst"
fi

. "$HERE/exp_lib.sh"
E8_CLIENTS=256
BACKENDS="${BACKENDS:-raft bft2}"
MEM="${MEM:-1g}"
BATCH_FLUSH="${BATCH_FLUSH:-100ms}"
export MEM BATCH_FLUSH VERIFY_SIGS READ_MODE
EXP_DEFAULT_BATCH_FLUSH=100ms
exp_begin "$EXP" "$@"

e8_rate() {
  local v
  v="E8_RATE_$(echo "$1" | tr '[:lower:]' '[:upper:]')"
  eval "echo \"\${$v:-\${E8_RATE:-}}\""
}
for b in $EXP_BACKEND_LIST; do
  r="$(e8_rate "$b")"
  if [ -n "$r" ]; then
    exp_rate_ok "$r" && exp_rate_on "$r" || die "E8_RATE / E8_RATE_<BACKEND> for $b must be a positive number, got '$r'"
  fi
done

exp_plan() {
  local b w
  for b in $EXP_BACKEND_LIST; do
    for w in a c; do
      exp_cfg "$b" 4 0 "$E8_CLIENTS" 100 "$w" 0 none $(e8_rate "$b")
    done
  done
}
exp_main
