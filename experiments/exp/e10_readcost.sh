#!/usr/bin/env bash
# E10 (phase 3, RQ6/RQ7 without faults): the cost of the read modes
# (docs/superpowers/specs/2026-11-09-phase3-accountability-design.md, section 7).
#
#   experiments/exp/e10_readcost.sh [--dry-run]
#
# Matrix: BACKENDS (default "raft bft2") x read mode (E10_READS, default
# "plain certified certified-signall quorum") x WORKLOAD (E10_WORKLOADS,
# default "a c"), N=4, f=1, CLIENTS=256, block 100, FAULT=none, MEM=1g,
# flush 100 ms, ATTEST_EVERY=1 for every mode (the attestation cost is the
# same in all of them), REPS=3: 2 x 4 x 2 x 3 = 48 runs. Read modes:
#   plain             READ_MODE=plain
#   certified         READ_MODE=certified
#   certified-signall READ_MODE=certified SIGN_READS=always
#   quorum            READ_MODE=quorum (plain Gets from f+1 nodes, compared)
# One EXP per mode, <E10_PREFIX>-<mode> (e10-plain, ...), since the CSV has
# no column for it; this script runs itself once per mode (E10_MODE,
# internal). Load: open loop at exp_rate_for E10 <backend> (E10_RATE_<B>,
# E10_RATE, else E9's); unset = closed loop.
#
# Exit: the worst exit of the modes (0, 4 = missing rows, 1 = aborted).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

E10_READS="${E10_READS:-plain certified certified-signall quorum}"
E10_PREFIX="${E10_PREFIX:-e10}"

if [ -z "${E10_MODE:-}" ]; then
  . "$HERE/../lib.sh"
  for a in "$@"; do
    case "$a" in --dry-run | -h | --help) ;; *) die "unknown argument '$a' (only --dry-run and --help)" ;; esac
  done
  for m in $E10_READS; do
    case "$m" in plain | certified | certified-signall | quorum) ;; *) die "E10_READS entries must be plain, certified, certified-signall or quorum, got '$m'" ;; esac
  done
  worst=0
  for m in $E10_READS; do
    rd="$m" sr=off
    [ "$m" != certified-signall ] || { rd=certified; sr=always; }
    rc=0
    E10_MODE="$m" EXP="$E10_PREFIX-$m" ATTEST_EVERY=1 READ_MODE="$rd" SIGN_READS="$sr" "$0" "$@" || rc=$?
    case "$rc" in
      0) ;;
      4) [ "$worst" -ne 0 ] || worst=4 ;;
      *) exit "$rc" ;;
    esac
  done
  exit "$worst"
fi

. "$HERE/exp_lib.sh"
E10_CLIENTS=256
E10_WORKLOADS="${E10_WORKLOADS:-a c}"
BACKENDS="${BACKENDS:-raft bft2}"
MEM="${MEM:-1g}"
BATCH_FLUSH="${BATCH_FLUSH:-100ms}"
export MEM BATCH_FLUSH ATTEST_EVERY READ_MODE SIGN_READS
EXP_DEFAULT_BATCH_FLUSH=100ms
exp_begin "$EXP" "$@"
for b in $EXP_BACKEND_LIST; do
  r="$(exp_rate_for E10 "$b")"
  if [ -n "$r" ]; then
    exp_rate_ok "$r" && exp_rate_on "$r" || die "E10_RATE / E10_RATE_<BACKEND> for $b must be a positive number, got '$r'"
  fi
done

exp_plan() {
  local b w
  for b in $EXP_BACKEND_LIST; do
    for w in $E10_WORKLOADS; do
      exp_cfg "$b" 4 1 "$E10_CLIENTS" 100 "$w" 0 none $(exp_rate_for E10 "$b")
    done
  done
}
exp_main
