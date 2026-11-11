#!/usr/bin/env bash
# E11 (phase 3, RQ6): in-protocol accountability against a wrong-state (F4)
# or read-lying (F6, F6E) node
# (docs/superpowers/specs/2026-11-09-phase3-accountability-design.md, section 7).
#
#   experiments/exp/e11_account.sh [--dry-run]
#
# Matrix: BACKENDS (default "raft bft2") x (fault, client mode) pairs
# (E11_PAIRS, default "F4:plain F4:cert-fo F4:cert-fo-sign F6:plain
# F6:cert-fo F6:cert-fo-sign F6E:cert-fo-sign"), N=4, f=1 (the fault hits
# node4), CLIENTS=256, ATTEST_EVERY=1, MEM=1g, flush 100 ms,
# DURATION=E11_DURATION (180s), the fault at second E11_AT (60), REPS=3:
# 2 x 7 x 3 = 42 runs. Client modes:
#   plain         READ_MODE=plain
#   cert-fo       READ_MODE=certified FAILOVER=1
#   cert-fo-sign  READ_MODE=certified FAILOVER=1 SIGN_READS=on-failure
# One EXP per pair, <E11_PREFIX>-<fault>-<mode> (e11-F6-cert-fo-sign, ...);
# this script runs itself once per pair (E11_PAIR, internal). Load: open loop
# at exp_rate_for E11 <backend>; unset = closed loop. run_one.sh keeps the
# .evict / .certs / .diverged lines; experiments/plot/rq6.py reads them.
#
# Exit: the worst exit of the pairs (0, 4 = missing rows, 1 = aborted).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

E11_PAIRS="${E11_PAIRS:-F4:plain F4:cert-fo F4:cert-fo-sign F6:plain F6:cert-fo F6:cert-fo-sign F6E:cert-fo-sign}"
E11_PREFIX="${E11_PREFIX:-e11}"

if [ -z "${E11_PAIR:-}" ]; then
  . "$HERE/../lib.sh"
  for a in "$@"; do
    case "$a" in --dry-run | -h | --help) ;; *) die "unknown argument '$a' (only --dry-run and --help)" ;; esac
  done
  for p in $E11_PAIRS; do
    case "${p%%:*}" in F4 | F6 | F6E) ;; *) die "E11_PAIRS: fault must be F4, F6 or F6E, got '$p'" ;; esac
    case "${p#*:}" in plain | cert-fo | cert-fo-sign) ;; *) die "E11_PAIRS: mode must be plain, cert-fo or cert-fo-sign, got '$p'" ;; esac
  done
  . "$HERE/interleave.sh"
  EXP_CHILDREN=""
  for p in $E11_PAIRS; do
    k="${p%%:*}" m="${p#*:}"
    case "$m" in
      plain) rd=plain fo=0 sr=off ;;
      cert-fo) rd=certified fo=1 sr=off ;;
      cert-fo-sign) rd=certified fo=1 sr=on-failure ;;
    esac
    EXP_CHILDREN="$EXP_CHILDREN
E11_PAIR=$p E11_KIND=$k EXP=$E11_PREFIX-$k-$m ATTEST_EVERY=1 READ_MODE=$rd FAILOVER=$fo SIGN_READS=$sr"
  done
  rc=0
  exp_interleave "$0" "$@" || rc=$?
  exit "$rc"
fi

. "$HERE/exp_lib.sh"
E11_CLIENTS=256
E11_AT="${E11_AT:-60}"
BACKENDS="${BACKENDS:-raft bft2}"
MEM="${MEM:-1g}"
BATCH_FLUSH="${BATCH_FLUSH:-100ms}"
if [ -n "${DURATION:-}" ] && [ "$DURATION" != "${E11_DURATION:-180s}" ]; then
  log "note: DURATION=$DURATION is ignored: E11 runs DURATION=${E11_DURATION:-180s}; set E11_DURATION to change it"
fi
DURATION="${E11_DURATION:-180s}"
export MEM BATCH_FLUSH ATTEST_EVERY READ_MODE FAILOVER SIGN_READS
EXP_DEFAULT_BATCH_FLUSH=100ms
exp_begin "$EXP" "$@"
hbdb_require_uint E11_AT "$E11_AT"
for b in $EXP_BACKEND_LIST; do
  r="$(exp_rate_for E11 "$b")"
  if [ -n "$r" ]; then
    exp_rate_ok "$r" && exp_rate_on "$r" || die "E11_RATE / E11_RATE_<BACKEND> for $b must be a positive number, got '$r'"
  fi
done

exp_plan() {
  local b
  for b in $EXP_BACKEND_LIST; do
    exp_cfg "$b" 4 1 "$E11_CLIENTS" 100 a 0 "$E11_KIND:$(fault_default_nodes 4 1)@$E11_AT" $(exp_rate_for E11 "$b")
  done
}
exp_main
