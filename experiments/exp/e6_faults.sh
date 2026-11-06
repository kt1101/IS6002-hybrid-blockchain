#!/usr/bin/env bash
# E6 (RQ3): behaviour under faults.
#
#   experiments/exp/e6_faults.sh [--dry-run]
#
# For each backend in kafka raft tendermint bft2 and each (N, f) in
# {(4,1), (7,2)}: CLIENTS = the backend's best from E1 (results/raw/e1.csv, see
# exp_lib.sh; FIXED_CLIENTS / BEST_CLIENTS_* override), block 100, YCSB-A, no
# RTT, COMMIT_MODE=sync, DURATION=180s, the fault injected at second 60 of the
# measurement window (experiments/faults.sh), REPS=3 reps each. The CSV column
# f holds f; the fault goes to the last nodes (fault_default_nodes in
# faults.sh: N=4 -> node4, N=7 -> node7,node6; node1, the raft bootstrap node,
# is not hit except by the leader faults below):
#   F1 crash        f nodes (docker stop)
#   F2 silent       f nodes (iptables drop of the consensus ports; kafka:
#                   veritas --fault=silent, votes withheld only, so ~1/N of
#                   the throughput remains by design, see NOTES.md)
#   F3 slow         f nodes (netem +FAULT_SLOW_MS=200 ms egress)
#   F4 wrong state  f nodes (veritas --fault=corrupt; never more than f)
#   F5 (tendermint and bft2 only) f+1 nodes, both flavours: F5-F2 (silent,
#                   liveness) and F5-F4 (corrupt, safety); F5_KINDS selects.
# Per (N, f): 4 faults x 4 backends + 2 flavours x 2 BFT backends = 20 configs,
# 40 over both sizes, 120 runs.
# Leader faults (E6_LEADER=1, the default; 0 turns them off): for each backend
# in E6_LEADER_BACKENDS (default "raft kafka") also F1:node1@AT and
# F3:node1@AT (crash / slow node1, one node). node1 bootstraps the raft
# configuration, so it is in practice always the raft leader when the fault
# hits (run_one.sh writes every "entering leader state" line to
# <tag>.leaders to confirm it); kafka has no leader node (the broker orders),
# so its node1 runs are the control. tendermint and bft2 rotate the
# proposer, so node1 is not special there. 2 faults x 2 backends x 2 (N, f) =
# 8 more configs, 24 more runs (144 in all).
# The auditor runs during every fault run (run_one.sh) and
# experiments/plot/rq3.py reads the .timeline/.faults/.audit files in
# results/raw/e6/.
#
# Open-loop load (phase 2, E6'): E6_RATE_<BACKEND> (E6_RATE_KAFKA, _RAFT,
# _TENDERMINT, _BFT2; e.g. 80% of the backend's E7 saturation throughput)
# or E6_RATE for every backend runs that backend's E6 runs with run_one.sh
# RATE=<rate> (the rate is part of the configuration, CSV column
# offered_rps); unset = closed loop as in phase 1.
# Rows in results/raw/e6.csv. The fault string is in the fault column
# (F1:node4@60, F1:node7+node6@60, F5-F4:node3+node4@60 ...). Note on the
# noise rule: the tps of a fault run averages the healthy and the faulty part,
# so a CV above 10% is common; EXTRA_REPS=0 turns the extra reps off.
#
# Other env: E6_NF="4:1 7:2" (N:f pairs), E6_AT=60 (injection second),
# E6_DURATION=180s, F5_KINDS="F5-F2 F5-F4", E6_LEADER=1,
# E6_LEADER_BACKENDS="raft kafka", E6_KINDS="F1 F2 F3 F4" (the f-node faults
# run for every backend; the leader faults are those of F1 F3 in E6_KINDS:
# e.g. E6_KINDS="F1 F2 F4" on a kernel without sch_netem, where F3 cannot be
# injected), and the common env, resumability, retry,
# noise rule and exit codes described in experiments/exp/exp_lib.sh.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/exp_lib.sh"

E6_NF="${E6_NF:-4:1 7:2}"
E6_AT="${E6_AT:-60}"
F5_KINDS="${F5_KINDS:-F5-F2 F5-F4}"
E6_LEADER="${E6_LEADER:-1}"
E6_LEADER_BACKENDS="${E6_LEADER_BACKENDS:-raft kafka}"
E6_KINDS="${E6_KINDS:-F1 F2 F3 F4}"
case "$E6_LEADER" in 0 | 1) ;; *) die "E6_LEADER must be 0 or 1, got '$E6_LEADER'" ;; esac
if [ -n "${DURATION:-}" ] && [ "$DURATION" != "${E6_DURATION:-180s}" ]; then
  log "note: DURATION=$DURATION is ignored: E6 runs DURATION=${E6_DURATION:-180s} (the fault is at second ${E6_AT}); set E6_DURATION to change it"
fi
DURATION="${E6_DURATION:-180s}"
exp_begin e6 "$@"
hbdb_require_uint E6_AT "$E6_AT"
[ -n "$E6_NF" ] || die "E6_NF must list at least one N:f pair"
for p in $E6_NF; do
  echo "$p" | grep -Eq '^[1-9][0-9]*:[1-9][0-9]*$' || die "E6_NF entries must look like 4:1, got '$p'"
  [ "${p##*:}" -eq $(((${p%%:*} - 1) / 3)) ] ||
    die "E6_NF entry '$p': f must be floor((N-1)/3) = $(((${p%%:*} - 1) / 3)) for N=${p%%:*} (the CSV f column and the F4/F5 node counts follow it)"
done
for k in $F5_KINDS; do
  case "$k" in F5-F2 | F5-F4) ;; *) die "F5_KINDS entries must be F5-F2 or F5-F4, got '$k'" ;; esac
done
[ -n "$E6_KINDS" ] || die "E6_KINDS must list at least one of F1 F2 F3 F4"
for k in $E6_KINDS; do
  case "$k" in F1 | F2 | F3 | F4) ;; *) die "E6_KINDS entries must be F1, F2, F3 or F4, got '$k'" ;; esac
done
E6_LEADER_KINDS=""
for k in F1 F3; do
  hbdb_in_list "$k" "$E6_KINDS" && E6_LEADER_KINDS="$E6_LEADER_KINDS $k"
done
for b in $E6_LEADER_BACKENDS; do
  hbdb_in_list "$b" "$EXP_BACKENDS_ALL" || die "E6_LEADER_BACKENDS: '$b' is not an E1-E6 backend (one of: $EXP_BACKENDS_ALL)"
done

# e6_rate BACKEND: the backend's open-loop rate (see the header), "" = closed loop.
e6_rate() {
  local v
  v="E6_RATE_$(echo "$1" | tr '[:lower:]' '[:upper:]')"
  eval "echo \"\${$v:-\${E6_RATE:-}}\""
}
for b in $EXP_BACKEND_LIST; do
  r="$(e6_rate "$b")"
  if [ -n "$r" ]; then
    exp_rate_ok "$r" && exp_rate_on "$r" || die "E6_RATE / E6_RATE_<BACKEND> for $b must be a positive number, got '$r'"
  fi
done

exp_plan() {
  for b in $EXP_BACKEND_LIST; do
    exp_resolve_clients "$b" || continue
    c="$EXP_RESOLVED_CLIENTS"
    rate="$(e6_rate "$b")"
    for p in $E6_NF; do
      n="${p%%:*}"
      f="${p##*:}"
      nodes="$(fault_default_nodes "$n" "$f")"
      for kind in $E6_KINDS; do
        exp_cfg "$b" "$n" "$f" "$c" 100 a 0 "$kind:$nodes@$E6_AT" $rate
      done
      if hbdb_in_list "$b" "tendermint bft2"; then
        nodes5="$(fault_default_nodes "$n" $((f + 1)))"
        for kind in $F5_KINDS; do
          exp_cfg "$b" "$n" "$f" "$c" 100 a 0 "$kind:$nodes5@$E6_AT" $rate
        done
      fi
      if [ "$E6_LEADER" = 1 ] && hbdb_in_list "$b" "$E6_LEADER_BACKENDS"; then
        for kind in $E6_LEADER_KINDS; do
          exp_cfg "$b" "$n" "$f" "$c" 100 a 0 "$kind:node1@$E6_AT" $rate
        done
      fi
    done
  done
}
exp_main
