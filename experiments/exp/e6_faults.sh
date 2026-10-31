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
# is not hit):
#   F1 crash        f nodes (docker stop)
#   F2 silent       f nodes (iptables drop of the consensus ports; kafka:
#                   veritas --fault=silent, votes withheld only, so ~1/N of
#                   the throughput remains by design, see NOTES.md)
#   F3 slow         f nodes (netem +FAULT_SLOW_MS=200 ms egress)
#   F4 wrong state  f nodes (veritas --fault=corrupt; never more than f)
#   F5 (tendermint and bft2 only) f+1 nodes, both flavours: F5-F2 (silent,
#                   liveness) and F5-F4 (corrupt, safety); F5_KINDS selects.
# Per (N, f): 4 faults x 4 backends + 2 flavours x 2 BFT backends = 20 configs,
# 40 over both sizes, 120 runs. The auditor runs during every fault run
# (run_one.sh) and experiments/plot/rq3.py reads the .timeline/.faults/.audit
# files in results/raw/e6/.
#
# Rows in results/raw/e6.csv. The fault string is in the fault column
# (F1:node4@60, F1:node7+node6@60, F5-F4:node3+node4@60 ...). Note on the
# noise rule: the tps of a fault run averages the healthy and the faulty part,
# so a CV above 10% is common; EXTRA_REPS=0 turns the extra reps off.
#
# Other env: E6_NF="4:1 7:2" (N:f pairs), E6_AT=60 (injection second),
# E6_DURATION=180s, F5_KINDS="F5-F2 F5-F4", and the common env, resumability,
# retry, noise rule and exit codes described in experiments/exp/exp_lib.sh.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/exp_lib.sh"

E6_NF="${E6_NF:-4:1 7:2}"
E6_AT="${E6_AT:-60}"
F5_KINDS="${F5_KINDS:-F5-F2 F5-F4}"
DURATION="${E6_DURATION:-180s}"
exp_begin e6 "$@"
hbdb_require_uint E6_AT "$E6_AT"
[ -n "$E6_NF" ] || die "E6_NF must list at least one N:f pair"
for p in $E6_NF; do
  echo "$p" | grep -Eq '^[1-9][0-9]*:[1-9][0-9]*$' || die "E6_NF entries must look like 4:1, got '$p'"
done
for k in $F5_KINDS; do
  case "$k" in F5-F2 | F5-F4) ;; *) die "F5_KINDS entries must be F5-F2 or F5-F4, got '$k'" ;; esac
done

for b in $EXP_BACKEND_LIST; do
  exp_resolve_clients "$b" || continue
  c="$EXP_RESOLVED_CLIENTS"
  for p in $E6_NF; do
    n="${p%%:*}"
    f="${p##*:}"
    nodes="$(fault_default_nodes "$n" "$f")"
    for kind in F1 F2 F3 F4; do
      exp_cfg "$b" "$n" "$f" "$c" 100 a 0 "$kind:$nodes@$E6_AT"
    done
    if hbdb_in_list "$b" "tendermint bft2"; then
      nodes5="$(fault_default_nodes "$n" $((f + 1)))"
      for kind in $F5_KINDS; do
        exp_cfg "$b" "$n" "$f" "$c" 100 a 0 "$kind:$nodes5@$E6_AT"
      done
    fi
  done
done
exp_finish
