# shellcheck shell=bash
# The backends the E1-E6 drivers compare, and which of them can run now.
# Source it after lib.sh (exp_lib.sh does); or execute it to print the list:
#   experiments/exp/backends.sh          # e.g. "kafka raft tendermint bft2"
#   BACKENDS="raft bft2" experiments/exp/backends.sh
#
# EXP_BACKENDS_ALL: the four comparison backends of the plan (the original
# servers kafka-orig and tm-orig are the P0 baseline, not part of E1-E6).
# exp_backends prints the ones a driver runs, in order, space-separated:
#   - BACKENDS (env, optional) narrows or reorders the list (every name must
#     be one of EXP_BACKENDS_ALL);
#   - a backend that lib.sh does not list in HBDB_IMPLEMENTED_BACKENDS yet
#     (e.g. bft2 before it is merged) is skipped with a note on stderr, so a
#     driver can run for the backends that exist.
# Bash 3.2 compatible.

EXP_BACKENDS_ALL="kafka raft tendermint bft2"

exp_backends() {
  local want="${BACKENDS:-$EXP_BACKENDS_ALL}" b out=""
  for b in $want; do
    hbdb_in_list "$b" "$EXP_BACKENDS_ALL" ||
      die "BACKENDS: '$b' is not an E1-E6 backend (one of: $EXP_BACKENDS_ALL)"
    if ! hbdb_in_list "$b" "$HBDB_IMPLEMENTED_BACKENDS"; then
      log "backend $b is not implemented yet: skipped"
      continue
    fi
    hbdb_in_list "$b" "$out" || out="${out:+$out }$b"
  done
  echo "$out"
}

# Executed (not sourced): print the list.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  set -euo pipefail
  . "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"
  exp_backends
fi
