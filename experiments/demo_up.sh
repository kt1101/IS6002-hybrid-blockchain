#!/usr/bin/env bash
# Start the cluster for the live demo's docker mode and wait until it serves
# requests (spec docs/superpowers/specs/2026-11-02-live-demo-design.md 5.1).
#
#   BACKEND=bft2 N=4 experiments/demo_up.sh
#
# A wrapper around cluster/up.sh with what the demo needs: node gRPC/observer
# ports and the TSO published on 127.0.0.1 (PUBLISH_PORTS=1), sync commit,
# a 200 ms batch flush (hand-typed Sets commit quickly), and the faults the
# demo cannot add later armed at start: node3 corrupt, and on kafka node4
# silent (veritas --fault, activated by experiments/demo_fault.sh).
# CPUS and MEM are left to up.sh's defaults (as in the experiments) unless set.
#
# Env: BACKEND (required: kafka, raft, tendermint or bft2), N=4,
#      DEMO_DRY_RUN=1 (print the commands instead of running them).
# Bash 3.2 compatible (runs on the macOS host).
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/experiments/lib.sh"

N="${N:-4}"
hbdb_check_backend
hbdb_check_n "$N"
hbdb_in_list "$BACKEND" "kafka raft tendermint bft2" ||
  die "BACKEND=$BACKEND has no demo observer; the demo supports: kafka raft tendermint bft2"

node_faults=""
demo_flags=""
if [ "$N" -ge 3 ]; then
  node_faults="node3=corrupt"
else
  demo_flags="$demo_flags --armed-corrupt=none"
fi
if [ "$BACKEND" = kafka ]; then
  if [ "$N" -ge 4 ]; then
    node_faults="$node_faults node4=silent"
  else
    demo_flags="$demo_flags --armed-silent=none"
  fi
fi
node_faults="${node_faults# }"

export BACKEND N
export PUBLISH_PORTS=1 COMMIT_MODE=sync BATCH_FLUSH=200ms
export NODE_FAULTS="$node_faults"

up="$HBDB_CLUSTER_DIR/up.sh"
health="$HBDB_CLUSTER_DIR/health.sh"
if [ "${DEMO_DRY_RUN:-}" = 1 ]; then
  echo "BACKEND=$BACKEND N=$N PUBLISH_PORTS=1 COMMIT_MODE=sync BATCH_FLUSH=200ms NODE_FAULTS=\"$NODE_FAULTS\" $up"
  echo "BACKEND=$BACKEND N=$N $health"
else
  "$up"
  "$health"
fi

echo
echo "Cluster is up. Start the demo with:"
echo "  go run ./cmd/demo --mode=docker --backend=$BACKEND --n=$N$demo_flags"
