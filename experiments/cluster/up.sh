#!/usr/bin/env bash
# Generate the compose file (and, for tm-orig and tendermint, the Tendermint
# testnet config)
# and start the cluster in the background.
#
# Env: BACKEND (required), N=4, BLOCK_SIZE=100, COMMIT_MODE=sync,
#      CPUS=0.5, MEM=768m (per node), YCSB_DIR=results/ycsb.
# Writes experiments/cluster/generated/{compose.json,tm/}.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

N="${N:-4}"
BLOCK_SIZE="${BLOCK_SIZE:-100}"
COMMIT_MODE="${COMMIT_MODE:-sync}"
CPUS="${CPUS:-0.5}"
MEM="${MEM:-768m}"

hbdb_check_backend
hbdb_check_n "$N"
hbdb_require_uint BLOCK_SIZE "$BLOCK_SIZE"

mkdir -p "$HBDB_GEN_DIR" "$HBDB_RESULTS_RAW" "$HBDB_YCSB_DIR"

if [ "$BACKEND" = tm-orig ] || [ "$BACKEND" = tendermint ]; then
  log "generating tendermint testnet config for $N validators"
  # rm runs inside the container so files it created earlier (owned by root
  # on a Linux host) can be removed too.
  docker run --rm -v "$HBDB_GEN_DIR:/gen" hbdb-node bash -c \
    "rm -rf /gen/tm && tendermint testnet --v $N --o /gen/tm" >&2
  # testnet writes hostname-based peers (nodeK); tm_config.py replaces them
  # with the node IPs and applies the original config tweaks (plus, for
  # tendermint, the larger tx/body/block limits: one tx is a Veritas block).
  python3 "$HBDB_CLUSTER_DIR/tm_config.py" --dir "$HBDB_GEN_DIR/tm" --n "$N" --backend "$BACKEND"
fi

log "generating compose: backend=$BACKEND n=$N block_size=$BLOCK_SIZE commit_mode=$COMMIT_MODE cpus=$CPUS mem=$MEM"
python3 "$HBDB_CLUSTER_DIR/gen_compose.py" \
  --backend "$BACKEND" --n "$N" --block-size "$BLOCK_SIZE" --commit-mode "$COMMIT_MODE" \
  --cpus "$CPUS" --mem "$MEM" \
  --tm-dir "$HBDB_GEN_DIR/tm" --ycsb-dir "$HBDB_YCSB_DIR" --results-dir "$HBDB_RESULTS_RAW" \
  --out "$HBDB_COMPOSE_FILE"

log "starting cluster (project $HBDB_PROJECT, $HBDB_COMPOSE_FILE)"
# For kafka backends this blocks until the broker healthcheck passes (topic created).
hbdb_compose up -d
hbdb_compose ps >&2
log "cluster started"
