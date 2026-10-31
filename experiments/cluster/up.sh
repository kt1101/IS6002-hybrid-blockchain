#!/usr/bin/env bash
# Generate the compose file (and, for tm-orig and tendermint, the Tendermint
# testnet config; bft2 needs no generated config: bft2-node derives its keys
# from a shared seed and the replica ID)
# and start the cluster in the background.
#
# Env: BACKEND (required), N=4, BLOCK_SIZE=100, COMMIT_MODE=sync,
#      CPUS=0.5, MEM=768m (per node), YCSB_DIR=results/ycsb,
#      NODE_FAULTS="" (space-separated node<i>=silent|corrupt, from
#      run_one.sh / faults.sh fault_node_faults: those nodes' veritas runs
#      with --fault armed, see gen_compose.py --node-fault).
# Writes experiments/cluster/generated/{compose.json,tm/}.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

N="${N:-4}"
BLOCK_SIZE="${BLOCK_SIZE:-100}"
COMMIT_MODE="${COMMIT_MODE:-sync}"
CPUS="${CPUS:-0.5}"
MEM="${MEM:-768m}"
NODE_FAULTS="${NODE_FAULTS:-}"

hbdb_check_backend
hbdb_check_n "$N"
hbdb_require_uint BLOCK_SIZE "$BLOCK_SIZE"
# Word-split list (no arrays: bash 3.2 under set -u); every item is checked
# so the unquoted expansion below is safe.
node_fault_args=""
for nf in $NODE_FAULTS; do
  echo "$nf" | grep -Eq '^node[1-9][0-9]*=[a-z]+$' || die "NODE_FAULTS item '$nf': want node<i>=<fault>, e.g. node4=corrupt"
  node_fault_args="$node_fault_args --node-fault $nf"
done

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

log "generating compose: backend=$BACKEND n=$N block_size=$BLOCK_SIZE commit_mode=$COMMIT_MODE cpus=$CPUS mem=$MEM${NODE_FAULTS:+ node_faults=$NODE_FAULTS}"
# shellcheck disable=SC2086
python3 "$HBDB_CLUSTER_DIR/gen_compose.py" \
  --backend "$BACKEND" --n "$N" --block-size "$BLOCK_SIZE" --commit-mode "$COMMIT_MODE" \
  --cpus "$CPUS" --mem "$MEM" $node_fault_args \
  --tm-dir "$HBDB_GEN_DIR/tm" --ycsb-dir "$HBDB_YCSB_DIR" --results-dir "$HBDB_RESULTS_RAW" \
  --out "$HBDB_COMPOSE_FILE"

log "starting cluster (project $HBDB_PROJECT, $HBDB_COMPOSE_FILE)"
# For kafka backends this blocks until the broker healthcheck passes (topic created).
hbdb_compose up -d
hbdb_compose ps >&2
log "cluster started"
