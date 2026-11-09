#!/usr/bin/env bash
# Generate the compose file (and, for tm-orig and tendermint, the Tendermint
# testnet config; bft2 needs no generated config: bft2-node derives its keys
# from a shared seed and the replica ID)
# and start the cluster in the background.
#
# Env: BACKEND (required), N=4, BLOCK_SIZE=100, COMMIT_MODE=sync,
#      CPUS=0.5, MEM=768m (per node), YCSB_DIR=results/ycsb,
#      NODE_FAULTS="" (space-separated node<i>=silent|corrupt|readlie|readlie-evasive, from
#      run_one.sh / faults.sh fault_node_faults: those nodes' veritas runs
#      with --fault armed, see gen_compose.py --node-fault).
#      BATCH_FLUSH="" (empty: veritas default 1s; else a duration like 250ms:
#      veritas --batch-flush of every node of a core backend).
#      KAFKA_CPUS=2 KAFKA_MEM=1536m (Kafka broker caps, gen_compose.py
#      --kafka-cpus/--kafka-mem), TM_TIMEOUT_COMMIT="" (tendermint: tm_config.py
#      --timeout-commit; empty = Tendermint's default), VERIFY_SIGS=none and
#      KEY_SEED, ATTEST_EVERY=0 (phase 2: veritas --verify-sigs, --key-seed,
#      --attest-every of every core node), ATTEST_QUORUM_N=0 (phase 3:
#      --attest-quorum, 0 = veritas' default).
# Writes experiments/cluster/generated/{compose.json,tm/}.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

N="${N:-4}"
BLOCK_SIZE="${BLOCK_SIZE:-100}"
COMMIT_MODE="${COMMIT_MODE:-sync}"
CPUS="${CPUS:-0.5}"
MEM="${MEM:-768m}"
NODE_FAULTS="${NODE_FAULTS:-}"
BATCH_FLUSH="${BATCH_FLUSH:-}"
KAFKA_CPUS="${KAFKA_CPUS:-2}"
KAFKA_MEM="${KAFKA_MEM:-1536m}"
TM_TIMEOUT_COMMIT="${TM_TIMEOUT_COMMIT:-}"
VERIFY_SIGS="${VERIFY_SIGS:-none}"
KEY_SEED="${KEY_SEED:-}"
ATTEST_EVERY="${ATTEST_EVERY:-0}"
hbdb_require_uint ATTEST_EVERY "$ATTEST_EVERY"
ATTEST_QUORUM_N="${ATTEST_QUORUM_N:-0}"
hbdb_require_uint ATTEST_QUORUM_N "$ATTEST_QUORUM_N"
core_args=""
if [ "$VERIFY_SIGS" != none ]; then
  core_args="--verify-sigs $VERIFY_SIGS"
fi
if [ "$VERIFY_SIGS" != none ] || [ "$ATTEST_EVERY" != 0 ]; then
  core_args="$core_args${KEY_SEED:+ --key-seed $KEY_SEED}"
fi
if [ "$ATTEST_EVERY" != 0 ]; then
  core_args="$core_args --attest-every $ATTEST_EVERY"
fi
if [ "$ATTEST_QUORUM_N" != 0 ]; then
  core_args="$core_args --attest-quorum $ATTEST_QUORUM_N"
fi

hbdb_check_backend
hbdb_check_n "$N"
hbdb_require_uint BLOCK_SIZE "$BLOCK_SIZE"
# Word-split list (no arrays: bash 3.2 under set -u); every item is checked
# so the unquoted expansion below is safe.
node_fault_args=""
for nf in $NODE_FAULTS; do
  echo "$nf" | grep -Eq '^node[1-9][0-9]*=[a-z]+(-[a-z]+)*$' || die "NODE_FAULTS item '$nf': want node<i>=<fault>, e.g. node4=corrupt"
  node_fault_args="$node_fault_args --node-fault $nf"
done

batch_flush_args=""
if [ -n "$BATCH_FLUSH" ]; then
  echo "$BATCH_FLUSH" | grep -Eq '^[0-9]+(ms|s|m|h)$' || die "BATCH_FLUSH='$BATCH_FLUSH': want a duration like 1s, 250ms, 2m"
  batch_flush_args="--batch-flush $BATCH_FLUSH"
fi

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
  python3 "$HBDB_CLUSTER_DIR/tm_config.py" --dir "$HBDB_GEN_DIR/tm" --n "$N" --backend "$BACKEND" \
    ${TM_TIMEOUT_COMMIT:+--timeout-commit "$TM_TIMEOUT_COMMIT"}
fi

log "generating compose: backend=$BACKEND n=$N block_size=$BLOCK_SIZE commit_mode=$COMMIT_MODE cpus=$CPUS mem=$MEM${NODE_FAULTS:+ node_faults=$NODE_FAULTS}${BATCH_FLUSH:+ batch_flush=$BATCH_FLUSH}"
# shellcheck disable=SC2086
python3 "$HBDB_CLUSTER_DIR/gen_compose.py" \
  --backend "$BACKEND" --n "$N" --block-size "$BLOCK_SIZE" --commit-mode "$COMMIT_MODE" \
  --cpus "$CPUS" --mem "$MEM" --kafka-cpus "$KAFKA_CPUS" --kafka-mem "$KAFKA_MEM" $core_args $node_fault_args $batch_flush_args \
  --tm-dir "$HBDB_GEN_DIR/tm" --ycsb-dir "$HBDB_YCSB_DIR" --results-dir "$HBDB_RESULTS_RAW" \
  --out "$HBDB_COMPOSE_FILE"

log "starting cluster (project $HBDB_PROJECT, $HBDB_COMPOSE_FILE)"
# For kafka backends this blocks until the broker healthcheck passes (topic created).
hbdb_compose up -d
hbdb_compose ps >&2
log "cluster started"
