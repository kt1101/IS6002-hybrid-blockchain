# shellcheck shell=bash
# Shared helpers for the experiment scripts. Source it, do not execute it:
#   . "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/experiments/lib.sh"
# Runs on the macOS host: keep it bash 3.2 compatible (no associative arrays,
# no ${var,,}, no mapfile, no `wait -n`).

HBDB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HBDB_PROJECT=hbdb
HBDB_CLUSTER_DIR="$HBDB_ROOT/experiments/cluster"
HBDB_GEN_DIR="$HBDB_CLUSTER_DIR/generated"
HBDB_COMPOSE_FILE="$HBDB_GEN_DIR/compose.json"
HBDB_RESULTS_RAW="$HBDB_ROOT/results/raw"
HBDB_YCSB_DIR="${YCSB_DIR:-$HBDB_ROOT/results/ycsb}"
case "$HBDB_YCSB_DIR" in
  /*) ;;
  *) HBDB_YCSB_DIR="$PWD/$HBDB_YCSB_DIR" ;;
esac

# Must match experiments/cluster/gen_compose.py.
HBDB_VERITAS_PORT=1990
HBDB_TSO_ADDR=172.28.0.3:7070
# Consensus ports of the node containers (gen_compose.py RAFT_PORT,
# RAFT_FORWARD_PORT, TM_P2P_PORT, BFT2_PORT); see hbdb_consensus_ports.
HBDB_RAFT_PORT=7100
HBDB_RAFT_FORWARD_PORT=7101
HBDB_TM_P2P_PORT=26656
# bft2: HotStuff replica-to-replica port of bft2-node (its local API, 7201,
# is loopback only and is not a consensus port).
HBDB_BFT2_PORT=7200
HBDB_ALL_BACKENDS="kafka-orig tm-orig kafka raft tendermint bft2"
HBDB_IMPLEMENTED_BACKENDS="kafka-orig tm-orig kafka raft tendermint bft2"

# Must match csvColumns in veritas/benchmark/recorder.go.
HBDB_CSV_HEADER="ts,commit,exp,backend,n,f,clients,block_size,workload,rtt_ms,fault,rep,commit_mode,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,wrong_gets,host_cpu_max_pct,saturated,net_bytes_per_block"

HBDB_SCRIPT="$(basename "$0")"

log() {
  printf '%s [%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$HBDB_SCRIPT" "$*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

# docker compose (v2) on the generated file with the fixed project name.
hbdb_compose() {
  docker compose -p "$HBDB_PROJECT" -f "$HBDB_COMPOSE_FILE" "$@"
}

hbdb_node_ip() {
  echo "172.28.0.$((10 + $1))"
}

# hbdb_in_list WORD "LIST OF WORDS"
hbdb_in_list() {
  case " $2 " in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

hbdb_backend_uses_tso() {
  case "$1" in
    kafka-orig | kafka | raft) return 0 ;;
    *) return 1 ;;
  esac
}

# ycsbbench --mode for a backend: kafka = versions from the TSO (backends run
# with client versions: kafka-orig, kafka, raft), tm = no TSO (tm-orig, and
# tendermint and bft2, which run --version-mode=ordered).
hbdb_bench_mode() {
  case "$1" in
    kafka-orig | kafka | raft) echo kafka ;;
    *) echo tm ;;
  esac
}

# Default ycsbbench --load-threads for a backend (run_one.sh LOAD_THREADS,
# check_consistency.sh): kafka-orig 10 (original), tm-orig 64 (each Set waits
# for a block; see run_one.sh), every new backend 256 (sync commit by default:
# each load Set waits for its block, so the load needs many in flight).
hbdb_default_load_threads() {
  case "$1" in
    kafka-orig) echo 10 ;;
    tm-orig) echo 64 ;;
    *) echo 256 ;;
  esac
}

# Max txs one Tendermint v0.35.0 block may carry: its socket ABCI client
# queues at most 256 async requests (abci/client/socket_client.go
# reqQueueSize) and execBlockOnProxyApp sends every DeliverTx of a block with
# DeliverTxAsync, so a block of more txs fails "buffer is full" and halts
# consensus (CheckTx is async too: more concurrent broadcasts fail as Submit
# errors).
HBDB_TM_ABCI_QUEUE=256

# hbdb_check_tendermint_limits BACKEND COMMIT_MODE CLIENTS LOAD_THREADS:
# for BACKEND=tendermint, fail unless the tx count per Tendermint block is
# bounded below the ABCI queue. In sync commit every Veritas block (one ABCI
# tx) carries at least one outstanding Set, so the txs in all mempools are at
# most the outstanding Sets: CLIENTS during the run, LOAD_THREADS during the
# load. Async commit has no such bound. Other backends: no-op.
hbdb_check_tendermint_limits() {
  [ "$1" = tendermint ] || return 0
  [ "$2" = sync ] ||
    die "BACKEND=tendermint needs COMMIT_MODE=sync (got '$2'): only sync commit bounds the txs per Tendermint block below the $HBDB_TM_ABCI_QUEUE-request ABCI queue (more halts consensus)"
  [ "$3" -le "$HBDB_TM_ABCI_QUEUE" ] ||
    die "BACKEND=tendermint needs CLIENTS <= $HBDB_TM_ABCI_QUEUE (got $3): more outstanding Sets can put more txs in one Tendermint block than its $HBDB_TM_ABCI_QUEUE-request ABCI queue holds (consensus halts)"
  [ "$4" -le "$HBDB_TM_ABCI_QUEUE" ] ||
    die "BACKEND=tendermint needs LOAD_THREADS <= $HBDB_TM_ABCI_QUEUE (got $4): more outstanding load Sets can put more txs in one Tendermint block than its $HBDB_TM_ABCI_QUEUE-request ABCI queue holds (consensus halts)"
}

# hbdb_consensus_ports BACKEND: the TCP ports a node uses to talk consensus
# with its peers, space-separated; the network-level silent fault (F2,
# experiments/faults.sh fault_silent_net) drops them. raft: the raft transport
# and the submit-forward HTTP endpoint; tendermint: the tendermint p2p port;
# bft2: the HotStuff replica port of bft2-node. Prints nothing for backends
# without a network-silent mode (kafka-orig, tm-orig, kafka: their F2 is
# FaultPolicy, Task 15).
hbdb_consensus_ports() {
  case "$1" in
    raft) echo "$HBDB_RAFT_PORT $HBDB_RAFT_FORWARD_PORT" ;;
    tendermint) echo "$HBDB_TM_P2P_PORT" ;;
    bft2) echo "$HBDB_BFT2_PORT" ;;
    *) ;;
  esac
}

# Milliseconds since the epoch (bash 3.2 has no EPOCHREALTIME and macOS date
# no %N: perl, then python3, then whole seconds from date).
hbdb_epoch_ms() {
  perl -MTime::HiRes=time -e 'printf "%d\n", time() * 1000' 2>/dev/null ||
    python3 -c 'import time; print(int(time.time() * 1000))' 2>/dev/null ||
    echo "$(($(date +%s) * 1000))"
}

# Fail unless $2 (named $1) is a non-negative integer.
hbdb_require_uint() {
  case "$2" in
    '' | *[!0-9]*) die "$1 must be a non-negative integer, got '$2'" ;;
  esac
}

# Validate BACKEND (known and implemented) and N.
hbdb_check_backend() {
  [ -n "${BACKEND:-}" ] || die "BACKEND is required (one of: $HBDB_ALL_BACKENDS)"
  hbdb_in_list "$BACKEND" "$HBDB_ALL_BACKENDS" || die "unknown BACKEND '$BACKEND' (one of: $HBDB_ALL_BACKENDS)"
  hbdb_in_list "$BACKEND" "$HBDB_IMPLEMENTED_BACKENDS" || die "backend $BACKEND not implemented yet"
}

hbdb_check_n() {
  hbdb_require_uint N "$1"
  [ "$1" -ge 1 ] || die "N must be >= 1"
  [ "$1" -le 200 ] || die "N must be <= 200 (node IPs are 172.28.0.(10+i))"
}
