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
# is loopback only and is not a consensus port) and its forward endpoint
# (phase 2: every /submit is forwarded to the peers' POST /forward).
HBDB_BFT2_PORT=7200
HBDB_BFT2_FORWARD_PORT=7202
# File holding the veritas server's pid in every core node container
# (gen_compose.py VERITAS_PIDFILE); faults.sh fault_signal sends SIGUSR1 there.
HBDB_VERITAS_PIDFILE=/tmp/hbdb-veritas.pid
# File veritas writes (its --fault spec) once SIGUSR1 activated the fault
# (cmd/veritas defaultFaultActiveFile); faults.sh fault_confirm reads it.
HBDB_FAULT_ACTIVE_FILE=/tmp/hbdb-fault-active
HBDB_ALL_BACKENDS="kafka-orig tm-orig kafka raft tendermint bft2"
HBDB_IMPLEMENTED_BACKENDS="kafka-orig tm-orig kafka raft tendermint bft2"

# Must match csvColumns in veritas/benchmark/recorder.go.
HBDB_CSV_HEADER="ts,commit,exp,backend,n,f,clients,block_size,workload,rtt_ms,fault,rep,commit_mode,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,wrong_gets,host_cpu_max_pct,saturated,net_bytes_per_block,offered_rps"

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
# bft2: the HotStuff replica port and the forward endpoint of bft2-node. Prints nothing for backends
# without a network-silent mode (kafka-orig, tm-orig: none; kafka: its F2 is
# veritas --fault=silent, FaultPolicy.OnVote).
hbdb_consensus_ports() {
  case "$1" in
    raft) echo "$HBDB_RAFT_PORT $HBDB_RAFT_FORWARD_PORT" ;;
    tendermint) echo "$HBDB_TM_P2P_PORT" ;;
    bft2) echo "$HBDB_BFT2_PORT $HBDB_BFT2_FORWARD_PORT" ;;
    *) ;;
  esac
}

# hbdb_backend_has_fault_policy BACKEND: true for the veritas-core backends,
# whose nodes accept veritas --fault (core.FaultPolicy; F2 on kafka, F4, F5-F4).
hbdb_backend_has_fault_policy() {
  case "$1" in
    kafka | raft | tendermint | bft2) return 0 ;;
    *) return 1 ;;
  esac
}

# hbdb_attest_quorum N SPEC (phase 3, ATTEST_QUORUM): the veritas
# --attest-quorum for SPEC at N nodes, f = floor((N-1)/3): auto -> 0 (veritas
# picks 2f+1, as in phase 2), f+1, 2f+1. Dies on any other SPEC.
hbdb_attest_quorum() {
  local f
  hbdb_require_uint N "$1"
  f=$((($1 - 1) / 3))
  case "$2" in
    auto) echo 0 ;;
    f+1) echo $((f + 1)) ;;
    2f+1) echo $((2 * f + 1)) ;;
    *) die "ATTEST_QUORUM must be auto, f+1 or 2f+1, got '$2'" ;;
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

# ---- per-container docker stats (run_one.sh <tag>.stats) ----
# Format given to `docker stats --no-stream --format`.
HBDB_STATS_FORMAT='{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}}'
# Header of results/raw/<EXP>/<tag>.stats: one line per container and sample.
# epoch_ms/utc: host clock just before the `docker stats` call of the sample
# (every container of one call shares them); cpu_pct as docker prints it
# without '%'; mem_used/mem_limit/net_rx/net_tx: docker's strings (MemUsage
# in KiB/MiB/GiB, NetIO in decimal kB/MB/GB, cumulative since the container
# started, only 3 significant digits, e.g. 33.5MB); *_bytes: the same parsed
# to bytes. Samples are ~2 s apart. For resources only: net_bytes_per_block
# uses the exact tx counters read with the breakdown snapshots.
HBDB_STATS_HEADER="epoch_ms,utc,name,cpu_pct,mem_used,mem_limit,net_rx,net_tx,mem_used_bytes,net_rx_bytes,net_tx_bytes"

# hbdb_stats_lines EPOCH_MS UTC: turn `docker stats` lines in
# HBDB_STATS_FORMAT (stdin) into HBDB_STATS_HEADER lines (stdout). Lines with
# fewer than 4 fields are dropped.
hbdb_stats_lines() {
  awk -F, -v ms="$1" -v utc="$2" '
    function bytes(s,   num, unit, m) {
      gsub(/ /, "", s)
      num = s
      sub(/[A-Za-z]+$/, "", num)
      unit = substr(s, length(num) + 1)
      if (num == "" || num !~ /^[0-9.]+$/) return ""
      if (unit == "B" || unit == "") m = 1
      else if (unit == "kB" || unit == "KB") m = 1000
      else if (unit == "MB") m = 1000000
      else if (unit == "GB") m = 1000000000
      else if (unit == "TB") m = 1000000000000
      else if (unit == "KiB") m = 1024
      else if (unit == "MiB") m = 1048576
      else if (unit == "GiB") m = 1073741824
      else if (unit == "TiB") m = 1099511627776
      else return ""
      return sprintf("%.0f", num * m)
    }
    NF >= 4 {
      cpu = $2; gsub(/[% ]/, "", cpu)
      n = split($3, mem, " / ")
      if (n < 2) { mem[1] = $3; mem[2] = "" }
      n = split($4, io, " / ")
      if (n < 2) { io[1] = $4; io[2] = "" }
      gsub(/ /, "", mem[1]); gsub(/ /, "", mem[2]); gsub(/ /, "", io[1]); gsub(/ /, "", io[2])
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", ms, utc, $1, cpu, mem[1], mem[2], io[1], io[2], bytes(mem[1]), bytes(io[1]), bytes(io[2])
    }'
}

# hbdb_json_int FILE KEY: the integer value of top-level "KEY": <int> in a
# JSON file written with one key per line (core.WriteBreakdownJSON); prints
# nothing if FILE or KEY is missing.
hbdb_json_int() {
  [ -f "$1" ] || return 0
  awk -v k="\"$2\":" '$1 == k { v = $2; gsub(/[^0-9-]/, "", v); print v; exit }' "$1"
}

# hbdb_net_bytes_per_block TX1 TX2 BLOCKS1 BLOCKS2: net_bytes_per_block of a
# run: (TX2 - TX1) / (BLOCKS2 - BLOCKS1), rounded to whole bytes. TX1/TX2 are
# the sums of the exact eth0 tx byte counters of the node containers (and the
# Kafka broker for BACKEND=kafka) read with breakdown snapshots 1 and 2
# (run_one.sh window_snapshot, <tag>.snaps), BLOCKS1/BLOCKS2 node1's applied
# blocks in those snapshots. Prints nothing (and returns 1) when a value is
# missing or not an integer, no block was applied, or the bytes went down.
hbdb_net_bytes_per_block() {
  local x1="$1" x2="$2" b1="$3" b2="$4" a
  for a in "$x1" "$x2" "$b1" "$b2"; do
    case "$a" in '' | *[!0-9]*) return 1 ;; esac
  done
  [ $((10#$b2 - 10#$b1)) -gt 0 ] && [ $((10#$x2 - 10#$x1)) -ge 0 ] || return 1
  awk -v dx="$((10#$x2 - 10#$x1))" -v db="$((10#$b2 - 10#$b1))" 'BEGIN { printf "%.0f\n", dx / db }'
}
