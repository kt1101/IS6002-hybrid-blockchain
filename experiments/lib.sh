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
HBDB_ALL_BACKENDS="kafka-orig tm-orig kafka raft tendermint bft2"
HBDB_IMPLEMENTED_BACKENDS="kafka-orig tm-orig kafka"

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
    kafka-orig | kafka) return 0 ;;
    *) return 1 ;;
  esac
}

# ycsbbench --mode for a backend: kafka = versions from the TSO (backends run
# with client versions), tm = no TSO.
hbdb_bench_mode() {
  case "$1" in
    kafka-orig | kafka) echo kafka ;;
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
