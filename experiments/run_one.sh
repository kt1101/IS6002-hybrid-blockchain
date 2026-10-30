#!/usr/bin/env bash
# Run one benchmark configuration on a fresh cluster and append one CSV row to
# results/raw/<EXP>.csv (timeline: results/raw/<EXP>/<tag>.timeline; per-node
# core breakdown JSON, when the backend writes one: results/raw/<EXP>/<tag>.breakdown/).
#
#   EXP=smoke BACKEND=kafka-orig N=4 CLIENTS=16 DURATION=20s experiments/run_one.sh
#
# Env (defaults):
#   EXP (required, [A-Za-z0-9._-]+)   BACKEND (required: kafka-orig | tm-orig | kafka | raft | tendermint)
#   N=4  F=0  CLIENTS=16  BLOCK_SIZE=100  WORKLOAD=a  RTT_MS=0  FAULT=none  REP=1
#   COMMIT_MODE: kafka-orig is always async, tm-orig always sync (the original
#                code has no switch); default = the backend's mode, a mismatch
#                is an error. Later backends default to sync.
#   DURATION=60s  WARMUP=10s
#   CPUS=0.5 MEM=768m (per node; CPUS a positive number, MEM a number with
#                 suffix m, g, M or G)   HEALTH_TIMEOUT=60 (seconds)
#   LOAD_THREADS: concurrent Sets in the load phase (10 = original; tm-orig
#                 defaults to 64 because each Set waits for a block; more
#                 threads only deepen the backlog at ~60-70 tx/s and make
#                 broadcast_tx_commit calls time out; tm_config.py raises rpc
#                 max-subscription-clients to 2000 and caps blocks at 225000
#                 bytes, ~220 txs); the new backends (kafka, raft,
#                 tendermint, bft2) default to 256: they run sync commit by
#                 default, so each load Set waits for its block and 10
#                 threads would leave blocks of ~1-10 txs and a very slow
#                 load (default from hbdb_default_load_threads in lib.sh)
#   LOAD_TIMEOUT=20m: ycsbbench --load-timeout; a load that does not finish in
#                 time, or any failed load Set, fails the run (exit 3)
#   LOAD_OP_TIMEOUT=30s: ycsbbench --load-op-timeout, the deadline of one load
#                 Set attempt; a record is tried up to 3 times (--load-attempts)
#                 and fails the load only if every attempt fails
#   SETTLE_TIMEOUT=10m: ycsbbench --settle-timeout; after the load every node
#                 must serve Get for 200 sample loaded keys within it (exit 3)
#   YCSB_DIR=results/ycsb (files <WORKLOAD>.load.dat / <WORKLOAD>.run.dat,
#                          made by experiments/ycsb/gen.sh: 10k records,
#                          100k operations by default)
#   KEEP_LOGS=0 (1: keep container logs also on success)
#
# CLIENTS = number of concurrent client workers: ycsbbench --ndrivers=CLIENTS
# --nthreads=1; driver k talks to node (k mod N)+1.
# RTT_MS and FAULT are recorded only; netem/faults come in later tasks, so any
# value other than 0 / none is rejected for now.
#
# Durations accept the suffixes ms, s, m and h (e.g. 500ms, 30s, 20m, 1h).
# Watchdog: ycsbbench is killed if it runs longer than LOAD_TIMEOUT +
# SETTLE_TIMEOUT + WARMUP + DURATION + 120 s (exit 3), in case a call that
# ignores its deadline (e.g. a hung TSO request) keeps it from exiting.
#
# Lock: only one run_one.sh at a time may use compose project hbdb. It takes
# the directory results/raw/.hbdb.lock (mkdir is atomic; file `owner` holds
# "pid EXP tag") and releases it on every exit path. If the lock is held by a
# live pid the run exits 1 without touching the cluster; a lock whose pid is
# dead is taken over.
# Logs: on failure (or KEEP_LOGS=1) container logs, compose.json, the bench
# log and docker-events.log (`docker events` of project hbdb during the run)
# go to results/raw/<EXP>/logs/<tag>.<UTC start time>/, so repeated attempts
# of one configuration never overwrite each other.
#
# Exit: 0 ok, 1 bad input or harness error (this includes a Kafka broker that
#       never becomes healthy: `docker compose up` in up.sh then fails, and a
#       cluster lock held by another live run_one.sh),
#       2 cluster not healthy (no CSV row),
#       3 benchmark failed, was killed by the watchdog, or a cluster container
#       died or was OOM-killed during it (checked with docker inspect: status,
#       OOMKilled, exit code; no CSV row).
#       down.sh always runs at the end.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

EXP="${EXP:-}"
BACKEND="${BACKEND:-}"
N="${N:-4}"
F="${F:-0}"
CLIENTS="${CLIENTS:-16}"
BLOCK_SIZE="${BLOCK_SIZE:-100}"
WORKLOAD="${WORKLOAD:-a}"
RTT_MS="${RTT_MS:-0}"
FAULT="${FAULT:-none}"
REP="${REP:-1}"
DURATION="${DURATION:-60s}"
WARMUP="${WARMUP:-10s}"
CPUS="${CPUS:-0.5}"
MEM="${MEM:-768m}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-60}"
LOAD_TIMEOUT="${LOAD_TIMEOUT:-20m}"
LOAD_OP_TIMEOUT="${LOAD_OP_TIMEOUT:-30s}"
SETTLE_TIMEOUT="${SETTLE_TIMEOUT:-10m}"
KEEP_LOGS="${KEEP_LOGS:-0}"

# ---- validate input (nothing touches Docker before this passes) ----
case "$EXP" in
  '' | *[!A-Za-z0-9._-]*) die "EXP is required and must match [A-Za-z0-9._-]+, got '$EXP'" ;;
esac
hbdb_check_backend
hbdb_check_n "$N"
hbdb_require_uint F "$F"
hbdb_require_uint CLIENTS "$CLIENTS"
[ "$CLIENTS" -ge 1 ] || die "CLIENTS must be >= 1"
hbdb_require_uint BLOCK_SIZE "$BLOCK_SIZE"
[ "$BLOCK_SIZE" -ge 1 ] || die "BLOCK_SIZE must be >= 1"
hbdb_require_uint REP "$REP"
hbdb_require_uint RTT_MS "$RTT_MS"
hbdb_require_uint HEALTH_TIMEOUT "$HEALTH_TIMEOUT"
case "$WORKLOAD" in
  '' | *[!A-Za-z0-9_-]*) die "WORKLOAD must match [A-Za-z0-9_-]+, got '$WORKLOAD'" ;;
esac
case "$FAULT" in
  '' | *[!A-Za-z0-9._:@-]*) die "FAULT must match [A-Za-z0-9._:@-]+, got '$FAULT'" ;;
esac
for d in "$DURATION" "$WARMUP" "$LOAD_TIMEOUT" "$LOAD_OP_TIMEOUT" "$SETTLE_TIMEOUT"; do
  echo "$d" | grep -Eq '^[0-9]+(ms|s|m|h)$' || die "DURATION/WARMUP/LOAD_TIMEOUT/LOAD_OP_TIMEOUT/SETTLE_TIMEOUT must look like 60s, 500ms, 2m; got '$d'"
done
# dur_seconds D: whole seconds of a validated duration (ms rounded up).
dur_seconds() {
  case "$1" in
    *ms) echo $(((10#${1%ms} + 999) / 1000)) ;;
    *s) echo $((10#${1%s})) ;;
    *m) echo $((10#${1%m} * 60)) ;;
    *h) echo $((10#${1%h} * 3600)) ;;
  esac
}
watchdog_secs=$(($(dur_seconds "$LOAD_TIMEOUT") + $(dur_seconds "$SETTLE_TIMEOUT") + $(dur_seconds "$WARMUP") + $(dur_seconds "$DURATION") + 120))
{ echo "$CPUS" | grep -Eq '^([0-9]+(\.[0-9]*)?|\.[0-9]+)$' && awk -v c="$CPUS" 'BEGIN { exit !(c > 0) }'; } ||
  die "CPUS must be a positive number, got '$CPUS'"
{ echo "$MEM" | grep -Eq '^[0-9]+(\.[0-9]+)?[mMgG]$' && awk -v m="${MEM%?}" 'BEGIN { exit !(m > 0) }'; } ||
  die "MEM must be a positive number with suffix m, g, M or G (e.g. 768m, 2g), got '$MEM'"
[ "$RTT_MS" -eq 0 ] || die "RTT_MS=$RTT_MS: network delay injection is not implemented yet (only RTT_MS=0)"
[ "$FAULT" = none ] || die "FAULT=$FAULT: fault injection is not implemented yet (only FAULT=none)"

case "$BACKEND" in
  kafka-orig) native_mode=async ;;
  tm-orig) native_mode=sync ;;
  *) native_mode="" ;;
esac
if [ -n "$native_mode" ]; then
  COMMIT_MODE="${COMMIT_MODE:-$native_mode}"
  [ "$COMMIT_MODE" = "$native_mode" ] ||
    die "COMMIT_MODE=$COMMIT_MODE: $BACKEND (original code) is always $native_mode"
else
  COMMIT_MODE="${COMMIT_MODE:-sync}"
fi
case "$COMMIT_MODE" in
  sync | async) ;;
  *) die "COMMIT_MODE must be sync or async, got '$COMMIT_MODE'" ;;
esac

LOAD_THREADS="${LOAD_THREADS:-$(hbdb_default_load_threads "$BACKEND")}"
hbdb_require_uint LOAD_THREADS "$LOAD_THREADS"
[ "$LOAD_THREADS" -ge 1 ] || die "LOAD_THREADS must be >= 1"

load_file="$HBDB_YCSB_DIR/$WORKLOAD.load.dat"
run_file="$HBDB_YCSB_DIR/$WORKLOAD.run.dat"
[ -s "$load_file" ] || die "missing $load_file (run experiments/ycsb/gen.sh)"
[ -s "$run_file" ] || die "missing $run_file (run experiments/ycsb/gen.sh)"

export BACKEND N BLOCK_SIZE COMMIT_MODE CPUS MEM HEALTH_TIMEOUT YCSB_DIR="$HBDB_YCSB_DIR"

tag="${BACKEND}_n${N}_f${F}_c${CLIENTS}_b${BLOCK_SIZE}_w${WORKLOAD}_rtt${RTT_MS}_${FAULT}_${COMMIT_MODE}_r${REP}"
tag="$(echo "$tag" | tr -c 'A-Za-z0-9._\n-' '_')"
exp_dir="$HBDB_RESULTS_RAW/$EXP"
csv="$HBDB_RESULTS_RAW/$EXP.csv"
row_tmp="$exp_dir/.$tag.row.csv"   # written by ycsbbench (header + 1 row)
bench_log="$exp_dir/$tag.bench.log"
samples="$exp_dir/.$tag.cpu"
sampler_stop="$exp_dir/.$tag.cpu.stop"
watchdog_fired="$exp_dir/.$tag.watchdog"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_dir="$exp_dir/logs/$tag.$stamp"   # one directory per attempt
events_file="$exp_dir/.$tag.events"
lock_dir="$HBDB_RESULTS_RAW/.hbdb.lock"
mkdir -p "$exp_dir"

if [ -s "$csv" ]; then
  [ "$(head -n 1 "$csv")" = "$HBDB_CSV_HEADER" ] || die "$csv has a different header; refusing to append"
fi

ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
# -dirty marks rows measured with uncommitted changes to tracked files.
commit="$(git -C "$HBDB_ROOT" describe --always --dirty --abbrev=7 2>/dev/null | tr -d ',' || true)"
commit="${commit:-unknown}"
tags="ts=$ts,commit=$commit,exp=$EXP,backend=$BACKEND,n=$N,f=$F,clients=$CLIENTS,block_size=$BLOCK_SIZE,workload=$WORKLOAD,rtt_ms=$RTT_MS,fault=$FAULT,rep=$REP,commit_mode=$COMMIT_MODE"

# ---- cleanup on every exit path ----
bench_pid=""
sampler_pid=""
watchdog_pid=""
events_pid=""
lock_held=0

# acquire_lock: one run at a time on compose project hbdb (see header).
acquire_lock() {
  local pid="" k=0
  if ! mkdir "$lock_dir" 2>/dev/null; then
    # The owner file is written right after mkdir: give a racing writer 3 s.
    while [ "$k" -lt 3 ]; do
      pid="$(cut -d' ' -f1 "$lock_dir/owner" 2>/dev/null || true)"
      [ -n "$pid" ] && break
      k=$((k + 1))
      sleep 1
    done
    [ -n "$pid" ] || die "$lock_dir exists without owner info; if no run_one.sh is running, remove it"
    if kill -0 "$pid" 2>/dev/null; then
      die "another run_one.sh holds $lock_dir ($(cat "$lock_dir/owner" 2>/dev/null)): only one run at a time may use compose project $HBDB_PROJECT"
    fi
    log "stale lock $lock_dir (pid $pid is not running): taking it over"
    rm -rf "$lock_dir"
    mkdir "$lock_dir" 2>/dev/null || die "could not take over $lock_dir"
  fi
  lock_held=1
  echo "$$ $EXP $tag" >"$lock_dir/owner"
}

release_lock() {
  if [ "$lock_held" = 1 ]; then
    rm -rf "$lock_dir"
    lock_held=0
  fi
}

stop_events() {
  if [ -n "$events_pid" ]; then
    kill "$events_pid" 2>/dev/null || true
    wait "$events_pid" 2>/dev/null || true
    events_pid=""
  fi
}

# Ask the sampler loop to stop (it finishes its current sample), kill it if it
# has not stopped after ~5 s.
stop_sampler() {
  if [ -n "$sampler_pid" ]; then
    : >"$sampler_stop"
    local k=0
    while kill -0 "$sampler_pid" 2>/dev/null && [ "$k" -lt 50 ]; do
      sleep 0.1
      k=$((k + 1))
    done
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
    sampler_pid=""
  fi
}

stop_watchdog() {
  if [ -n "$watchdog_pid" ]; then
    kill "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true
    watchdog_pid=""
  fi
}

# check_containers: return 1 (and log each one) if any container of the
# project is not running, e.g. OOM-killed (exit 137) under its mem_limit, or
# has been OOM-killed although it is still running (a process other than the
# main one was killed).
check_containers() {
  local ids id line bad=""
  ids="$(docker ps -aq --filter "label=com.docker.compose.project=$HBDB_PROJECT")"
  if [ -z "$ids" ]; then
    log "no containers found for project $HBDB_PROJECT"
    return 1
  fi
  for id in $ids; do
    line="$(docker inspect --format '{{index .Config.Labels "com.docker.compose.service"}} {{.State.Status}} {{.State.OOMKilled}} {{.State.ExitCode}}' "$id" 2>/dev/null)" ||
      line="$id unknown unknown unknown"
    # shellcheck disable=SC2086
    set -- $line
    if [ "${2:-}" != running ] || [ "${3:-}" = true ]; then
      bad="$bad $1(status=${2:-?},oom_killed=${3:-?},exit_code=${4:-?})"
    fi
  done
  if [ -n "$bad" ]; then
    log "containers not running or OOM-killed:$bad"
    return 1
  fi
  return 0
}

# Core-based nodes write results/raw/breakdown-node<i>.json when stopped
# (down.sh). Move them into directory $1 so the next run cannot overwrite them; an
# attempt that is retried replaces its files. No-op when there are none
# (original backends).
collect_breakdown() {
  local f found=0
  for f in "$HBDB_RESULTS_RAW"/breakdown-node*.json; do
    [ -e "$f" ] || continue
    [ "$found" = 1 ] || { mkdir -p "$1" || return 0; found=1; }
    mv -f "$f" "$1/" || true
  done
}

save_logs() {
  [ -f "$HBDB_COMPOSE_FILE" ] || return 0
  mkdir -p "$log_dir"
  log "saving container logs to $log_dir"
  hbdb_compose ps -a >"$log_dir/ps.txt" 2>&1 || true
  local svc
  for svc in $(hbdb_compose config --services 2>/dev/null); do
    hbdb_compose logs --no-color --timestamps "$svc" >"$log_dir/$svc.log" 2>&1 || true
  done
  cp "$HBDB_COMPOSE_FILE" "$log_dir/compose.json" 2>/dev/null || true
  [ -f "$bench_log" ] && cp "$bench_log" "$log_dir/" || true
  [ -f "$events_file" ] && cp "$events_file" "$log_dir/docker-events.log" || true
}

cleanup() {
  local rc=$?
  set +e
  trap - EXIT INT TERM
  if [ "$lock_held" != 1 ]; then
    # Never got the lock (bad input or another run is active): the cluster and
    # the tag's temp files belong to someone else, touch nothing.
    exit "$rc"
  fi
  stop_watchdog
  stop_sampler
  stop_events
  if [ -n "$bench_pid" ]; then
    kill "$bench_pid" 2>/dev/null
    wait "$bench_pid" 2>/dev/null
  fi
  if [ "$rc" -ne 0 ] || [ "$KEEP_LOGS" = 1 ]; then
    save_logs
  fi
  "$HBDB_CLUSTER_DIR/down.sh"
  collect_breakdown "$exp_dir/$tag.breakdown"
  rm -f "$row_tmp" "$samples" "$sampler_stop" "$watchdog_fired" "$events_file"
  release_lock
  if [ "$rc" -eq 0 ]; then
    log "done: $EXP/$tag"
  else
    log "failed with exit code $rc: $EXP/$tag"
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ---- host state ----
# One line per run phase in results/raw/<EXP>/host.log: load average, free and
# compressed memory and the cumulative swapout count of the macOS host, so a
# row measured on a busy or swapping host can be spotted later. No-op where
# vm_stat is missing (non-macOS hosts).
host_state() {
  command -v vm_stat >/dev/null 2>&1 || return 0
  local load vm
  load="$(sysctl -n vm.loadavg 2>/dev/null | tr -d '{}' | awk '{print $1","$2","$3}')"
  vm="$(vm_stat 2>/dev/null | awk '
    /page size of/ { ps = $8 }
    /^Pages free/ { gsub(/\./, "", $3); free = $3 }
    /occupied by compressor/ { gsub(/\./, "", $5); comp = $5 }
    /^Swapouts/ { gsub(/\./, "", $2); sw = $2 }
    END { printf "%d,%d,%d", free * ps / 1048576, comp * ps / 1048576, sw }')"
  echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$tag,$1,$load,$vm" >>"$exp_dir/host.log" 2>/dev/null || true
}

# ---- cluster ----
acquire_lock
[ -s "$exp_dir/host.log" ] || echo "ts,tag,phase,load1,load5,load15,free_mb,compressor_mb,swapouts" >"$exp_dir/host.log"
host_state start
log "run $EXP/$tag (duration $DURATION, warmup $WARMUP, load threads $LOAD_THREADS, load timeout $LOAD_TIMEOUT, load op timeout $LOAD_OP_TIMEOUT, settle timeout $SETTLE_TIMEOUT, watchdog ${watchdog_secs}s)"
"$HBDB_CLUSTER_DIR/down.sh" || exit 1
# Files left by an earlier cluster (e.g. check_consistency.sh) are not this run's.
collect_breakdown "$HBDB_RESULTS_RAW/breakdown-stale.$(date -u +%Y%m%dT%H%M%SZ)"
# Record the project's container events (die, kill, oom, ...) for the whole
# run; saved with the logs only if they are saved.
docker events --filter "label=com.docker.compose.project=$HBDB_PROJECT" >"$events_file" 2>&1 &
events_pid=$!
"$HBDB_CLUSTER_DIR/up.sh" || exit 1
if ! "$HBDB_CLUSTER_DIR/health.sh"; then
  log "health check failed; no CSV row written"
  exit 2
fi

# ---- benchmark + docker stats sampling ----
addrs=""
i=1
while [ "$i" -le "$N" ]; do
  addrs="${addrs:+$addrs,}$(hbdb_node_ip "$i"):$HBDB_VERITAS_PORT"
  i=$((i + 1))
done
tso_arg="--tso-addr="
if hbdb_backend_uses_tso "$BACKEND"; then
  tso_arg="--tso-addr=$HBDB_TSO_ADDR"
fi

rm -f "$row_tmp" "$samples" "$sampler_stop" "$watchdog_fired" "$exp_dir/$tag.timeline"
: >"$bench_log"
: >"$samples"

ncpu="$(docker info --format '{{.NCPU}}')"
hbdb_require_uint NCPU "$ncpu"
[ "$ncpu" -ge 1 ] || die "docker info reports NCPU=$ncpu"
container_ids="$(hbdb_compose ps -q)"
[ -n "$container_ids" ] || die "no running containers in project $HBDB_PROJECT"

log "starting ycsbbench: $CLIENTS clients over $N nodes, mode $(hbdb_bench_mode "$BACKEND")"
hbdb_compose exec -T client ycsbbench \
  --load-path="/ycsb/$WORKLOAD.load.dat" --run-path="/ycsb/$WORKLOAD.run.dat" \
  --ndrivers="$CLIENTS" --nthreads=1 --load-threads="$LOAD_THREADS" --load-timeout="$LOAD_TIMEOUT" \
  --load-op-timeout="$LOAD_OP_TIMEOUT" --settle-timeout="$SETTLE_TIMEOUT" \
  --veritas-addrs="$addrs" "$tso_arg" --mode="$(hbdb_bench_mode "$BACKEND")" \
  --warmup="$WARMUP" --duration="$DURATION" \
  --out="/results/$EXP/.$tag.row.csv" --tags="$tags" \
  --timeline="/results/$EXP/$tag.timeline" >"$bench_log" 2>&1 &
bench_pid=$!

# Watchdog: kill ycsbbench (the background subshell and its docker compose
# exec child) once it has run for watchdog_secs.
(
  set +e
  end=$(($(date +%s) + watchdog_secs))
  while [ "$(date +%s)" -lt "$end" ]; do
    sleep 1
  done
  : >"$watchdog_fired"
  pkill -TERM -P "$bench_pid" 2>/dev/null
  kill "$bench_pid" 2>/dev/null
) &
watchdog_pid=$!

# Sample host CPU use once the run phase (warmup + measurement) has started:
# sum of all cluster containers' CPU% / (100 * NCPU) * 100. There is no sleep
# between samples: each `docker stats --no-stream` call itself takes about
# 1 s, so the cadence is roughly one sample per second.
(
  set +e
  started=0
  while [ ! -e "$sampler_stop" ]; do
    if [ "$started" = 0 ] && grep -q 'ycsbbench: run phase started' "$bench_log" 2>/dev/null; then
      started=1
    fi
    if [ "$started" = 1 ]; then
      # shellcheck disable=SC2086
      docker stats --no-stream --format '{{.CPUPerc}}' $container_ids 2>/dev/null |
        awk -v n="$ncpu" '{ gsub(/%/, ""); s += $1 } END { if (NR > 0) printf "%.2f\n", s / (100 * n) * 100 }' >>"$samples"
    else
      sleep 0.5 # waiting for the run-phase marker
    fi
  done
) &
sampler_pid=$!

if wait "$bench_pid"; then
  bench_rc=0
else
  bench_rc=$?
fi
bench_pid=""
stop_watchdog
stop_sampler
sed 's/^/    /' "$bench_log" | tail -n 30 >&2
containers_ok=1
check_containers || containers_ok=0
if [ "$bench_rc" -ne 0 ]; then
  if [ -e "$watchdog_fired" ]; then
    log "ycsbbench did not finish within ${watchdog_secs}s (LOAD_TIMEOUT + SETTLE_TIMEOUT + WARMUP + DURATION + 120 s); killed by the watchdog"
  fi
  log "ycsbbench failed (exit $bench_rc), see $bench_log"
  exit 3
fi
if [ "$containers_ok" -ne 1 ]; then
  log "a cluster container died during the run; no CSV row written"
  exit 3
fi
grep -q 'ycsbbench: run phase started' "$bench_log" || log "warning: run-phase marker not found in $bench_log"

# ---- assemble the final row ----
[ -s "$row_tmp" ] || die "ycsbbench wrote no row to $row_tmp"
[ "$(head -n 1 "$row_tmp")" = "$HBDB_CSV_HEADER" ] || die "unexpected header in $row_tmp: $(head -n 1 "$row_tmp")"
row="$(sed -n 2p "$row_tmp")"
nf="$(echo "$row" | awk -F, '{ print NF }')"
[ "$nf" -eq 22 ] || die "expected 22 CSV fields from ycsbbench, got $nf: $row"

host_cpu_max_pct="$(awk 'NF { if (!seen || $1 > m) { m = $1; seen = 1 } } END { if (seen) printf "%.2f", m }' "$samples")"
if [ -n "$host_cpu_max_pct" ]; then
  saturated="$(awk -v c="$host_cpu_max_pct" 'BEGIN { print (c > 90) ? 1 : 0 }')"
else
  log "warning: no docker stats samples; host_cpu_max_pct and saturated left empty"
  saturated=""
fi
net_bytes_per_block="" # original backends expose no committed-block count

final="$(echo "$row" | cut -d, -f1-19),$host_cpu_max_pct,$saturated,$net_bytes_per_block"
if [ ! -s "$csv" ]; then
  echo "$HBDB_CSV_HEADER" >"$csv"
fi
echo "$final" >>"$csv"
host_state end
log "row appended to $csv (host_cpu_max_pct=$host_cpu_max_pct saturated=$saturated)"
echo "$final"
