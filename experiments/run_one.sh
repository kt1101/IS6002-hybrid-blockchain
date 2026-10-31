#!/usr/bin/env bash
# Run one benchmark configuration on a fresh cluster and append one CSV row to
# results/raw/<EXP>.csv (timeline: results/raw/<EXP>/<tag>.timeline; per-node
# core breakdown JSON, when the backend writes one: results/raw/<EXP>/<tag>.breakdown/).
#
#   EXP=smoke BACKEND=kafka-orig N=4 CLIENTS=16 DURATION=20s experiments/run_one.sh
#
# Env (defaults):
#   EXP (required, [A-Za-z0-9._-]+)   BACKEND (required: kafka-orig | tm-orig | kafka | raft | tendermint | bft2)
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
#   BACKEND=tendermint additionally needs COMMIT_MODE=sync, CLIENTS <= 256 and
#                 LOAD_THREADS <= 256 (hbdb_check_tendermint_limits in lib.sh:
#                 Tendermint's 256-request ABCI queue per block)
#
# CLIENTS = number of concurrent client workers: ycsbbench --ndrivers=CLIENTS
# --nthreads=1; driver k talks to node (k mod N)+1.
#
# RTT_MS > 0 (E5): after health.sh passes and before ycsbbench starts (so the
# load and settle phases run with it too), every node gets netem delay
# RTT_MS/2 on its egress (net_set_rtt, experiments/net.sh): node <-> node RTT
# = RTT_MS, client <-> node RTT = RTT_MS/2.
# FAULT (RQ3, experiments/faults.sh; e.g. F1:node4@60, F1:node3,node4@60,
# F2:node4@60, F3:node1@60; FAULT_SLOW_MS=200 is the F3 delay): injected AT
# seconds after the start of the MEASUREMENT window, i.e. WARMUP after the
# "ycsbbench: run phase started" line appears in the bench log; in the
# timeline file (which starts with the warmup) that is line index WARMUP+AT
# (0-based). AT must be < DURATION. Every injection (and the RTT netem) is
# logged with its UTC time, epoch ms and both offsets to
# results/raw/<EXP>/<tag>.faults. The fault column of the CSV row records
# FAULT with ',' replaced by '+' (F1:node3+node4@60). kafka-orig and tm-orig
# have no F2, F4 or F5. F2 on kafka, F4 (any core backend, e.g.
# F4:node4@60) and F5-F4 (tendermint/bft2, f+1 nodes, e.g.
# F5-F4:node3,node4@60) are veritas --fault flags: the faulty nodes are
# created with the fault armed (NODE_FAULTS, passed to up.sh) and the injector
# activates it with SIGUSR1 at AT (logged as silent_on / corrupt_on; several
# nodes in parallel), then confirms it through the node's active file within
# FAULT_CONFIRM_MS=5000 (silent_confirmed / corrupt_confirmed; unconfirmed =
# a failed injection, exit 3); F4 hits at most f = floor((N-1)/3) nodes;
# F5-F2 is F2 (network) on f+1 nodes. F2 on kafka withholds only the node's
# votes, so its own blocks still commit (~1/N of the throughput remains). Nodes F1 stopped on purpose do not count as dead
# containers (exit 3); a failed injection, or one that never ran, is exit 3
# (no CSV row).
# Audit (FAULT != none; backends kafka, raft, tendermint, bft2): in parallel
# with the bench `auditor --interval=1s --node-timeout=800ms` runs in the client container from the
# run-phase marker on and writes results/raw/<EXP>/<tag>.audit, one line
# `unix_ts,nodes` per tick (nodes: empty = consistent, '+'-joined nodeK =
# divergent, down:nodeK = unreachable, e.g. the F1-stopped node; see
# cmd/auditor). unix_ts is the container clock, the .faults epoch_ms the host
# clock; experiments/plot/rq3.py reads both. The auditor is stopped (SIGTERM
# through its pid file, killed if need be) as soon as the bench ends and on
# every exit path; down.sh removes the client container anyway. kafka-orig and
# tm-orig are not audited (their Verify races with commits / returns nothing,
# see check_consistency.sh). A missing or empty .audit file is only a warning.
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
. "$HBDB_ROOT/experiments/net.sh"
. "$HBDB_ROOT/experiments/faults.sh"

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
  '' | *[!A-Za-z0-9._:@,+-]*) die "FAULT must match [A-Za-z0-9._:@,+-]+, got '$FAULT'" ;;
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
# dur_ms D: milliseconds of a validated duration.
dur_ms() {
  case "$1" in
    *ms) echo $((10#${1%ms})) ;;
    *s) echo $((10#${1%s} * 1000)) ;;
    *m) echo $((10#${1%m} * 60000)) ;;
    *h) echo $((10#${1%h} * 3600000)) ;;
  esac
}
watchdog_secs=$(($(dur_seconds "$LOAD_TIMEOUT") + $(dur_seconds "$SETTLE_TIMEOUT") + $(dur_seconds "$WARMUP") + $(dur_seconds "$DURATION") + 120))
{ echo "$CPUS" | grep -Eq '^([0-9]+(\.[0-9]*)?|\.[0-9]+)$' && awk -v c="$CPUS" 'BEGIN { exit !(c > 0) }'; } ||
  die "CPUS must be a positive number, got '$CPUS'"
{ echo "$MEM" | grep -Eq '^[0-9]+(\.[0-9]+)?[mMgG]$' && awk -v m="${MEM%?}" 'BEGIN { exit !(m > 0) }'; } ||
  die "MEM must be a positive number with suffix m, g, M or G (e.g. 768m, 2g), got '$MEM'"
fault_parse "$FAULT" "$BACKEND" "$N" "$(dur_seconds "$DURATION")"
# Nodes whose veritas runs with --fault armed (FaultPolicy faults; up.sh ->
# gen_compose.py --node-fault); the injector activates it at AT.
NODE_FAULTS="$(fault_node_faults)"
# CSV- and --tags-safe form of FAULT (no commas).
fault_rec="$(echo "$FAULT" | tr ',' '+')"

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
hbdb_check_tendermint_limits "$BACKEND" "$COMMIT_MODE" "$CLIENTS" "$LOAD_THREADS"

load_file="$HBDB_YCSB_DIR/$WORKLOAD.load.dat"
run_file="$HBDB_YCSB_DIR/$WORKLOAD.run.dat"
[ -s "$load_file" ] || die "missing $load_file (run experiments/ycsb/gen.sh)"
[ -s "$run_file" ] || die "missing $run_file (run experiments/ycsb/gen.sh)"

export BACKEND N BLOCK_SIZE COMMIT_MODE CPUS MEM HEALTH_TIMEOUT NODE_FAULTS YCSB_DIR="$HBDB_YCSB_DIR"

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
faults_file="$exp_dir/$tag.faults"     # injection log (kept)
fault_stopped="$exp_dir/.$tag.stopped" # services F1 stopped on purpose
fault_stop="$exp_dir/.$tag.fault.stop" # aborts a pending injection
audit_file="$exp_dir/$tag.audit"         # periodic auditor output (kept)
audit_pidfile="$exp_dir/.$tag.audit.pid" # pid of the auditor in the client container
audit_stop="$exp_dir/.$tag.audit.stop"   # asks the audit supervisor to stop the auditor
audit_err="$exp_dir/.$tag.audit.err"     # stdout/stderr of the audit supervisor
lock_dir="$HBDB_RESULTS_RAW/.hbdb.lock"
mkdir -p "$exp_dir"

if [ -s "$csv" ]; then
  [ "$(head -n 1 "$csv")" = "$HBDB_CSV_HEADER" ] || die "$csv has a different header; refusing to append"
fi

ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
# -dirty marks rows measured with uncommitted changes to tracked files.
commit="$(git -C "$HBDB_ROOT" describe --always --dirty --abbrev=7 2>/dev/null | tr -d ',' || true)"
commit="${commit:-unknown}"
tags="ts=$ts,commit=$commit,exp=$EXP,backend=$BACKEND,n=$N,f=$F,clients=$CLIENTS,block_size=$BLOCK_SIZE,workload=$WORKLOAD,rtt_ms=$RTT_MS,fault=$fault_rec,rep=$REP,commit_mode=$COMMIT_MODE"

# ---- cleanup on every exit path ----
bench_pid=""
sampler_pid=""
watchdog_pid=""
events_pid=""
fault_pid=""
fault_rc=0
audit_pid=""
audit_started=""
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

# Abort a pending injection and wait for the scheduler; sets fault_rc to its
# exit status (0 injected, 1 failed, 2 aborted before the injection).
stop_faults() {
  if [ -n "$fault_pid" ]; then
    : >"$fault_stop"
    if wait "$fault_pid"; then
      fault_rc=0
    else
      fault_rc=$?
    fi
    fault_pid=""
  fi
}

# Stop the periodic auditor: tell its supervisor (start_audit) to SIGTERM it,
# wait up to ~15 s, then kill the supervisor. Safe to call twice.
stop_audit() {
  if [ -n "$audit_pid" ]; then
    : >"$audit_stop"
    local k=0
    while kill -0 "$audit_pid" 2>/dev/null && [ "$k" -lt 150 ]; do
      sleep 0.1
      k=$((k + 1))
    done
    if kill -0 "$audit_pid" 2>/dev/null; then
      log "warning: the audit supervisor did not stop within 15 s, killing it (down.sh removes the auditor with the client container)"
      pkill -TERM -P "$audit_pid" 2>/dev/null || true
      kill "$audit_pid" 2>/dev/null || true
    fi
    wait "$audit_pid" 2>/dev/null || true
    audit_pid=""
  fi
}

# start_audit ADDRS: background supervisor (pid in audit_pid) of the periodic
# auditor. It waits for the run-phase marker in the bench log (the load phase
# is not audited), starts `auditor --interval=1s` in the client container (the
# wrapper sh writes its own pid, then execs the auditor, so the pid file holds
# the auditor's pid) and, once $audit_stop exists or this run_one.sh is gone,
# sends it SIGTERM through that pid file, retried until the auditor has exited
# (the stop may come before the pid file exists).
start_audit() {
  local addrs="$1" parent=$$
  (
    set +e
    cpid="/results/$EXP/.$tag.audit.pid"
    while ! grep -q "$FAULT_RUN_MARKER" "$bench_log" 2>/dev/null; do
      [ ! -e "$audit_stop" ] && kill -0 "$parent" 2>/dev/null || exit 0
      sleep 0.2
    done
    hbdb_compose exec -T client sh -c 'echo $$ >"$1" && exec auditor --addrs="$2" --interval=1s --node-timeout=800ms --out="$3"' \
      sh "$cpid" "$addrs" "/results/$EXP/$tag.audit" &
    child=$!
    while kill -0 "$child" 2>/dev/null; do
      if [ -e "$audit_stop" ] || ! kill -0 "$parent" 2>/dev/null; then
        if [ -s "$audit_pidfile" ]; then
          hbdb_compose exec -T client sh -c 'kill -TERM "$(cat "$1")"' sh "$cpid" >/dev/null 2>&1
        fi
      fi
      sleep 0.2
    done
    wait "$child"
  ) >"$audit_err" 2>&1 &
  audit_pid=$!
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
# main one was killed). A service listed in $fault_stopped (stopped on purpose
# by F1) may be stopped; it still counts if it was OOM-killed.
check_containers() {
  local ids id line bad="" stopped=""
  if [ -f "$fault_stopped" ]; then
    stopped="$(tr '\n' ' ' <"$fault_stopped")"
  fi
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
    if [ "${2:-}" != running ] && [ "${3:-}" != true ] && hbdb_in_list "${1:-}" "$stopped"; then
      log "container $1 is ${2:-?} (exit code ${4:-?}): stopped on purpose by FAULT=$FAULT"
    elif [ "${2:-}" != running ] || [ "${3:-}" = true ]; then
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
  [ -s "$audit_err" ] && cp "$audit_err" "$log_dir/audit-supervisor.log" || true
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
  stop_audit
  stop_faults
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
  rm -f "$row_tmp" "$samples" "$sampler_stop" "$watchdog_fired" "$events_file" "$fault_stopped" "$fault_stop" "$audit_pidfile" "$audit_stop" "$audit_err"
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

# ---- network delay (E5) and fault scheduling setup (faults.sh globals) ----
# Stale fault files of an earlier attempt go BEFORE the RTT netem is logged
# (a later rm would delete the netem_rtt line).
rm -f "$faults_file" "$fault_stopped" "$fault_stop"
FAULT_LOG=""
if [ "$RTT_MS" -gt 0 ] || [ "$FAULT" != none ]; then
  FAULT_LOG="$faults_file"
fi
FAULT_STOPPED="$fault_stopped"
FAULT_STOP="$fault_stop"
FAULT_PARENT_PID=$$ # the injector aborts if this run_one.sh is gone
FAULT_WARMUP_MS="$(dur_ms "$WARMUP")"
FAULT_T0_MS=""
if [ "$RTT_MS" -gt 0 ]; then
  # After health.sh (which ran without delay) and before ycsbbench, so load,
  # settle, warmup and measurement all run with it.
  if ! net_set_rtt "$RTT_MS"; then
    log "could not apply netem for RTT_MS=$RTT_MS"
    exit 1
  fi
  fault_log netem_rtt "node1-node$N" "egress delay ${NET_BASE_DELAY_MS}ms on every node (RTT_MS=$RTT_MS)"
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
rm -f "$audit_file" "$audit_pidfile" "$audit_stop" "$audit_err"
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

# Fault scheduler (faults.sh fault_run): waits for the run-phase marker in
# the bench log, then injects FAULT at WARMUP + AT seconds after it.
if [ "$FAULT" != none ]; then
  log "fault $FAULT: injection at t=${FAULT_AT}s of the measurement window (WARMUP $WARMUP + ${FAULT_AT}s after the run-phase marker), log $faults_file"
  (
    set +e
    fault_run "$bench_log"
  ) &
  fault_pid=$!
  case "$BACKEND" in
    kafka-orig | tm-orig)
      log "audit skipped: BACKEND=$BACKEND cannot be audited (Verify races with commits / returns nothing)"
      ;;
    *)
      log "audit: auditor --interval=1s from the run phase on, output $audit_file"
      audit_started=1
      start_audit "$addrs"
      ;;
  esac
fi

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
stop_audit
stop_faults
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
if [ "$FAULT" != none ] && [ "$fault_rc" -ne 0 ]; then
  if [ "$fault_rc" -eq 2 ]; then
    log "fault $FAULT was never injected (the run ended before t=${FAULT_AT}s); no CSV row written"
  else
    log "fault injection $FAULT failed (exit $fault_rc, see $faults_file); no CSV row written"
  fi
  exit 3
fi
grep -q 'ycsbbench: run phase started' "$bench_log" || log "warning: run-phase marker not found in $bench_log"
if [ -n "$audit_started" ] && [ ! -s "$audit_file" ]; then
  log "warning: the auditor wrote no $audit_file (see $audit_err; RQ3 detection latency unavailable)"
fi

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
