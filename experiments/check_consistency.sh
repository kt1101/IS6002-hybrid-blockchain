#!/usr/bin/env bash
# Consistency check of a backend: bring up an N=4 cluster, run a 30 s YCSB-A
# benchmark, wait 5 s for the last blocks, then run `auditor --once` (every
# replica must report the same ledger root digest).
#
#   experiments/check_consistency.sh <backend>
#
# <backend>: one of the NEW backends (kafka, raft, tendermint, bft2). The
# originals are refused: kafka-orig because its Verify races with commits (the
# server's Merkle tree is not synchronized, the auditor must never be pointed
# at it), tm-orig because its Verify returns nil,nil (a gRPC marshal error).
# A backend that is known but not implemented yet is refused by
# hbdb_check_backend ("not implemented yet"). bft2 runs a HotStuff sidecar
# (bft2-node, in the hbdb-node image via experiments/build.sh) on every node;
# it has no tendermint-style limits on CLIENTS/LOAD_THREADS.
#
# Env (defaults): N=4 (fixed, not overridable), DURATION=30s, WARMUP=10s,
#   WAIT_SECS=5 (pause between the end of the bench and the audit),
#   BLOCK_SIZE=100, COMMIT_MODE=sync, CPUS=0.5, MEM=768m, HEALTH_TIMEOUT=60,
#   CLIENTS=16, WORKLOAD=a (needs $YCSB_DIR/<w>.load.dat and .run.dat, made by
#   experiments/ycsb/gen.sh), KEEP_LOGS=0, LOAD_THREADS (ycsbbench --load-threads;
#   default from hbdb_default_load_threads in lib.sh, 256 for the new backends,
#   as in run_one.sh). BACKEND=tendermint needs COMMIT_MODE=sync, CLIENTS <= 256
#   and LOAD_THREADS <= 256 (hbdb_check_tendermint_limits in lib.sh).
#
# DURATION and WARMUP accept only the suffixes s and m (30s, 2m), unlike run_one.sh.
#
# Lock: shares the lock of run_one.sh (both use compose project hbdb): the
# directory results/raw/.hbdb.lock, see acquire_lock below. A live holder makes
# this script exit 1 without touching the cluster.
#
# The auditor runs in the `client` container, nodes are named node<i> in the
# order of --addrs (node i = 172.28.0.(10+i):1990).
#
# Exit: 0 CONSISTENT, 1 bad input or harness error, 2 cluster not healthy,
#       3 benchmark failed or timed out, 4 DIVERGENT (the auditor's output is
#       printed), 5 the auditor failed or its output is neither a CONSISTENT
#       line nor a DIVERGENT line (a node could not be queried, or
#       `docker compose exec` itself failed).
#       1 also covers a cluster lock held by another live run.
#       down.sh always runs at the end.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

BACKEND="${1:-}"
[ "$#" -le 1 ] || die "usage: $0 <backend>"
case "$BACKEND" in
  '') die "usage: $0 <backend> (one of: $HBDB_ALL_BACKENDS, except kafka-orig and tm-orig)" ;;
  kafka-orig) die "kafka-orig is not audited: its Verify races with commits and can crash the server" ;;
  tm-orig) die "tm-orig is not audited: its Verify returns nil,nil (gRPC marshal error)" ;;
esac

N=4
DURATION="${DURATION:-30s}"
WARMUP="${WARMUP:-10s}"
WAIT_SECS="${WAIT_SECS:-5}"
BLOCK_SIZE="${BLOCK_SIZE:-100}"
COMMIT_MODE="${COMMIT_MODE:-sync}"
CPUS="${CPUS:-0.5}"
MEM="${MEM:-768m}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-60}"
CLIENTS="${CLIENTS:-16}"
WORKLOAD="${WORKLOAD:-a}"
KEEP_LOGS="${KEEP_LOGS:-0}"
LOAD_THREADS="${LOAD_THREADS:-$(hbdb_default_load_threads "$BACKEND")}"
LOAD_TIMEOUT_S=1200 # ycsbbench --load-timeout 20m
SETTLE_TIMEOUT_S=600 # ycsbbench --settle-timeout 10m

hbdb_check_backend
hbdb_require_uint WAIT_SECS "$WAIT_SECS"
hbdb_require_uint BLOCK_SIZE "$BLOCK_SIZE"
hbdb_require_uint HEALTH_TIMEOUT "$HEALTH_TIMEOUT"
hbdb_require_uint CLIENTS "$CLIENTS"
hbdb_require_uint LOAD_THREADS "$LOAD_THREADS"
[ "$LOAD_THREADS" -ge 1 ] || die "LOAD_THREADS must be >= 1"
[ "$CLIENTS" -ge 1 ] || die "CLIENTS must be >= 1"
case "$COMMIT_MODE" in
  sync | async) ;;
  *) die "COMMIT_MODE must be sync or async, got '$COMMIT_MODE'" ;;
esac
hbdb_check_tendermint_limits "$BACKEND" "$COMMIT_MODE" "$CLIENTS" "$LOAD_THREADS"
case "$WORKLOAD" in
  '' | *[!A-Za-z0-9_-]*) die "WORKLOAD must match [A-Za-z0-9_-]+, got '$WORKLOAD'" ;;
esac
for d in "$DURATION" "$WARMUP"; do
  echo "$d" | grep -Eq '^[0-9]+(s|m)$' || die "DURATION/WARMUP must look like 30s or 2m; got '$d'"
done
dur_seconds() {
  case "$1" in
    *s) echo $((10#${1%s})) ;;
    *m) echo $((10#${1%m} * 60)) ;;
  esac
}
watchdog_secs=$((LOAD_TIMEOUT_S + SETTLE_TIMEOUT_S + $(dur_seconds "$WARMUP") + $(dur_seconds "$DURATION") + 120))

load_file="$HBDB_YCSB_DIR/$WORKLOAD.load.dat"
run_file="$HBDB_YCSB_DIR/$WORKLOAD.run.dat"
[ -s "$load_file" ] || die "missing $load_file (run experiments/ycsb/gen.sh)"
[ -s "$run_file" ] || die "missing $run_file (run experiments/ycsb/gen.sh)"

export BACKEND N BLOCK_SIZE COMMIT_MODE CPUS MEM HEALTH_TIMEOUT YCSB_DIR="$HBDB_YCSB_DIR"

out_dir="$HBDB_ROOT/results/consistency"
tag="${BACKEND}_n${N}_$(date -u +%Y%m%dT%H%M%SZ)"
bench_log="$out_dir/$tag.bench.log"
audit_log="$out_dir/$tag.audit.txt"
log_dir="$out_dir/$tag.logs"
lock_dir="$HBDB_RESULTS_RAW/.hbdb.lock"
mkdir -p "$out_dir" "$HBDB_RESULTS_RAW"
: >"$bench_log"

bench_pid=""
watchdog_pid=""
lock_held=0

# acquire_lock / release_lock: one run at a time on compose project hbdb.
# KEEP IN SYNC WITH run_one.sh: the protocol (lock path, `owner` file holding
# "pid ..." inside the lock directory, stale-lock takeover when the pid is
# dead) must stay identical, because both scripts share the lock. Duplicated
# for now; to be factored into lib.sh together with the bench invocation.
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
    [ -n "$pid" ] || die "$lock_dir exists without owner info; if no run is active, remove it"
    if kill -0 "$pid" 2>/dev/null; then
      die "another run holds $lock_dir ($(cat "$lock_dir/owner" 2>/dev/null)): only one run at a time may use compose project $HBDB_PROJECT"
    fi
    log "stale lock $lock_dir (pid $pid is not running): taking it over"
    rm -rf "$lock_dir"
    mkdir "$lock_dir" 2>/dev/null || die "could not take over $lock_dir"
  fi
  lock_held=1
  echo "$$ check_consistency $tag" >"$lock_dir/owner"
}

release_lock() {
  if [ "$lock_held" = 1 ]; then
    rm -rf "$lock_dir"
    lock_held=0
  fi
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
}

cleanup() {
  local rc=$?
  set +e
  trap - EXIT INT TERM
  if [ "$lock_held" != 1 ]; then
    # Never got the lock (another run is active): the cluster belongs to it,
    # touch nothing.
    exit "$rc"
  fi
  if [ -n "$watchdog_pid" ]; then
    kill "$watchdog_pid" 2>/dev/null
    wait "$watchdog_pid" 2>/dev/null
  fi
  if [ -n "$bench_pid" ]; then
    kill "$bench_pid" 2>/dev/null
    wait "$bench_pid" 2>/dev/null
  fi
  # rc 4 (divergent) keeps the logs too: they are the evidence.
  if [ "$rc" -ne 0 ] || [ "$KEEP_LOGS" = 1 ]; then
    save_logs
  fi
  "$HBDB_CLUSTER_DIR/down.sh"
  release_lock
  if [ "$rc" -eq 0 ]; then
    log "done: $tag"
  else
    log "failed with exit code $rc: $tag"
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ---- cluster ----
acquire_lock
log "consistency check $tag (bench $DURATION + warmup $WARMUP, then ${WAIT_SECS}s wait, watchdog ${watchdog_secs}s)"
"$HBDB_CLUSTER_DIR/down.sh" || exit 1
"$HBDB_CLUSTER_DIR/up.sh" || exit 1
if ! "$HBDB_CLUSTER_DIR/health.sh"; then
  log "health check failed"
  exit 2
fi

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

# ---- benchmark (same ycsbbench invocation as run_one.sh, no CSV row) ----
# KEEP IN SYNC WITH run_one.sh: if its ycsbbench flags change (load/settle
# options, commit mode, ...), change them here too. To be factored into lib.sh.
log "starting ycsbbench: $CLIENTS clients over $N nodes, workload $WORKLOAD, mode $(hbdb_bench_mode "$BACKEND")"
hbdb_compose exec -T client ycsbbench \
  --load-path="/ycsb/$WORKLOAD.load.dat" --run-path="/ycsb/$WORKLOAD.run.dat" \
  --ndrivers="$CLIENTS" --nthreads=1 --load-threads="$LOAD_THREADS" \
  --veritas-addrs="$addrs" "$tso_arg" --mode="$(hbdb_bench_mode "$BACKEND")" \
  --warmup="$WARMUP" --duration="$DURATION" >"$bench_log" 2>&1 &
bench_pid=$!

# Watchdog: a bench that ignores its deadlines must not hang the check.
(
  set +e
  end=$(($(date +%s) + watchdog_secs))
  while [ "$(date +%s)" -lt "$end" ]; do
    sleep 1
  done
  pkill -TERM -P "$bench_pid" 2>/dev/null
  kill "$bench_pid" 2>/dev/null
) &
watchdog_pid=$!

if wait "$bench_pid"; then
  bench_rc=0
else
  bench_rc=$?
fi
bench_pid=""
kill "$watchdog_pid" 2>/dev/null || true
wait "$watchdog_pid" 2>/dev/null || true
watchdog_pid=""
sed 's/^/    /' "$bench_log" | tail -n 20 >&2
if [ "$bench_rc" -ne 0 ]; then
  log "ycsbbench failed (exit $bench_rc), see $bench_log"
  exit 3
fi

# ---- audit ----
log "waiting ${WAIT_SECS}s for the last blocks"
sleep "$WAIT_SECS"
log "running auditor --once on $addrs"
if hbdb_compose exec -T client auditor --addrs="$addrs" --once >"$audit_log" 2>&1; then
  audit_rc=0
else
  audit_rc=$?
fi
sed 's/^/    /' "$audit_log" >&2
# Classify by output as well as by rc: `docker compose exec` has its own
# failures that also exit 1, so rc 1 alone does not mean DIVERGENT.
if [ "$audit_rc" -eq 0 ] && grep -qx 'CONSISTENT' "$audit_log"; then
  log "CONSISTENT"
  cat "$audit_log"
elif [ "$audit_rc" -eq 1 ] && grep -q '^DIVERGENT ' "$audit_log"; then
  log "DIVERGENT: $(grep '^DIVERGENT ' "$audit_log" || true)"
  cat "$audit_log"
  exit 4
else
  log "auditor failed or gave no verdict (exit $audit_rc), see $audit_log"
  exit 5
fi
