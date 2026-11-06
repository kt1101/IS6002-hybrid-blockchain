#!/usr/bin/env bash
# Tests of the E1-E6 drivers (exp_lib.sh, e1..e6, backends.sh, kafka_ops.sh)
# and of the BATCH_FLUSH / PRE_DOWN_HOOK checks of run_one.sh. No Docker, no Go:
# the drivers run in a copy of the experiments/ tree in a temporary directory
# whose run_one.sh is a stub (it logs its environment and appends a CSV row),
# and kafka_ops.sh runs with a stub `docker` on PATH.
#
#   experiments/exp/test_exp.sh        (exit 0 = all passed)
#
# Bash 3.2 compatible, like the scripts it tests.
set -u

# Hermetic: the drivers and run_one.sh read many knobs from the environment
# (REPS, EXTRA_REPS, BACKENDS, EXP, FIXED_CLIENTS, BATCH_FLUSH, ...). A caller
# that exported any of them (e.g. EXTRA_REPS=0 for a smoke run) must not change
# what this suite checks, so drop every one of them first. Only PATH, HOME,
# TMPDIR and KEEP_TMP survive.
for v in EXP BACKEND BACKENDS N F CLIENTS BLOCK_SIZE WORKLOAD RTT_MS FAULT REP TAG COMMIT_MODE \
  REPS EXTRA_REPS NS FIXED_CLIENTS BEST_CLIENTS_KAFKA BEST_CLIENTS_RAFT BEST_CLIENTS_TENDERMINT BEST_CLIENTS_BFT2 \
  BEST_CLIENTS_KAFKA_ORIG BEST_CLIENTS_TM_ORIG E1_EXP E6_NF E6_AT E6_DURATION F5_KINDS BLOCKS WORKLOADS RTTS \
  DURATION WARMUP DRY_RUN BATCH_FLUSH PRE_DOWN_HOOK KAFKA_OPS_CSV KAFKA_OPS_WAIT LOAD_THREADS LOAD_TIMEOUT \
  LOAD_OP_TIMEOUT SETTLE_TIMEOUT HEALTH_TIMEOUT CPUS MEM KEEP_LOGS YCSB_DIR FAULT_SLOW_MS FAULT_CONFIRM_MS \
  HBDB_NET_DEV NET_BASE_DELAY_MS NET_LIMIT_PKTS STUB_RC STUB_BACKEND STUB_NOISY_REP2 STUB_FAIL_ONCE \
  DOCKER_STUB_LOG DOCKER_STUB_FAIL DOCKER_STUB_NOLINE DOCKER_STUB_DASH PART NS_B \
  E6_LEADER E6_LEADER_BACKENDS HBDB_CAFFEINATED NO_CAFFEINATE EXP_MIN_FREE_GB EXP_ALLOW_COMMIT_CHANGE DF_STUB_AVAIL_KB \
  VALID_MAX_WRITE_ERR_PCT VALID_ZERO_RUN_SECS AUDIT_WAIT_SECS; do
  unset "$v"
done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
SRC_ROOT="$(cd "$HERE/../.." && pwd -P)"

# A TMPDIR ending in '/' would put '//' into every path the drivers print.
TMPDIR="${TMPDIR:-/tmp}"
TMPDIR="${TMPDIR%/}"
export TMPDIR
TMP_ROOT="$(mktemp -d "$TMPDIR/test_exp.XXXXXX")"
TMP_ROOT="$(cd "$TMP_ROOT" && pwd -P)"
[ -n "${KEEP_TMP:-}" ] || trap 'rm -rf "$TMP_ROOT"' EXIT

# No test may start a cluster: a docker on PATH that records the call and fails
# (the kafka_ops tests below put their own canned docker in front).
mkdir -p "$TMP_ROOT/bin"
cat >"$TMP_ROOT/bin/docker" <<'STUB'
#!/usr/bin/env bash
echo "docker $*" >>"${TMP_ROOT_DOCKER_LOG:?}"
echo "test_exp.sh: unexpected docker call: $*" >&2
exit 97
STUB
chmod +x "$TMP_ROOT/bin/docker"
export TMP_ROOT_DOCKER_LOG="$TMP_ROOT/unexpected-docker.log"
: >"$TMP_ROOT_DOCKER_LOG"
# caffeinate stub (the real one exists on macOS): log the call, run the
# command without the -dims flags argument.
cat >"$TMP_ROOT/bin/caffeinate" <<'STUB'
#!/usr/bin/env bash
echo "caffeinate $*" >>"${CAFFEINATE_LOG:?}"
shift
exec "$@"
STUB
chmod +x "$TMP_ROOT/bin/caffeinate"
export CAFFEINATE_LOG="$TMP_ROOT/caffeinate.log"
: >"$CAFFEINATE_LOG"
# df stub: DF_STUB_AVAIL_KB free (default 100 GiB), so the disk guard never
# depends on the machine running the tests.
cat >"$TMP_ROOT/bin/df" <<'STUB'
#!/usr/bin/env bash
echo "Filesystem 1024-blocks Used Available Capacity Mounted on"
echo "/dev/stub 999999999 1 ${DF_STUB_AVAIL_KB:-104857600} 1% /"
STUB
chmod +x "$TMP_ROOT/bin/df"
export PATH="$TMP_ROOT/bin:$PATH"
pass=0
fail=0
ok() { pass=$((pass + 1)); }
bad() {
  fail=$((fail + 1))
  echo "FAIL: $*" >&2
}
eq() { if [ "$2" = "$3" ]; then ok; else bad "$1: got '$2', want '$3'"; fi; }
contains() {
  case "$2" in
    *"$3"*) ok ;;
    *) bad "$1: '$3' not in: $2" ;;
  esac
}
lacks() {
  case "$2" in
    *"$3"*) bad "$1: '$3' unexpectedly in: $2" ;;
    *) ok ;;
  esac
}

# ---- a fresh tree with a stub run_one.sh ----
ROOT=""
newtree() {
  ROOT="$TMP_ROOT/tree.$1"
  rm -rf "$ROOT"
  mkdir -p "$ROOT/results/raw" "$ROOT/results/ycsb"
  cp -R "$SRC_ROOT/experiments" "$ROOT/experiments"
  for w in a b c; do
    echo "INSERT x" >"$ROOT/results/ycsb/$w.load.dat"
    echo "READ x" >"$ROOT/results/ycsb/$w.run.dat"
  done
  cat >"$ROOT/experiments/run_one.sh" <<'STUB'
#!/usr/bin/env bash
# Stub of run_one.sh: log the environment, append one row (23 columns).
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "EXP=$EXP BACKEND=$BACKEND N=$N F=$F CLIENTS=$CLIENTS BLOCK_SIZE=$BLOCK_SIZE WORKLOAD=$WORKLOAD RTT_MS=$RTT_MS FAULT=$FAULT REP=$REP COMMIT_MODE=$COMMIT_MODE DURATION=${DURATION:-} HOOK=${PRE_DOWN_HOOK:-}" >>"$root/stub.log"
if [ -n "${STUB_RC:-}" ] && { [ -z "${STUB_BACKEND:-}" ] || [ "$STUB_BACKEND" = "$BACKEND" ]; }; then
  exit "$STUB_RC"
fi
if [ -n "${STUB_FAIL_ONCE:-}" ] && [ ! -e "$root/.failed-once" ]; then
  : >"$root/.failed-once"
  exit 3
fi
tps=100
if [ "${STUB_NOISY_REP2:-}" = 1 ] && [ "$REP" = 2 ]; then tps=300; fi
csv="$root/results/raw/$EXP.csv"
[ -s "$csv" ] || echo "ts,commit,exp,backend,n,f,clients,block_size,workload,rtt_ms,fault,rep,commit_mode,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,wrong_gets,host_cpu_max_pct,saturated,net_bytes_per_block,offered_rps" >"$csv"
echo "2026-10-30T00:00:00Z,abc1234,$EXP,$BACKEND,$N,$F,$CLIENTS,$BLOCK_SIZE,$WORKLOAD,$RTT_MS,$(echo "$FAULT" | tr ',' '+'),$REP,$COMMIT_MODE,$tps,1,1,1,0,0,10,0,,${RATE:-0}" >>"$csv"
STUB
  chmod +x "$ROOT/experiments/run_one.sh"
  : >"$ROOT/stub.log"
}

# drv NAME ARGS...: run driver NAME of the current tree; output in $OUT, status in $RC.
drv() {
  local name="$1"
  shift
  OUT="$("$ROOT/experiments/exp/$name" "$@" 2>&1)"
  RC=$?
}
calls() { grep -c . "$ROOT/stub.log" 2>/dev/null || true; }
rows() { # rows EXP -> data rows of results/raw/EXP.csv
  if [ -s "$ROOT/results/raw/$1.csv" ]; then awk 'END { print NR - 1 }' "$ROOT/results/raw/$1.csv"; else echo 0; fi
}
# e1_rows BACKEND CLIENTS_LIST...: write a complete E1 CSV for BACKEND with
# tps = 10 for every CLIENTS but the first-listed best value (tps 99).
e1_rows() {
  local b="$1" best="$2" c r csv="$ROOT/results/raw/e1.csv"
  [ -s "$csv" ] || echo "ts,commit,exp,backend,n,f,clients,block_size,workload,rtt_ms,fault,rep,commit_mode,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,wrong_gets,host_cpu_max_pct,saturated,net_bytes_per_block" >"$csv"
  for c in 4 16 64 256; do
    for r in 1 2 3; do
      local t=10
      [ "$c" = "$best" ] && t=99
      echo "ts,abc,e1,$b,4,0,$c,100,a,0,none,$r,sync,$t,1,1,1,0,0,10,0," >>"$csv"
    done
  done
}

# ---------------------------------------------------------------- dry runs --
newtree dry
for spec in "e1_clients.sh 48" "e3_block.sh 36" "e4_workload.sh 36" "e5_rtt.sh 36" "e2_nodes.sh 24" "e6_faults.sh 144"; do
  set -- $spec
  OUT="$("$ROOT/experiments/exp/$1" --dry-run 2>/dev/null)"
  RC=$?
  eq "$1 --dry-run exit" "$RC" 0
  eq "$1 --dry-run line count" "$(printf '%s\n' "$OUT" | grep -c '^DRY_RUN: ')" "$2"
  eq "$1 --dry-run prints only plan lines on stdout" "$(printf '%s\n' "$OUT" | grep -vc '^DRY_RUN: ')" 0
done
eq "dry runs write nothing" "$(ls "$ROOT/results/raw" | tr '\n' ' ')" ""
eq "dry runs call no run_one" "$(calls)" 0
OUT="$(NS="4 7 10 13 16" "$ROOT/experiments/exp/e2_nodes.sh" --dry-run 2>/dev/null | grep -c '^DRY_RUN: ')"
eq "E2 with the plan's NS has 60 runs" "$OUT" 60
OUT="$(DRY_RUN=1 "$ROOT/experiments/exp/e1_clients.sh" 2>/dev/null | grep -c '^DRY_RUN: ')"
eq "DRY_RUN=1 is --dry-run" "$OUT" 48
OUT="$(BACKENDS="raft bft2" "$ROOT/experiments/exp/e1_clients.sh" --dry-run 2>/dev/null | grep -c '^DRY_RUN: ')"
eq "BACKENDS narrows the plan" "$OUT" 24
OUT="$(F5_KINDS="F5-F4" "$ROOT/experiments/exp/e6_faults.sh" --dry-run 2>/dev/null | grep -c '^DRY_RUN: ')"
eq "F5_KINDS=F5-F4 leaves 44 configs... 132 runs" "$OUT" 132
OUT="$(E6_LEADER=0 "$ROOT/experiments/exp/e6_faults.sh" --dry-run 2>/dev/null | grep -c '^DRY_RUN: ')"
eq "E6_LEADER=0: the 120 runs of the plan" "$OUT" 120
OUT="$("$ROOT/experiments/exp/e6_faults.sh" --dry-run 2>/dev/null)"
eq "E6 leader faults: raft+kafka x F1/F3 x 2 (N,f) x 3 reps" "$(printf '%s\n' "$OUT" | grep -cE 'FAULT=F[13]:node1@60 ')" 24
eq "E6 leader faults only for raft and kafka" "$(printf '%s\n' "$OUT" | grep -E 'FAULT=F[13]:node1@60 ' | grep -vcE 'BACKEND=(raft|kafka) ')" 0
contains "E6 leader fault N=7" "$OUT" "BACKEND=raft N=7 F=2 CLIENTS=best-of-e1 BLOCK_SIZE=100 WORKLOAD=a RTT_MS=0 FAULT=F1:node1@60 REP=1"
OUT="$(E6_LEADER_BACKENDS="raft bft2" "$ROOT/experiments/exp/e6_faults.sh" --dry-run 2>/dev/null | grep -cE 'FAULT=F[13]:node1@60 ')"
eq "E6_LEADER_BACKENDS selects the backends" "$OUT" 24
E6_LEADER_BACKENDS="kafka-orig" "$ROOT/experiments/exp/e6_faults.sh" --dry-run >/dev/null 2>&1
eq "E6_LEADER_BACKENDS rejects a non E1-E6 backend" "$?" 1
E6_LEADER=2 "$ROOT/experiments/exp/e6_faults.sh" --dry-run >/dev/null 2>&1
eq "E6_LEADER must be 0 or 1" "$?" 1
OUT="$(E6_KINDS="F1 F2 F4" "$ROOT/experiments/exp/e6_faults.sh" --dry-run 2>/dev/null)"
eq "E6_KINDS without F3: (24 + 8 + 4 leader F1) configs x 3 reps" "$(printf '%s\n' "$OUT" | grep -c '^DRY_RUN: ')" 108
eq "E6_KINDS without F3 plans no F3 run" "$(printf '%s\n' "$OUT" | grep -c 'FAULT=F3:')" 0
eq "E6_KINDS without F3 keeps the F1 leader fault" "$(printf '%s\n' "$OUT" | grep -c 'FAULT=F1:node1@60 ')" 12
E6_KINDS="F1 F9" "$ROOT/experiments/exp/e6_faults.sh" --dry-run >/dev/null 2>&1
eq "E6_KINDS rejects an unknown fault" "$?" 1
# I9: rep is the outermost loop of the plan.
OUT="$("$ROOT/experiments/exp/e1_clients.sh" --dry-run 2>/dev/null)"
eq "dry run: the first 16 runs are rep 1 of every config" "$(printf '%s\n' "$OUT" | head -n 16 | grep -c ' REP=1 ')" 16
eq "dry run: rep 3 last" "$(printf '%s\n' "$OUT" | tail -n 16 | grep -c ' REP=3 ')" 16
eq "dry run: rep 1 order follows the plan" "$(printf '%s\n' "$OUT" | head -n 5 | sed 's/.*BACKEND=\([a-z0-9]*\) .*CLIENTS=\([0-9]*\) .*/\1\/\2/' | tr '\n' ' ')" "kafka/4 kafka/16 kafka/64 kafka/256 raft/4 "
eq "dry run: no caffeinate" "$(grep -c . "$CAFFEINATE_LOG")" 0
OUT="$("$ROOT/experiments/exp/e6_faults.sh" --dry-run 2>/dev/null)"
eq "F5 only for tendermint and bft2" "$(printf '%s\n' "$OUT" | grep 'FAULT=F5-' | grep -vcE 'BACKEND=(tendermint|bft2) ')" 0
contains "E6 N=7 fault nodes" "$OUT" "N=7 F=2 CLIENTS=best-of-e1 BLOCK_SIZE=100 WORKLOAD=a RTT_MS=0 FAULT=F1:node7,node6@60"
contains "E6 F5 uses f+1 nodes" "$OUT" "BACKEND=bft2 N=7 F=2 CLIENTS=best-of-e1 BLOCK_SIZE=100 WORKLOAD=a RTT_MS=0 FAULT=F5-F4:node7,node6,node5@60"
contains "E6 F5 N=4" "$OUT" "BACKEND=tendermint N=4 F=1 CLIENTS=best-of-e1 BLOCK_SIZE=100 WORKLOAD=a RTT_MS=0 FAULT=F5-F2:node4,node3@60"
eq "E6 F4 corrupts f nodes only" "$(printf '%s\n' "$OUT" | grep 'FAULT=F4:' | grep 'N=7' | head -1 | sed 's/.*FAULT=\([^ ]*\) .*/\1/')" "F4:node7,node6@60"
"$ROOT/experiments/exp/backends.sh" >"$TMP_ROOT/b.out" 2>&1
eq "backends.sh lists the four backends" "$(cat "$TMP_ROOT/b.out")" "kafka raft tendermint bft2"
eq "BACKENDS reorders" "$(BACKENDS="bft2 kafka" "$ROOT/experiments/exp/backends.sh" 2>&1)" "bft2 kafka"
BACKENDS="kafka-orig" "$ROOT/experiments/exp/backends.sh" >"$TMP_ROOT/b.out" 2>&1
eq "BACKENDS rejects a non E1-E6 backend" "$?" 1

# --------------------------------------------------- a backend not available --
newtree unimpl
sed -i.bak 's/^HBDB_IMPLEMENTED_BACKENDS=.*/HBDB_IMPLEMENTED_BACKENDS="kafka-orig tm-orig kafka raft tendermint"/' "$ROOT/experiments/lib.sh"
drv e1_clients.sh --dry-run
eq "unimplemented bft2 skipped: exit" "$RC" 0
contains "unimplemented bft2 noted" "$OUT" "backend bft2 is not implemented yet: skipped"
eq "unimplemented bft2 skipped: 36 runs" "$(printf '%s\n' "$OUT" | grep -c '^DRY_RUN: ')" 36

# ------------------------------------------------------- full E1, resumable --
newtree e1
drv e1_clients.sh
eq "E1 exit" "$RC" 0
eq "E1 rows" "$(rows e1)" 48
eq "E1 run_one calls" "$(calls)" 48
eq "E1 always sync" "$(grep -vc 'COMMIT_MODE=sync' "$ROOT/stub.log")" 0
eq "E1 N=4 block 100 workload a rtt 0 no fault" "$(grep -vc 'N=4 F=0 CLIENTS=[0-9]* BLOCK_SIZE=100 WORKLOAD=a RTT_MS=0 FAULT=none' "$ROOT/stub.log")" 0
eq "E1 clients" "$(grep 'BACKEND=raft ' "$ROOT/stub.log" | sed 's/.*CLIENTS=\([0-9]*\) .*REP=\([0-9]\) .*/\1\/\2/' | tr '\n' ' ')" "4/1 16/1 64/1 256/1 4/2 16/2 64/2 256/2 4/3 16/3 64/3 256/3 "
eq "E1 rep outermost across backends" "$(sed -n '1p;16p;17p' "$ROOT/stub.log" | sed 's/.*BACKEND=\([a-z0-9]*\) .*REP=\([0-9]\) .*/\1\/\2/' | tr '\n' ' ')" "kafka/1 bft2/1 kafka/2 "
eq "E1 re-executed once under caffeinate -dims" "$(grep -c "^caffeinate -dims $ROOT/experiments/exp/e1_clients.sh$" "$CAFFEINATE_LOG")" 1
eq "E1 params file" "$(cut -d= -f1 "$ROOT/results/raw/e1.params" | tr '\n' ' ')" "DURATION WARMUP CPUS MEM LOAD_THREADS BATCH_FLUSH commit "
contains "E1 params values" "$(cat "$ROOT/results/raw/e1.params")" "DURATION=60s"
eq "E1 no hook" "$(grep -vc 'HOOK=$' "$ROOT/stub.log")" 0
contains "E1 summary" "$OUT" "rows present: 48 / 48 planned"
contains "E1 summary: no failures" "$OUT" "failures in this run: none"
: >"$ROOT/stub.log"
drv e1_clients.sh
eq "E1 resumed: exit" "$RC" 0
eq "E1 resumed: nothing re-run" "$(calls)" 0
eq "E1 resumed: rows" "$(rows e1)" 48
# a deleted row is run again, alone
grep -v 'raft,4,0,64,100,a,0,none,2,' "$ROOT/results/raw/e1.csv" >"$TMP_ROOT/e1.new" && cp "$TMP_ROOT/e1.new" "$ROOT/results/raw/e1.csv"
drv e1_clients.sh
eq "E1 refill: one run" "$(calls)" 1
contains "E1 refill: the missing one" "$(cat "$ROOT/stub.log")" "BACKEND=raft N=4 F=0 CLIENTS=64 BLOCK_SIZE=100 WORKLOAD=a RTT_MS=0 FAULT=none REP=2"
eq "E1 refill: rows" "$(rows e1)" 48

# ------------------------------------------------------ noise rule (CV > 10%) --
newtree noisy
STUB_NOISY_REP2=1 BACKENDS=raft drv e1_clients.sh
eq "noisy: exit" "$RC" 0
eq "noisy: each noisy config gets 2 extra reps (4 x (3+2))" "$(rows e1)" 20
contains "noisy: log" "$OUT" "extra rep 1/2 (rep 4)"
contains "noisy: still above" "$OUT" "still above 10% after the extra reps"
contains "noisy: flagged" "$OUT" "ABOVE 10%"
newtree noisy0
STUB_NOISY_REP2=1 EXTRA_REPS=0 BACKENDS=raft drv e1_clients.sh
eq "EXTRA_REPS=0: no extras" "$(rows e1)" 12
newtree quiet
BACKENDS=raft drv e1_clients.sh
eq "quiet: no extras" "$(rows e1)" 12
contains "quiet: cv" "$OUT" "cv=0.0000 (3 rows)"

# ---------------------------------------------------- failures: retry, abort --
newtree fail
STUB_RC=3 STUB_BACKEND=raft BACKENDS="raft kafka" drv e1_clients.sh
eq "failing backend: exit 4" "$RC" 4
eq "failing backend: every run tried twice, the other backend untouched" "$(calls)" $((12 * 2 + 12))
eq "failing backend: rows of the good backend only" "$(rows e1)" 12
eq "failing backend: .failures lines" "$(grep -c . "$ROOT/results/raw/e1.failures")" 12
contains "failing backend: .failures format" "$(head -1 "$ROOT/results/raw/e1.failures")" " raft 4 0 4 100 a 0 none 1 3"
contains "failing backend: summary" "$OUT" "failures in this run (also in"
newtree fail2
STUB_RC=2 BACKENDS=raft drv e1_clients.sh
eq "rc 2 is retried too" "$(calls)" 24
newtree abort
STUB_RC=1 BACKENDS=raft drv e1_clients.sh
eq "rc 1 aborts the batch with 1" "$RC" 1
eq "rc 1 aborts after the first call" "$(calls)" 1
newtree abort2
STUB_RC=130 BACKENDS=raft drv e1_clients.sh
eq "rc 130 aborts with 130" "$RC" 130
eq "rc 130 aborts after the first call" "$(calls)" 1

# ---------------------------------------------------------- best CLIENTS, E2 --
newtree e2none
drv e2_nodes.sh
eq "E2 without E1 rows: nothing runs" "$(calls)" 0
eq "E2 without E1 rows: exit 4" "$RC" 4
contains "E2 without E1 rows: says so" "$OUT" "raft skipped"
newtree e2
e1_rows raft 64
e1_rows kafka 16
BACKENDS="raft kafka" drv e2_nodes.sh
eq "E2: exit" "$RC" 0
eq "E2: 2 backends x 2 N x 3" "$(rows e2)" 12
eq "E2: raft runs at its best clients (64)" "$(grep 'BACKEND=raft ' "$ROOT/stub.log" | grep -vc 'CLIENTS=64 ')" 0
eq "E2: kafka runs at its best clients (16)" "$(grep 'BACKEND=kafka ' "$ROOT/stub.log" | grep -vc 'CLIENTS=16 ')" 0
eq "E2: N values" "$(grep 'BACKEND=raft ' "$ROOT/stub.log" | sed 's/.* N=\([0-9]*\) .*/\1/' | sort -u | tr '\n' ' ')" "4 7 "
eq "E2: kafka gets the kafka-ops hook" "$(grep 'BACKEND=kafka ' "$ROOT/stub.log" | grep -vc "HOOK=$ROOT/experiments/exp/kafka_ops.sh\$")" 0
eq "E2: raft gets no hook" "$(grep 'BACKEND=raft ' "$ROOT/stub.log" | grep -vc 'HOOK=$')" 0
contains "E2 summary: ops rows" "$OUT" "kafka ops rows: 0 in $ROOT/results/raw/e2-kafka-ops.csv for 6 kafka rows"
# partial E1 (a rep missing) -> the backend is skipped, never picked from partial data
newtree e2partial
e1_rows raft 64
grep -v 'raft,4,0,256,100,a,0,none,3,' "$ROOT/results/raw/e1.csv" >"$TMP_ROOT/e1.new" && cp "$TMP_ROOT/e1.new" "$ROOT/results/raw/e1.csv"
BACKENDS=raft drv e2_nodes.sh
eq "E2 with partial E1: skipped" "$(calls)" 0
eq "E2 with partial E1: exit 4" "$RC" 4
contains "E2 with partial E1: names the gap" "$OUT" "256/3"
BEST_CLIENTS_RAFT=16 BACKENDS=raft drv e2_nodes.sh
eq "BEST_CLIENTS_RAFT overrides" "$(grep -vc 'CLIENTS=16 ' "$ROOT/stub.log")" 0
eq "BEST_CLIENTS_RAFT: runs" "$(calls)" 6
newtree e2dry
e1_rows raft 64
OUT="$(BACKENDS=raft "$ROOT/experiments/exp/e2_nodes.sh" --dry-run 2>/dev/null | head -1)"
contains "E2 dry run reads the best from E1" "$OUT" "BACKEND=raft N=4 F=0 CLIENTS=64 "

# retry that succeeds on the second attempt
newtree retryok
STUB_FAIL_ONCE=1 BACKENDS=raft drv e1_clients.sh
eq "a run that fails once is retried and succeeds: exit" "$RC" 0
eq "retry: 12 rows" "$(rows e1)" 12
eq "retry: 13 calls" "$(calls)" 13
eq "retry: no failure recorded" "$(ls "$ROOT/results/raw/e1.failures" 2>/dev/null | wc -l | tr -d ' ')" 0

# skipped backends name the override
newtree skiphint
BACKENDS=raft drv e3_block.sh
contains "skipped backend names BEST_CLIENTS_RAFT" "$OUT" "BEST_CLIENTS_RAFT=<n>"

# --------------------------------------------------------- E3, E4, E5, E6 ----
newtree e345
FIXED_CLIENTS=32 drv e3_block.sh
eq "E3 exit" "$RC" 0
eq "E3 runs" "$(calls)" 36
eq "E3 block sizes" "$(grep 'BACKEND=raft ' "$ROOT/stub.log" | sed 's/.*BLOCK_SIZE=\([0-9]*\) .*/\1/' | sort -n -u | tr '\n' ' ')" "10 100 1000 "
FIXED_CLIENTS=32 drv e4_workload.sh
eq "E4 runs" "$(grep -c 'WORKLOAD=[abc] ' "$ROOT/stub.log")" $((36 + 36 + 0))
eq "E4 workloads" "$(rows e4)" 36
FIXED_CLIENTS=32 drv e5_rtt.sh
eq "E5 rtts" "$(awk -F, 'NR > 1 { print $10 }' "$ROOT/results/raw/e5.csv" | sort -n -u | tr '\n' ' ')" "0 20 60 "
eq "E5 rows" "$(rows e5)" 36
newtree e6
FIXED_CLIENTS=64 REPS=1 BACKENDS="tendermint kafka" drv e6_faults.sh
eq "E6 exit" "$RC" 0
eq "E6 runs (tendermint 12 + kafka 8 + kafka leader 4)" "$(calls)" 24
eq "E6 DURATION=180s" "$(grep -vc 'DURATION=180s' "$ROOT/stub.log")" 0
contains "E6 F5 N=7" "$(cat "$ROOT/stub.log")" "BACKEND=tendermint N=7 F=2 CLIENTS=64 BLOCK_SIZE=100 WORKLOAD=a RTT_MS=0 FAULT=F5-F4:node7,node6,node5@60 REP=1"
eq "E6 CSV fault column uses +" "$(awk -F, 'NR > 1 && $5 == 7 && $4 == "kafka" { print $11 }' "$ROOT/results/raw/e6.csv" | sort | tr '\n' ' ')" "F1:node1@60 F1:node7+node6@60 F2:node7+node6@60 F3:node1@60 F3:node7+node6@60 F4:node7+node6@60 "
: >"$ROOT/stub.log"
FIXED_CLIENTS=64 REPS=1 BACKENDS="tendermint kafka" drv e6_faults.sh
eq "E6 resumed: nothing re-run (the fault string with + matches)" "$(calls)" 0
newtree e6dur
FIXED_CLIENTS=64 REPS=1 E6_DURATION=60s BACKENDS=raft drv e6_faults.sh
eq "E6 AT must be < DURATION" "$RC" 1
contains "E6 AT must be < DURATION: message" "$OUT" "must be < DURATION"

# ------------------------------------------------------------- guards ---------
newtree guard
FIXED_CLIENTS=512 BACKENDS=tendermint drv e3_block.sh
eq "tendermint with CLIENTS > 256 is refused" "$RC" 1
eq "tendermint guard: nothing ran" "$(calls)" 0
contains "tendermint guard: message" "$OUT" "needs CLIENTS <= 256"
FIXED_CLIENTS=512 BACKENDS=tendermint drv e3_block.sh --dry-run
eq "tendermint guard also in --dry-run" "$RC" 1
# the whole plan is validated before the first run: kafka (first) must not run
FIXED_CLIENTS=512 BACKENDS="kafka raft tendermint" drv e3_block.sh
eq "bad tendermint plan after good backends: refused" "$RC" 1
eq "bad plan: not one run happened" "$(calls)" 0
eq "bad plan: no CSV" "$(rows e3)" 0
LOAD_THREADS=300 FIXED_CLIENTS=64 BACKENDS="kafka tendermint" drv e5_rtt.sh
eq "LOAD_THREADS plan refused before any run" "$(calls)" 0
FIXED_CLIENTS=512 BACKENDS="kafka tendermint" drv e3_block.sh --dry-run
eq "bad plan: --dry-run prints no plan line before dying" "$(printf '%s\n' "$OUT" | grep -c '^DRY_RUN: ')" 0
FIXED_CLIENTS=64 E6_NF="7:1" BACKENDS=raft drv e6_faults.sh --dry-run
eq "E6_NF with f != floor((N-1)/3) refused" "$RC" 1
contains "E6_NF message" "$OUT" "f must be floor((N-1)/3) = 2 for N=7"
FIXED_CLIENTS=64 E6_NF="4:1 7:2 10:3" BACKENDS=raft drv e6_faults.sh --dry-run
eq "E6_NF with consistent pairs accepted" "$RC" 0
DURATION=20s FIXED_CLIENTS=64 BACKENDS=raft drv e6_faults.sh --dry-run
contains "E6 says it ignores DURATION" "$OUT" "DURATION=20s is ignored: E6 runs DURATION=180s"
contains "E6 still uses 180s" "$OUT" "DURATION=180s"
FIXED_CLIENTS=64 BACKENDS=raft drv e6_faults.sh --dry-run
lacks "E6 quiet when DURATION is not set" "$OUT" "is ignored"
LOAD_THREADS=300 FIXED_CLIENTS=64 BACKENDS=tendermint drv e3_block.sh --dry-run
contains "tendermint LOAD_THREADS guard" "$OUT" "needs LOAD_THREADS <= 256"
COMMIT_MODE=async drv e1_clients.sh --dry-run
eq "COMMIT_MODE=async refused" "$RC" 1
COMMIT_MODE=sync drv e1_clients.sh --dry-run
eq "COMMIT_MODE=sync accepted" "$RC" 0
BATCH_FLUSH=100ms drv e1_clients.sh --dry-run
eq "BATCH_FLUSH != 1s needs another EXP" "$RC" 1
contains "BATCH_FLUSH message" "$OUT" "set EXP to a new name"
BATCH_FLUSH=100ms EXP=e1bf drv e1_clients.sh --dry-run
eq "BATCH_FLUSH with its own EXP" "$RC" 0
BATCH_FLUSH=1s drv e1_clients.sh --dry-run
eq "BATCH_FLUSH=1s is the default" "$RC" 0
drv e1_clients.sh --bogus
eq "unknown argument refused" "$RC" 1
drv e1_clients.sh --help
eq "--help exits 0" "$RC" 0
contains "--help prints the header" "$OUT" "E1 (RQ1/2)"
rm -f "$ROOT/results/ycsb/c.run.dat"
FIXED_CLIENTS=8 BACKENDS=raft drv e4_workload.sh
eq "missing YCSB data is refused before any run" "$RC" 1
eq "missing YCSB data: nothing ran" "$(calls)" 0

# ------------------------------------- params guard, disk guard, caffeinate ----
newtree params
BACKENDS=raft REPS=1 drv e1_clients.sh
eq "params: first run ok" "$RC" 0
[ -s "$ROOT/results/raw/e1.params" ] && ok || bad "params: no e1.params"
: >"$ROOT/stub.log"
DURATION=30s BACKENDS=raft REPS=1 drv e1_clients.sh
eq "params: other DURATION refused" "$RC" 1
contains "params: message names the setting" "$OUT" "DURATION=30s (EXP started with 60s)"
eq "params: nothing ran" "$(calls)" 0
CPUS=1 BACKENDS=raft REPS=1 drv e1_clients.sh --dry-run
eq "params: also in --dry-run" "$RC" 1
LOAD_THREADS=128 BACKENDS=raft REPS=2 drv e1_clients.sh
eq "params: LOAD_THREADS refused" "$RC" 1
sed -i.bak 's/^commit=.*/commit=0000000/' "$ROOT/results/raw/e1.params"
BACKENDS=raft REPS=2 drv e1_clients.sh
eq "params: other commit refused" "$RC" 1
contains "params: commit message" "$OUT" "(EXP started with 0000000)"
EXP_ALLOW_COMMIT_CHANGE=1 BACKENDS=raft REPS=2 drv e1_clients.sh
eq "params: EXP_ALLOW_COMMIT_CHANGE=1 goes on" "$RC" 0
eq "params: ... and runs rep 2" "$(calls)" 4
EXP=e1other DURATION=30s BACKENDS=raft REPS=1 drv e1_clients.sh
eq "params: a new EXP may use other settings" "$RC" 0
newtree disk
DF_STUB_AVAIL_KB=4194304 BACKENDS=raft drv e1_clients.sh
eq "disk: < 5 GiB free stops the batch" "$RC" 1
contains "disk: message" "$OUT" "MiB free on the disk"
eq "disk: no run" "$(calls)" 0
DF_STUB_AVAIL_KB=4194304 EXP_MIN_FREE_GB=2 BACKENDS=raft REPS=1 drv e1_clients.sh
eq "disk: EXP_MIN_FREE_GB lowers the limit" "$RC" 0
DF_STUB_AVAIL_KB=4194304 BACKENDS=raft drv e1_clients.sh --dry-run
eq "disk: not checked in --dry-run" "$RC" 0
newtree caff
: >"$CAFFEINATE_LOG"
NO_CAFFEINATE=1 BACKENDS=raft REPS=1 drv e1_clients.sh
eq "NO_CAFFEINATE=1: no re-exec" "$(grep -c . "$CAFFEINATE_LOG")" 0
HBDB_CAFFEINATED=1 BACKENDS=raft REPS=1 EXP=e1b drv e1_clients.sh
eq "already under caffeinate: no second re-exec" "$(grep -c . "$CAFFEINATE_LOG")" 0
STUB_RC=3 BACKENDS=raft REPS=1 EXP=e1c drv e1_clients.sh
eq "caffeinate keeps the driver's exit code" "$RC" 4
eq "caffeinate re-exec logged once" "$(grep -c . "$CAFFEINATE_LOG")" 1

# ---------------------------------------------------- kafka_ops.sh hook --------
newtree ops
cat >"$TMP_ROOT/bin/docker" <<'STUB'
#!/usr/bin/env bash
# Stub docker: `docker compose ... exec -T kafka sh -c SCRIPT sh nodeK` prints a describe table.
for last; do :; done
echo "docker $*" >>"$DOCKER_STUB_LOG"
[ -z "${DOCKER_STUB_FAIL:-}" ] || exit 1
case "$last" in
  node4) [ -z "${DOCKER_STUB_NOLINE:-}" ] || { echo "Consumer group '$last' has no active members."; exit 0; } ;;
esac
base=$(( ${last#node} * 100 ))
cur=$base
[ -z "${DOCKER_STUB_DASH:-}" ] || cur="-"
echo ""
echo "GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID"
echo "$last  shared-log  0  $cur  1000  $((1000 - base))  rdkafka-1  /172.28.0.1  rdkafka"
echo "$last  other-topic  0  7  8  1  rdkafka-1  /172.28.0.1  rdkafka"
STUB
chmod +x "$TMP_ROOT/bin/docker"
export DOCKER_STUB_LOG="$TMP_ROOT/docker.log"
hook() { # hook ENVASSIGNMENTS... -> runs the hook of the current tree
  OUT="$(env PATH="$TMP_ROOT/bin:$PATH" KAFKA_OPS_WAIT=0 "$@" "$ROOT/experiments/exp/kafka_ops.sh" 2>&1)"
  RC=$?
}
: >"$DOCKER_STUB_LOG"
hook EXP=e2 BACKEND=kafka N=4 CLIENTS=64 REP=2
eq "hook exit" "$RC" 0
OPS="$ROOT/results/raw/e2-kafka-ops.csv"
eq "hook header" "$(head -1 "$OPS")" "ts,exp,backend,n,clients,rep,reads,writes,current_offsets,log_end_offsets"
eq "hook row" "$(sed -n 2p "$OPS" | cut -d, -f2-)" "e2,kafka,4,64,2,1000,1000.00,100+200+300+400,1000+1000+1000+1000"
eq "hook calls the describe tool of the kafka service once per node" "$(grep -c 'exec -T kafka' "$DOCKER_STUB_LOG")" 4
contains "hook describes group node3" "$(cat "$DOCKER_STUB_LOG")" " sh node3"
hook EXP=e2 BACKEND=kafka N=7 CLIENTS=64 REP=1
eq "hook appends (header once)" "$(grep -c '^ts,' "$OPS")" 1
eq "hook appended row" "$(awk 'END { print NR }' "$OPS")" 3
hook EXP=e2 BACKEND=kafka N=4 CLIENTS=64 REP=3 DOCKER_STUB_DASH=1
eq "a '-' current offset is marked, reads left empty (not 0)" "$(tail -1 "$OPS" | cut -d, -f7-)" ",1000.00,-+-+-+-,1000+1000+1000+1000"
contains "a '-' current offset: warning" "$OUT" "no committed offset yet for node1 node2 node3 node4"
hook EXP=e2 BACKEND=raft N=4 CLIENTS=64 REP=1
eq "hook ignores other backends" "$RC" 0
eq "hook ignores other backends: no row" "$(awk 'END { print NR }' "$OPS")" 4
hook EXP=e2 BACKEND=kafka N=4 CLIENTS=64 REP=4 DOCKER_STUB_FAIL=1
eq "describe failure fails the hook" "$RC" 1
eq "describe failure: no partial row" "$(awk 'END { print NR }' "$OPS")" 4
hook EXP=e2 BACKEND=kafka N=4 CLIENTS=64 REP=4 DOCKER_STUB_NOLINE=1
eq "no topic line fails the hook" "$RC" 1
contains "no topic line: message" "$OUT" "no usable shared-log line for group node4"
eq "no topic line: no partial row" "$(awk 'END { print NR }' "$OPS")" 4
hook EXP=e2 BACKEND=kafka N=4 CLIENTS=64 REP=5 KAFKA_OPS_CSV="$TMP_ROOT/other.csv"
eq "KAFKA_OPS_CSV overrides the file" "$(awk 'END { print NR }' "$TMP_ROOT/other.csv")" 2
hook EXP=e2 BACKEND=kafka N=x CLIENTS=64 REP=1
eq "bad N is refused" "$RC" 1

# ------------------------------------- run_one.sh: BATCH_FLUSH / hook checks ----
# These die in the validation, before run_one.sh takes the lock or touches Docker.
run_one_check() { # run_one_check ENV... -> status in RC, output in OUT
  OUT="$(env EXP=zzcheck BACKEND=raft N=4 "$@" "$SRC_ROOT/experiments/run_one.sh" 2>&1)"
  RC=$?
}
run_one_check BATCH_FLUSH=0s
eq "BATCH_FLUSH=0s refused" "$RC" 1
contains "BATCH_FLUSH=0s message" "$OUT" "BATCH_FLUSH must be a positive duration"
run_one_check BATCH_FLUSH=abc
eq "BATCH_FLUSH=abc refused" "$RC" 1
run_one_check BATCH_FLUSH=1s BACKEND=kafka-orig COMMIT_MODE=async
eq "BATCH_FLUSH on kafka-orig refused" "$RC" 1
contains "BATCH_FLUSH on kafka-orig message" "$OUT" "original server without it"
run_one_check BATCH_FLUSH=1s BACKEND=tm-orig
eq "BATCH_FLUSH on tm-orig refused" "$RC" 1
run_one_check PRE_DOWN_HOOK=/nonexistent/hook
eq "missing PRE_DOWN_HOOK refused" "$RC" 1
contains "missing PRE_DOWN_HOOK message" "$OUT" "is not an executable file"
eq "run_one checks never reached docker" "$(grep -c . "$TMP_ROOT_DOCKER_LOG")" 0
eq "refused runs create nothing" "$(ls "$SRC_ROOT/results/raw/zzcheck" 2>/dev/null | wc -l | tr -d ' ')" 0
contains "tag suffix for a non-default BATCH_FLUSH" "$(grep -F 'tag="${tag}_bf${BATCH_FLUSH}"' "$SRC_ROOT/experiments/run_one.sh")" '_bf'

echo "test_exp.sh: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
