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
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="$(cd "$HERE/../.." && pwd)"

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/test_exp.XXXXXX")"
[ -n "${KEEP_TMP:-}" ] || trap 'rm -rf "$TMP_ROOT"' EXIT
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
# Stub of run_one.sh: log the environment, append one row (22 columns).
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "EXP=$EXP BACKEND=$BACKEND N=$N F=$F CLIENTS=$CLIENTS BLOCK_SIZE=$BLOCK_SIZE WORKLOAD=$WORKLOAD RTT_MS=$RTT_MS FAULT=$FAULT REP=$REP COMMIT_MODE=$COMMIT_MODE DURATION=${DURATION:-} HOOK=${PRE_DOWN_HOOK:-}" >>"$root/stub.log"
if [ -n "${STUB_RC:-}" ] && { [ -z "${STUB_BACKEND:-}" ] || [ "$STUB_BACKEND" = "$BACKEND" ]; }; then
  exit "$STUB_RC"
fi
tps=100
if [ "${STUB_NOISY_REP2:-}" = 1 ] && [ "$REP" = 2 ]; then tps=300; fi
csv="$root/results/raw/$EXP.csv"
[ -s "$csv" ] || echo "ts,commit,exp,backend,n,f,clients,block_size,workload,rtt_ms,fault,rep,commit_mode,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,wrong_gets,host_cpu_max_pct,saturated,net_bytes_per_block" >"$csv"
echo "2026-10-30T00:00:00Z,abc1234,$EXP,$BACKEND,$N,$F,$CLIENTS,$BLOCK_SIZE,$WORKLOAD,$RTT_MS,$(echo "$FAULT" | tr ',' '+'),$REP,$COMMIT_MODE,$tps,1,1,1,0,0,10,0," >>"$csv"
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
for spec in "e1_clients.sh 48" "e3_block.sh 36" "e4_workload.sh 36" "e5_rtt.sh 36" "e2_nodes.sh 24" "e6_faults.sh 120"; do
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
eq "F5_KINDS=F5-F4 leaves 36 configs... 108 runs" "$OUT" 108
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
eq "E1 clients" "$(grep 'BACKEND=raft ' "$ROOT/stub.log" | sed 's/.*CLIENTS=\([0-9]*\) .*REP=\([0-9]\) .*/\1\/\2/' | tr '\n' ' ')" "4/1 4/2 4/3 16/1 16/2 16/3 64/1 64/2 64/3 256/1 256/2 256/3 "
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
eq "E6 runs (tendermint 12 + kafka 8)" "$(calls)" 20
eq "E6 DURATION=180s" "$(grep -vc 'DURATION=180s' "$ROOT/stub.log")" 0
contains "E6 F5 N=7" "$(cat "$ROOT/stub.log")" "BACKEND=tendermint N=7 F=2 CLIENTS=64 BLOCK_SIZE=100 WORKLOAD=a RTT_MS=0 FAULT=F5-F4:node7,node6,node5@60 REP=1"
eq "E6 CSV fault column uses +" "$(awk -F, 'NR > 1 && $5 == 7 && $4 == "kafka" { print $11 }' "$ROOT/results/raw/e6.csv" | sort | tr '\n' ' ')" "F1:node7+node6@60 F2:node7+node6@60 F3:node7+node6@60 F4:node7+node6@60 "
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

# ---------------------------------------------------- kafka_ops.sh hook --------
newtree ops
mkdir -p "$TMP_ROOT/bin"
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
eq "a '-' current offset is 0" "$(tail -1 "$OPS" | cut -d, -f7-)" "0,1000.00,0+0+0+0,1000+1000+1000+1000"
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
eq "refused runs create nothing" "$(ls "$SRC_ROOT/results/raw/zzcheck" 2>/dev/null | wc -l | tr -d ' ')" 0
contains "tag suffix for a non-default BATCH_FLUSH" "$(grep -F 'tag="${tag}_bf${BATCH_FLUSH}"' "$SRC_ROOT/experiments/run_one.sh")" '_bf'

echo "test_exp.sh: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
