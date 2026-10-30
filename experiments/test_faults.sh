#!/usr/bin/env bash
# Unit tests for experiments/faults.sh, experiments/net.sh, the fault helpers
# of lib.sh and the fault handling of run_one.sh. No Docker needed: docker,
# hbdb_compose, the clock and sleep are stubbed.
#
#   experiments/test_faults.sh        (exit 0 = all passed)
#
# Bash 3.2 compatible, like the scripts it tests.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/lib.sh"
. "$HERE/faults.sh"

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/test_faults.XXXXXX")"
[ -n "${KEEP_TMP:-}" ] || trap 'rm -rf "$TMP_ROOT"' EXIT # KEEP_TMP=1: keep it for debugging
CALLS="$TMP_ROOT/calls"
: >"$CALLS"
pass=0
fail=0

ok() {
  pass=$((pass + 1))
}
bad() {
  fail=$((fail + 1))
  echo "FAIL: $*" >&2
}
# eq NAME GOT WANT
eq() {
  if [ "$2" = "$3" ]; then ok; else bad "$1: got '$2', want '$3'"; fi
}
# contains NAME HAYSTACK NEEDLE
contains() {
  case "$2" in
    *"$3"*) ok ;;
    *) bad "$1: '$3' not in: $2" ;;
  esac
}

# ---- stubs ----
FAKE_NOW=1000000
SLEPT_MS=0
hbdb_epoch_ms() { echo "$FAKE_NOW"; }
sleep() {
  local s="$1" ms
  case "$s" in
    *.*) ms=$((10#${s%.*} * 1000 + 10#$(printf '%-3s' "${s#*.}" | tr ' ' 0))) ;;
    *) ms=$((10#$s * 1000)) ;;
  esac
  FAKE_NOW=$((FAKE_NOW + ms))
  SLEPT_MS=$((SLEPT_MS + ms))
}
DOCKER_STOP_RC=0
TIMES="$TMP_ROOT/times" # "<call> <fake epoch ms>" per stubbed docker call
docker() {
  echo "docker $*" >>"$CALLS"
  echo "docker_$1 $FAKE_NOW" >>"$TIMES"
  case "$1" in
    stop) return "$DOCKER_STOP_RC" ;;
  esac
  return 0
}
COMPOSE_EXEC_RC=0
hbdb_compose() {
  echo "compose $*" >>"$CALLS"
  echo "compose_$1 $FAKE_NOW" >>"$TIMES"
  case "$1" in
    ps) [ "$3" = node9 ] || echo "cid_$3" ;;
    exec)
      if [ "${4:-}" = sh ] && [ "${5:-}" = -c ]; then
        printf '%s\n' "$6" >"$TMP_ROOT/last_script"
      fi
      return "$COMPOSE_EXEC_RC"
      ;;
  esac
  return 0
}
reset() {
  : >"$CALLS"
  FAKE_NOW=1000000
  SLEPT_MS=0
  FAULT_T0_MS=""
  FAULT_WARMUP_MS=0
  FAULT_LOG="$TMP_ROOT/faults.csv"
  FAULT_STOPPED="$TMP_ROOT/stopped"
  FAULT_STOP="$TMP_ROOT/stop"
  NET_BASE_DELAY_MS=0
  DOCKER_STOP_RC=0
  COMPOSE_EXEC_RC=0
  FAULT_PARENT_PID=""
  rm -f "$FAULT_LOG" "$FAULT_STOPPED" "$FAULT_STOP" "$TMP_ROOT/last_script"
  : >"$TIMES"
}

# parse_rc SPEC BACKEND N DUR: run fault_parse in a subshell (die exits);
# prints "rc|stderr".
parse_rc() {
  local err rc
  err="$( (fault_parse "$@") 2>&1 >/dev/null)"
  rc=$?
  echo "$rc|$err"
}

# ---- lib.sh: consensus ports ----
eq "ports raft" "$(hbdb_consensus_ports raft)" "7100 7101"
eq "ports tendermint" "$(hbdb_consensus_ports tendermint)" "26656"
for b in kafka-orig tm-orig kafka bft2; do
  eq "ports $b" "$(hbdb_consensus_ports "$b")" ""
done
# The constants must match gen_compose.py.
gc="$HERE/cluster/gen_compose.py"
eq "RAFT_PORT matches gen_compose" "$(sed -n 's/^RAFT_PORT = \([0-9]*\).*/\1/p' "$gc")" "$HBDB_RAFT_PORT"
eq "RAFT_FORWARD_PORT matches gen_compose" "$(sed -n 's/^RAFT_FORWARD_PORT = \([0-9]*\).*/\1/p' "$gc")" "$HBDB_RAFT_FORWARD_PORT"
eq "TM_P2P_PORT matches gen_compose" "$(sed -n 's/^TM_P2P_PORT = \([0-9]*\).*/\1/p' "$gc")" "$HBDB_TM_P2P_PORT"
grep -q '"cap_add"\] = \["NET_ADMIN"\]' "$gc" && ok || bad "gen_compose.py no longer gives the nodes NET_ADMIN"

# ---- default fault nodes ----
eq "default 4/1" "$(fault_default_nodes 4 1)" "node4"
eq "default 7/2" "$(fault_default_nodes 7 2)" "node7,node6"
eq "default 4/3" "$(fault_default_nodes 4 3)" "node4,node3,node2"
r="$( (fault_default_nodes 4 4) 2>/dev/null)"; rc=$?
eq "default 4/4 rejected (would hit node1)" "$rc" 1
r="$( (fault_default_nodes 4 0) 2>/dev/null)"; rc=$?
eq "default 4/0 rejected" "$rc" 1

# ---- parser: valid ----
fault_parse none raft 4 40
eq "none kind" "$FAULT_KIND" none
fault_parse F1:node4@20 raft 4 40
eq "F1 kind" "$FAULT_KIND" F1
eq "F1 nodes" "$FAULT_NODES" node4
eq "F1 at" "$FAULT_AT" 20
fault_parse F1:node2,node3@60 kafka 4 180
eq "F1 two nodes" "$FAULT_NODES" "node2 node3"
eq "F1 two at" "$FAULT_AT" 60
fault_parse F1:node7+node6@060 tendermint 7 180
eq "F1 plus separator" "$FAULT_NODES" "node7 node6"
eq "AT leading zero is decimal" "$FAULT_AT" 60
fault_parse F3:node1@60 kafka-orig 4 180
eq "F3 leader" "$FAULT_KIND/$FAULT_NODES" "F3/node1"
fault_parse F2:node4@60 raft 4 180
eq "F2 raft" "$FAULT_KIND/$FAULT_NODES/$FAULT_BACKEND" "F2/node4/raft"
fault_parse F2:node4@0 tendermint 4 180
eq "F2 tendermint at 0" "$FAULT_AT" 0
fault_parse "F1:$(fault_default_nodes 7 2)@60" raft 7 180
eq "default nodes round-trip" "$FAULT_NODES" "node7 node6"

# ---- parser: rejected ----
r="$(parse_rc F1:node5@20 raft 4 40)"
eq "node > N rc" "${r%%|*}" 1
contains "node > N msg" "$r" "node5 does not exist (N=4"
r="$(parse_rc F1:node0@20 raft 4 40)"
eq "node0 rc" "${r%%|*}" 1
r="$(parse_rc F1:node4,node4@20 raft 4 40)"
contains "duplicate" "$r" "listed twice"
r="$(parse_rc F1:node4@40 raft 4 40)"
contains "AT >= DURATION" "$r" "AT=40 must be < DURATION (40s)"
r="$(parse_rc F1:node1,node2,node3,node4@10 raft 4 40)"
contains "F1 all nodes" "$r" "may not stop every node"
r="$(parse_rc F2:node4@20 kafka 4 40)"
contains "F2 kafka" "$r" "Task 15"
r="$(parse_rc F2:node4@20 kafka-orig 4 40)"
contains "F2 kafka-orig" "$r" "Task 15"
r="$(parse_rc F2:node4@20 tm-orig 4 40)"
contains "F2 tm-orig" "$r" "no network-silent mode"
r="$(parse_rc F2:node4@20 bft2 4 40)"
contains "F2 bft2 (no port yet)" "$r" "no network-silent mode"
r="$(parse_rc F4:node4@20 raft 4 40)"
contains "F4" "$r" "F4 (wrong state) needs Task 15"
r="$(parse_rc F5:node3,node4@20 tendermint 4 40)"
contains "F5" "$r" "F5 (f+1 nodes of F2/F4"
r="$(parse_rc F9:node4@20 raft 4 40)"
contains "F9" "$r" "unknown fault kind F9"
for s in F1 F1:node4 F1:@20 F1:node4@ f1:node4@20 F1:n4@20 "F1:node4@2s" F1:node4@20,; do
  r="$(parse_rc "$s" raft 4 40)"
  eq "malformed '$s'" "${r%%|*}" 1
done

# ---- net.sh ----
eq "half 20" "$(net_half_ms 20)" 10
eq "half 25" "$(net_half_ms 25)" 12.5
eq "add 10+200" "$(net_add_ms 10 200)" 210
reset
N=4
net_set_rtt 20 2>/dev/null
eq "rtt base" "$NET_BASE_DELAY_MS" 10
eq "rtt exec count" "$(grep -c 'tc qdisc replace dev eth0 root netem delay 10ms' "$CALLS")" 4
for i in 1 2 3 4; do
  grep -q "^compose exec -T node$i tc qdisc replace dev eth0 root netem delay 10ms limit 100000$" "$CALLS" && ok || bad "rtt node$i"
done
reset
N=4
net_set_rtt 0 2>/dev/null
contains "rtt 0 removes qdisc" "$(cat "$CALLS")" "tc qdisc del dev eth0 root"
reset
N=2
COMPOSE_EXEC_RC=1
(net_set_rtt 20) 2>/dev/null; rc=$?
eq "rtt tc failure" "$rc" 1

# ---- wait arithmetic ----
reset
FAULT_T0_MS=$((FAKE_NOW + 10000)) # t=0 after a 10 s warmup
fault_wait_until 20; rc=$?
eq "wait rc" "$rc" 0
eq "wait ends at t0+AT" "$FAKE_NOW" $((1000000 + 30000))
eq "wait slept" "$SLEPT_MS" 30000
reset
FAULT_T0_MS=$((FAKE_NOW - 5000)) # already past
fault_wait_until 2; rc=$?
eq "wait past rc" "$rc" 0
eq "wait past no sleep" "$SLEPT_MS" 0
reset
FAULT_T0_MS="$FAKE_NOW"
: >"$FAULT_STOP"
fault_wait_until 20; rc=$?
eq "wait aborted by stop file" "$rc" 1

# ---- F1 via fault_run: marker, t=0 = marker + WARMUP, AT ----
reset
bench_log="$TMP_ROOT/bench.log"
printf 'ycsbbench: loaded 10 records\nycsbbench: run phase started (warmup 10s, duration 40s)\n' >"$bench_log"
FAULT_WARMUP_MS=10000
fault_parse F1:node3,node4@20 raft 4 40
fault_run "$bench_log" 2>/dev/null; rc=$?
eq "F1 run rc" "$rc" 0
eq "F1 t0" "$FAULT_T0_MS" 1010000
eq "F1 injected at marker+warmup+AT" "$FAKE_NOW" 1030000
grep -q '^docker stop cid_node3 cid_node4$' "$CALLS" && ok || bad "F1 docker stop call: $(cat "$CALLS")"
eq "F1 stopped file" "$(tr '\n' ' ' <"$FAULT_STOPPED")" "node3 node4 "
eq "faults header" "$(head -n 1 "$FAULT_LOG")" "$FAULT_LOG_HEADER"
line="$(grep ',run_phase_start,' "$FAULT_LOG")"
eq "run_phase_start offsets" "$(echo "$line" | cut -d, -f3,4)" "-10.000,0.000"
line="$(grep ',docker_stop,node3+node4,' "$FAULT_LOG")"
eq "F1 log offsets" "$(echo "$line" | cut -d, -f2,3,4,5)" "1030000,20.000,30.000,F1:node3+node4@20"
echo "$line" | cut -d, -f1 | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$' && ok || bad "F1 log utc: $line"
eq "faults log fields" "$(awk -F, '{ print NF }' "$FAULT_LOG" | sort -u)" 8
grep -q ',stopped,node3+node4,' "$FAULT_LOG" && ok || bad "F1 stopped line"

# The container ids are looked up before the wait, the stop is issued at AT.
eq "F1 ids resolved before the wait" "$(grep '^compose_ps ' "$TIMES" | awk '{ print $2 }' | sort -u)" 1000000
eq "F1 docker stop at exactly t0+AT" "$(grep '^docker_stop ' "$TIMES" | awk '{ print $2 }')" 1030000

# docker stop fails -> rc 1
reset
FAULT_T0_MS="$FAKE_NOW"
DOCKER_STOP_RC=1
fault_crash node4 0 2>/dev/null; rc=$?
eq "F1 docker stop failure rc" "$rc" 1
grep -q ',docker_stop_failed,node4,' "$FAULT_LOG" && ok || bad "F1 failure logged"
# no container -> rc 1, nothing stopped
reset
FAULT_T0_MS="$FAKE_NOW"
fault_crash node9 0 2>/dev/null; rc=$?
eq "F1 missing container rc" "$rc" 1
grep -q '^docker stop' "$CALLS" && bad "F1 stopped despite missing container" || ok
# aborted before AT -> rc 2, nothing stopped
reset
FAULT_T0_MS="$FAKE_NOW"
: >"$FAULT_STOP"
fault_crash node4 5; rc=$?
eq "F1 aborted rc" "$rc" 2
[ -s "$FAULT_STOPPED" ] && bad "F1 aborted but stopped file written" || ok
# marker never seen + stop file -> fault_run rc 2
reset
: >"$TMP_ROOT/empty.log"
: >"$FAULT_STOP"
fault_parse F1:node4@20 raft 4 40
fault_run "$TMP_ROOT/empty.log"; rc=$?
eq "fault_run without marker" "$rc" 2

# orphan guard: the parent run_one.sh is gone -> abort, nothing injected
reset
true &
dead_pid=$!
wait "$dead_pid"
FAULT_PARENT_PID="$dead_pid"
FAULT_T0_MS="$FAKE_NOW"
fault_crash node4 5 2>/dev/null; rc=$?
eq "orphan: F1 aborted" "$rc" 2
grep -q '^docker stop' "$CALLS" && bad "orphan injector stopped a container" || ok
reset
FAULT_PARENT_PID="$dead_pid"
printf 'ycsbbench: run phase started\n' >"$TMP_ROOT/m.log"
fault_parse F3:node1@1 raft 4 40
fault_run "$TMP_ROOT/m.log" 2>/dev/null; rc=$?
eq "orphan: fault_run aborted" "$rc" 2
grep -q 'tc qdisc' "$CALLS" && bad "orphan injector ran tc" || ok
reset
: >"$TMP_ROOT/empty2.log"
FAULT_PARENT_PID="$dead_pid"
fault_wait_marker "$TMP_ROOT/empty2.log" 2>/dev/null; rc=$?
eq "orphan: marker wait aborted" "$rc" 1
reset
FAULT_PARENT_PID=$$ # alive: no abort
FAULT_T0_MS="$FAKE_NOW"
fault_wait_until 1; rc=$?
eq "live parent: wait completes" "$rc" 0

# ---- F3 slow ----
reset
FAULT_T0_MS="$FAKE_NOW"
fault_slow node1 3 200 2>/dev/null; rc=$?
eq "F3 rc" "$rc" 0
grep -q '^compose exec -T node1 tc qdisc replace dev eth0 root netem delay 200ms limit 100000$' "$CALLS" && ok || bad "F3 tc call: $(cat "$CALLS")"
eq "F3 at AT" "$FAKE_NOW" 1003000
grep -q ',netem_applied,node1,tc returned$' "$FAULT_LOG" && ok || bad "F3 applied line: $(cat "$FAULT_LOG")"
reset
FAULT_T0_MS="$FAKE_NOW"
NET_BASE_DELAY_MS=30 # RTT_MS=60
fault_slow node1 0 200 2>/dev/null
grep -q 'netem delay 230ms limit 100000$' "$CALLS" && ok || bad "F3 on top of RTT: $(cat "$CALLS")"
grep -q ',netem_delay,node1,delay 230ms = slow 200ms + base 30ms on eth0$' "$FAULT_LOG" && ok || bad "F3 log: $(cat "$FAULT_LOG")"
reset
FAULT_T0_MS="$FAKE_NOW"
COMPOSE_EXEC_RC=1
fault_slow node1 0 200 2>/dev/null; rc=$?
eq "F3 tc failure rc" "$rc" 1

# ---- F2 network silent ----
reset
FAULT_T0_MS="$FAKE_NOW"
fault_silent_net node4 1 "$(hbdb_consensus_ports raft)" 2>/dev/null; rc=$?
eq "F2 rc" "$rc" 0
grep -q ',iptables_applied,node4,iptables returned$' "$FAULT_LOG" && ok || bad "F2 applied line: $(cat "$FAULT_LOG")"
grep -q '^compose exec -T node4 sh -c ' "$CALLS" && ok || bad "F2 exec call: $(cat "$CALLS")"
grep -q ',iptables_drop,node4,tcp OUTPUT --dport/--sport 7100 7101$' "$FAULT_LOG" && ok || bad "F2 log: $(cat "$FAULT_LOG")"
# Run the generated script with a stub iptables: the first time every rule is
# missing (-C fails) and gets appended; the second time nothing is added.
stub_bin="$TMP_ROOT/bin"
mkdir -p "$stub_bin"
cat >"$stub_bin/iptables" <<'EOF'
#!/bin/sh
echo "iptables $*" >>"$IPT_CALLS"
case " $* " in
  *" -C "*) [ -n "${IPT_PRESENT:-}" ] && exit 0 || exit 1 ;;
esac
exit 0
EOF
chmod +x "$stub_bin/iptables"
IPT_CALLS="$TMP_ROOT/ipt"
: >"$IPT_CALLS"
IPT_CALLS="$IPT_CALLS" PATH="$stub_bin:$PATH" sh -c "$(cat "$TMP_ROOT/last_script")"; rc=$?
eq "F2 script rc" "$rc" 0
for want in "--dport 7100" "--sport 7100" "--dport 7101" "--sport 7101"; do
  grep -q "^iptables -w -A OUTPUT -p tcp $want -j DROP$" "$IPT_CALLS" && ok || bad "F2 rule $want: $(cat "$IPT_CALLS")"
done
eq "F2 appends" "$(grep -c ' -A ' "$IPT_CALLS")" 4
: >"$IPT_CALLS"
IPT_PRESENT=1 IPT_CALLS="$IPT_CALLS" PATH="$stub_bin:$PATH" sh -c "$(cat "$TMP_ROOT/last_script")"
eq "F2 idempotent (no appends when present)" "$(grep -c ' -A ' "$IPT_CALLS")" 0
# tendermint port
reset
FAULT_T0_MS="$FAKE_NOW"
fault_silent_net node4 0 "$(hbdb_consensus_ports tendermint)" 2>/dev/null
: >"$IPT_CALLS"
IPT_CALLS="$IPT_CALLS" PATH="$stub_bin:$PATH" sh -c "$(cat "$TMP_ROOT/last_script")"
eq "F2 tendermint rules" "$(grep -c -e '-A OUTPUT -p tcp --[ds]port 26656 -j DROP' "$IPT_CALLS")" 2
eq "F2 tendermint only its port" "$(grep -c ' -A ' "$IPT_CALLS")" 2
# empty port list dies
reset
(fault_silent_net node4 0 "") 2>/dev/null; rc=$?
eq "F2 empty ports dies" "$rc" 1
# F2 through fault_inject: every node, every port
reset
FAULT_T0_MS="$FAKE_NOW"
fault_parse F2:node3,node4@0 raft 4 40
fault_inject 2>/dev/null; rc=$?
eq "F2 inject rc" "$rc" 0
eq "F2 inject execs" "$(grep -c '^compose exec -T node[34] sh -c' "$CALLS")" 2

# ---- run_one.sh check_containers: skips exactly the F1-stopped services ----
eval "$(sed -n '/^check_containers() {/,/^}/p' "$HERE/run_one.sh")"
INSPECT="$TMP_ROOT/inspect"
docker() {
  case "$1" in
    ps) awk '{ print $1 }' "$INSPECT" ;;
    inspect)
      local id
      for id; do :; done
      awk -v id="$id" '$1 == id { print $2, $3, $4, $5 }' "$INSPECT"
      ;;
  esac
}
fault_stopped="$TMP_ROOT/cc_stopped"
FAULT=F1:node4@20
printf 'c1 node1 running false 0\nc4 node4 exited false 0\nck client running false 0\n' >"$INSPECT"
echo node4 >"$fault_stopped"
check_containers 2>/dev/null; eq "check: F1-stopped node4 ok" "$?" 0
printf 'c1 node1 running false 0\nc3 node3 exited false 137\nc4 node4 exited false 0\n' >"$INSPECT"
check_containers 2>/dev/null; eq "check: unexpected node3 death" "$?" 1
printf 'c4 node4 exited true 137\n' >"$INSPECT"
check_containers 2>/dev/null; eq "check: OOM-killed stopped node still counts" "$?" 1
rm -f "$fault_stopped"
printf 'c4 node4 exited false 0\n' >"$INSPECT"
check_containers 2>/dev/null; eq "check: no fault, stopped node fails" "$?" 1
printf 'c1 node1 running false 0\n' >"$INSPECT"
check_containers 2>/dev/null; eq "check: all running" "$?" 0
echo node4 >"$fault_stopped"
printf 'c4 node14 exited false 0\n' >"$INSPECT"
check_containers 2>/dev/null; eq "check: node14 is not node4" "$?" 1

# ---- run_one.sh input validation (before any Docker call) ----
fakebin="$TMP_ROOT/fakebin"
mkdir -p "$fakebin"
printf '#!/bin/sh\necho "docker $*" >>"%s"\nexit 1\n' "$TMP_ROOT/real_docker_calls" >"$fakebin/docker"
chmod +x "$fakebin/docker"
: >"$TMP_ROOT/real_docker_calls"
empty_ycsb="$TMP_ROOT/ycsb"
mkdir -p "$empty_ycsb"
# run_validate ENV...: rc|stderr of run_one.sh with ENV (no ycsb data, so a
# valid configuration stops at the "missing ... load.dat" check).
run_validate() {
  local err rc
  err="$(env PATH="$fakebin:$PATH" YCSB_DIR="$empty_ycsb" EXP=ftest N=4 DURATION=40s "$@" "$HERE/run_one.sh" 2>&1 >/dev/null)"
  rc=$?
  echo "$rc|$err"
}
r="$(run_validate BACKEND=raft FAULT=F1:node4@20)"
contains "run_one F1 raft parses" "$r" "missing $empty_ycsb/a.load.dat"
r="$(run_validate BACKEND=raft FAULT=F1:node3,node4@20)"
contains "run_one comma list accepted" "$r" "missing $empty_ycsb/a.load.dat"
r="$(run_validate BACKEND=raft RTT_MS=20)"
contains "run_one RTT_MS accepted" "$r" "missing $empty_ycsb/a.load.dat"
r="$(run_validate BACKEND=kafka FAULT=F2:node4@20)"
eq "run_one F2 kafka rc" "${r%%|*}" 1
contains "run_one F2 kafka msg" "$r" "Task 15"
r="$(run_validate BACKEND=raft FAULT=F4:node4@20)"
contains "run_one F4 msg" "$r" "Task 15"
r="$(run_validate BACKEND=raft FAULT=F1:node5@20)"
contains "run_one node5 N=4" "$r" "node5 does not exist"
r="$(run_validate BACKEND=raft FAULT=F1:node4@40)"
contains "run_one AT >= DURATION" "$r" "must be < DURATION"
r="$(run_validate BACKEND=raft FAULT='F1:node4;x@1')"
contains "run_one bad chars" "$r" "FAULT must match"
eq "run_one validation touched no docker" "$(cat "$TMP_ROOT/real_docker_calls")" ""

# ---- run_one.sh end to end with Docker and the cluster scripts stubbed ----
# A copy of the harness in a sandbox repo whose cluster/{down,up,health}.sh
# are no-ops and whose `docker` (first on PATH) fakes compose, stats,
# inspect, stop and a ycsbbench that prints the run-phase marker, sleeps
# STUB_BENCH_SECS and writes its row and timeline. This covers the wiring:
# RTT netem logging order, the scheduler, stop_faults / fault_rc -> exit 3,
# the check_containers exemption, the CSV-safe fault column and cleanup.
SB="$TMP_ROOT/sb"
mkdir -p "$SB/experiments/cluster" "$SB/bin" "$SB/ycsb" "$SB/state"
cp "$HERE/run_one.sh" "$HERE/lib.sh" "$HERE/net.sh" "$HERE/faults.sh" "$SB/experiments/"
for s in down up health; do
  printf '#!/bin/sh\necho %s >>"$STUB_STATE/cluster.log"\nexit 0\n' "$s" >"$SB/experiments/cluster/$s.sh"
  chmod +x "$SB/experiments/cluster/$s.sh"
done
echo "READ k1" >"$SB/ycsb/a.load.dat"
echo "READ k1" >"$SB/ycsb/a.run.dat"
cat >"$SB/bin/docker" <<'EOF'
#!/bin/sh
# Fake docker for test_faults.sh. State in $STUB_STATE.
echo "docker $*" >>"$STUB_STATE/calls"
svc_of() { case "$1" in ck) echo client ;; c*) echo "node${1#c}" ;; esac; }
case "$1" in
  events) exec sleep 600 ;;
  info) echo 4; exit 0 ;;
  stats) for a; do case "$a" in c*) echo "1.00%" ;; esac; done; sleep 0.2; exit 0 ;;
  ps) printf 'c1\nc2\nc3\nc4\nck\n'; exit 0 ;;
  inspect)
    for id; do :; done
    if [ -e "$STUB_STATE/stopped_$id" ]; then echo "$(svc_of "$id") exited false 0"
    elif [ "$id" = "${STUB_DEAD:-}" ]; then echo "$(svc_of "$id") exited false 137"
    else echo "$(svc_of "$id") running false 0"; fi
    exit 0 ;;
  stop)
    shift
    [ "${STUB_STOP_RC:-0}" = 0 ] || exit "$STUB_STOP_RC"
    for id; do : >"$STUB_STATE/stopped_$id"; done
    exit 0 ;;
  compose)
    shift 5 # compose -p hbdb -f FILE
    case "$1" in
      ps) if [ -n "${3:-}" ]; then echo "c${3#node}"; else printf 'c1\nc2\nc3\nc4\nck\n'; fi; exit 0 ;;
      exec)
        shift 2 # exec -T
        svc="$1"
        shift
        if [ "$svc" != client ]; then exit "${STUB_EXEC_RC:-0}"; fi
        out="" tl="" tags=""
        for a; do
          case "$a" in
            --out=*) out="$STUB_RESULTS${a#--out=/results}" ;;
            --timeline=*) tl="$STUB_RESULTS${a#--timeline=/results}" ;;
            --tags=*) tags="${a#--tags=}" ;;
          esac
        done
        echo "ycsbbench: loaded 1 records"
        echo "ycsbbench: run phase started (warmup x, duration y)"
        sleep "${STUB_BENCH_SECS:-1}"
        printf '5\n5\n5\n' >"$tl"
        vals="$(echo "$tags" | tr ',' '\n' | sed 's/^[^=]*=//' | tr '\n' ',' | sed 's/,$//')"
        printf '%s\n%s,100.0,1.0,1.0,2.0,0,0,,,\n' "$STUB_HEADER" "$vals" >"$out"
        exit "${STUB_BENCH_RC:-0}" ;;
      *) exit 0 ;;
    esac ;;
  *) exit 0 ;;
esac
EOF
chmod +x "$SB/bin/docker"
# run_one.sh's host_state reads vm.loadavg; some sandboxes forbid sysctl.
printf '#!/bin/sh\necho "{ 1.00 1.00 1.00 }"\n' >"$SB/bin/sysctl"
chmod +x "$SB/bin/sysctl"
SB_RAW="$SB/results/raw"
# itest NAME ENV...: run the sandboxed run_one.sh; sets it_rc, it_err.
itest() {
  local name="$1"
  shift
  rm -rf "$SB/state"
  mkdir -p "$SB/state"
  it_err="$(env PATH="$SB/bin:$PATH" STUB_STATE="$SB/state" STUB_RESULTS="$SB_RAW" STUB_HEADER="$HBDB_CSV_HEADER" \
    YCSB_DIR="$SB/ycsb" BACKEND=raft N=4 WARMUP=0s "$@" "$SB/experiments/run_one.sh" 2>&1 >/dev/null)"
  it_rc=$?
}
raw_tag() { # raw_tag EXP: the single .faults file of the experiment
  ls "$SB_RAW/$1"/*.faults 2>/dev/null
}

# A: RTT only -> .faults keeps the netem_rtt record; netem with limit.
itest rtt EXP=it_rtt RTT_MS=20 DURATION=2s STUB_BENCH_SECS=0.3
eq "itest rtt rc" "$it_rc" 0
f="$(raw_tag it_rtt)"
[ -n "$f" ] && ok || bad "itest rtt: no .faults file: $it_err"
grep -q ',netem_rtt,node1-node4,egress delay 10ms on every node (RTT_MS=20)$' "$f" 2>/dev/null && ok || bad "itest rtt: no netem_rtt line"
eq "itest rtt tc calls" "$(grep -c 'exec -T node[1-4] tc qdisc replace dev eth0 root netem delay 10ms limit 100000$' "$SB/state/calls")" 4
eq "itest rtt row" "$(tail -n 1 "$SB_RAW/it_rtt.csv" | cut -d, -f10,11)" "20,none"
eq "itest rtt lock released" "$(ls -d "$SB_RAW/.hbdb.lock" 2>/dev/null)" ""

# B: RTT + F1 on two nodes with a warmup: injected at marker + WARMUP + AT,
# the stopped nodes do not fail the run, the fault column is CSV-safe.
itest f1 EXP=it_f1 RTT_MS=20 FAULT=F1:node3,node4@1 WARMUP=1s DURATION=2s STUB_BENCH_SECS=3
eq "itest f1 rc" "$it_rc" 0
f="$(raw_tag it_f1)"
grep -q ',netem_rtt,' "$f" 2>/dev/null && ok || bad "itest f1: netem_rtt line lost"
line="$(grep ',docker_stop,node3+node4,' "$f" 2>/dev/null)"
t_s="$(echo "$line" | cut -d, -f3)"
tl_s="$(echo "$line" | cut -d, -f4)"
awk -v t="$t_s" -v l="$tl_s" 'BEGIN { exit !(t >= 1.0 && t < 1.6 && l >= 2.0 && l < 2.6) }' && ok ||
  bad "itest f1 offsets t_s=$t_s timeline_s=$tl_s (want ~1 / ~2): $(cat "$f" 2>/dev/null)"
grep -q '^docker stop c3 c4$' "$SB/state/calls" && ok || bad "itest f1 docker stop: $(grep stop "$SB/state/calls")"
contains "itest f1 exemption logged" "$it_err" "container node3 is exited (exit code 0): stopped on purpose by FAULT=F1:node3,node4@1"
row="$(tail -n 1 "$SB_RAW/it_f1.csv")"
eq "itest f1 row fields" "$(echo "$row" | awk -F, '{ print NF }')" 22
eq "itest f1 fault column" "$(echo "$row" | cut -d, -f11)" "F1:node3+node4@1"
eq "itest f1 temp files removed" "$(ls -a "$SB_RAW/it_f1" | grep -c -e '\.stopped$' -e '\.fault\.stop$')" 0

# C: docker stop fails -> exit 3, no row.
itest stopfail EXP=it_stopfail FAULT=F1:node4@0 DURATION=2s STUB_BENCH_SECS=1 STUB_STOP_RC=1
eq "itest stop failure rc" "$it_rc" 3
contains "itest stop failure msg" "$it_err" "fault injection F1:node4@0 failed"
[ -e "$SB_RAW/it_stopfail.csv" ] && bad "itest stop failure wrote a CSV row" || ok

# D: the benchmark ends before AT -> never injected -> exit 3, no row.
itest early EXP=it_early FAULT=F1:node4@2 DURATION=3s STUB_BENCH_SECS=0.3
eq "itest early end rc" "$it_rc" 3
contains "itest early end msg" "$it_err" "was never injected"
grep -q '^docker stop' "$SB/state/calls" && bad "itest early end: a container was stopped" || ok
[ -e "$SB_RAW/it_early.csv" ] && bad "itest early end wrote a CSV row" || ok

# E: an unexpected death next to an F1 stop still fails the run.
itest dead EXP=it_dead FAULT=F1:node4@0 DURATION=2s STUB_BENCH_SECS=1 STUB_DEAD=c2
eq "itest unexpected death rc" "$it_rc" 3
contains "itest unexpected death msg" "$it_err" "node2(status=exited,oom_killed=false,exit_code=137)"

# F: F3 through run_one: the slow node gets slow + base delay.
itest f3 EXP=it_f3 RTT_MS=60 FAULT=F3:node1@0 DURATION=2s STUB_BENCH_SECS=1
eq "itest f3 rc" "$it_rc" 0
grep -q 'exec -T node1 tc qdisc replace dev eth0 root netem delay 230ms limit 100000$' "$SB/state/calls" && ok || bad "itest f3 tc"
f="$(raw_tag it_f3)"
eq "itest f3 log actions" "$(cut -d, -f6 "$f" 2>/dev/null | tr '\n' ' ')" "action netem_rtt run_phase_start netem_delay netem_applied "

# The recorded fault value must be CSV/--tags safe and the tag file-name safe.
grep -q 'fault_rec="$(echo "$FAULT" | tr '"','"' '"'+'"')"' "$HERE/run_one.sh" && ok || bad "run_one fault_rec"
grep -q 'fault=$fault_rec,' "$HERE/run_one.sh" && ok || bad "run_one tags use fault_rec"

echo "test_faults.sh: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
