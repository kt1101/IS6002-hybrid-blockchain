# shellcheck shell=bash
# Container- and network-level fault injection (spec section 4, RQ3).
# Source it after lib.sh, do not execute it:
#   . "$HBDB_ROOT/experiments/faults.sh"      (also sources net.sh)
# Bash 3.2 compatible (runs on the macOS host).
#
# FAULT strings (run_one.sh FAULT=...):
#   none                 no fault
#   F1:node3,node4@60    crash: `docker stop` the nodes at second 60
#   F2:node4@60          silent (raft, tendermint, bft2): the node stays up
#                        but its consensus traffic is dropped by iptables in
#                        its container (fault_silent_net). kafka-orig and
#                        kafka: needs Task 15's --fault flag (FaultPolicy);
#                        tm-orig: no silent mode.
#   F3:node1@60          slow: netem delay FAULT_SLOW_MS (200) ms on the
#                        node's egress, on top of the RTT_MS/2 base delay
#   F4, F5               wrong state / f+1 faulty nodes: need Task 15's
#                        --fault flag (rejected for now)
# Nodes are node1..node$N, separated by ',' or '+' (run_one.sh records the
# string with '+' so it fits a CSV field and a ycsbbench --tags value). AT is
# a whole number of seconds, 0 <= AT < DURATION. Node choice convention: the
# last nodes (fault_default_nodes), so node1 (the raft bootstrap node) is not
# hit unless the scenario names it (F3 "leader slow" = F3:node1@AT).
#
# Time base: t=0 is the start of the MEASUREMENT window = the moment
# ycsbbench prints "ycsbbench: run phase started" (seen in its log) + WARMUP.
# AT counts from there. The timeline file (ycsbbench --timeline) starts at the
# run phase, i.e. at the start of the warmup, so the fault falls into
# timeline line index WARMUP+AT (0-based); the log has both offsets.
#
# Injection log (FAULT_LOG, run_one.sh: results/raw/<EXP>/<tag>.faults), CSV:
#   utc,epoch_ms,t_s,timeline_s,fault,action,node,detail
# t_s = seconds since t=0 (negative before it), timeline_s = t_s + WARMUP;
# both empty for lines written before the run phase (the RTT netem).
#
# Globals the caller sets (run_one.sh):
#   FAULT_LOG        injection log file ("" = do not log)
#   FAULT_STOPPED    file: one service name per line for every node F1 stopped
#                    on purpose (run_one.sh's container check skips them)
#   FAULT_STOP       file whose existence aborts a pending wait ("" = none)
#   FAULT_PARENT_PID a pending wait aborts once this pid is gone ("" = no
#                    check), so an injector orphaned by a SIGKILLed
#                    run_one.sh can never act on a later run's cluster
#   FAULT_WARMUP_MS  warmup in ms
#   FAULT_T0_MS      epoch ms of t=0 (set by fault_wait_marker; when empty a
#                    fault_* call counts AT from the moment it is called)
#   FAULT_SLOW_MS    F3 delay in ms (default 200)
# fault_parse sets FAULT_SPEC, FAULT_KIND (none|F1|F2|F3), FAULT_NODES
# (space-separated), FAULT_AT, FAULT_BACKEND.

if ! command -v net_delay >/dev/null 2>&1; then
  . "$HBDB_ROOT/experiments/net.sh"
fi

FAULT_LOG="${FAULT_LOG:-}"
FAULT_STOPPED="${FAULT_STOPPED:-}"
FAULT_STOP="${FAULT_STOP:-}"
FAULT_PARENT_PID="${FAULT_PARENT_PID:-}"
FAULT_WARMUP_MS="${FAULT_WARMUP_MS:-0}"
FAULT_T0_MS="${FAULT_T0_MS:-}"
FAULT_SLOW_MS="${FAULT_SLOW_MS:-200}"
FAULT_SPEC=none
FAULT_KIND=none
FAULT_NODES=""
FAULT_AT=""
FAULT_BACKEND=""
FAULT_RUN_MARKER='ycsbbench: run phase started'
FAULT_LOG_HEADER="utc,epoch_ms,t_s,timeline_s,fault,action,node,detail"

# fault_default_nodes N COUNT: the COUNT nodes a fault hits by default, the
# last ones first, comma-separated: N=4 COUNT=1 -> node4; N=7 COUNT=2 ->
# node7,node6. COUNT must be 1..N-1 (node1 is never a default fault node).
fault_default_nodes() {
  local n="$1" count="$2" i out=""
  hbdb_require_uint N "$n"
  hbdb_require_uint COUNT "$count"
  [ "$count" -ge 1 ] || die "fault_default_nodes: COUNT must be >= 1"
  [ "$count" -lt "$n" ] || die "fault_default_nodes: COUNT=$count must be < N=$n (node1 is never a default fault node)"
  i="$n"
  while [ "$i" -gt $((n - count)) ]; do
    out="${out:+$out,}node$i"
    i=$((i - 1))
  done
  echo "$out"
}

# fault_parse SPEC BACKEND N DURATION_S: validate SPEC (see the header) for
# this backend and cluster size and set FAULT_*; die on anything invalid.
fault_parse() {
  local spec="$1" backend="$2" n="$3" dur="$4" rest nodes_part at node num seen="" count=0 ports
  FAULT_SPEC="$spec"
  FAULT_BACKEND="$backend"
  FAULT_KIND=none
  FAULT_NODES=""
  FAULT_AT=""
  [ "$spec" = none ] && return 0
  hbdb_require_uint N "$n"
  hbdb_require_uint DURATION_S "$dur"
  echo "$spec" | grep -Eq '^F[0-9]+:node[1-9][0-9]*([,+]node[1-9][0-9]*)*@[0-9]+$' ||
    die "FAULT='$spec': want none or F<k>:node<i>[,node<j>...]@<seconds>, e.g. F1:node4@60, F1:node3,node4@60, F3:node1@60"
  FAULT_KIND="${spec%%:*}"
  rest="${spec#*:}"
  nodes_part="${rest%@*}"
  at="${rest##*@}"
  case "$FAULT_KIND" in
    F1 | F2 | F3) ;;
    F4) die "FAULT='$spec': F4 (wrong state) needs Task 15's --fault flag (FaultPolicy.OnApply); not implemented yet" ;;
    F5) die "FAULT='$spec': F5 (f+1 nodes of F2/F4, BFT backends only) needs Task 15's --fault flag; not implemented yet" ;;
    *) die "FAULT='$spec': unknown fault kind $FAULT_KIND (F1 crash, F2 silent, F3 slow, F4 wrong state, F5 f+1 faulty)" ;;
  esac
  for node in $(echo "$nodes_part" | tr ',+' '  '); do
    num="${node#node}"
    [ "$num" -le "$n" ] || die "FAULT='$spec': $node does not exist (N=$n: node1..node$n)"
    hbdb_in_list "$node" "$seen" && die "FAULT='$spec': $node listed twice"
    seen="$seen $node"
    count=$((count + 1))
  done
  FAULT_NODES="${seen# }"
  at=$((10#$at))
  [ "$at" -lt "$dur" ] || die "FAULT='$spec': AT=$at must be < DURATION (${dur}s): AT counts from the start of the measurement window"
  FAULT_AT="$at"
  case "$FAULT_KIND" in
    F1)
      [ "$count" -lt "$n" ] || die "FAULT='$spec': F1 may not stop every node (N=$n)"
      ;;
    F2)
      case "$backend" in
        kafka-orig | kafka)
          die "FAULT='$spec': F2 (silent) for BACKEND=$backend needs Task 15's --fault flag (FaultPolicy: the node withholds its approve); not implemented yet" ;;
      esac
      ports="$(hbdb_consensus_ports "$backend")"
      [ -n "$ports" ] ||
        die "FAULT='$spec': BACKEND=$backend has no network-silent mode (hbdb_consensus_ports lists no consensus ports for it)"
      [ "$count" -lt "$n" ] || die "FAULT='$spec': F2 may not silence every node (N=$n)"
      ;;
    F3)
      hbdb_require_uint FAULT_SLOW_MS "$FAULT_SLOW_MS"
      [ "$FAULT_SLOW_MS" -ge 1 ] || die "FAULT_SLOW_MS must be >= 1"
      ;;
  esac
  return 0
}

# fault_log ACTION NODE DETAIL: append one line to FAULT_LOG (see header).
fault_log() {
  local now t_s="" tl_s="" utc
  [ -n "$FAULT_LOG" ] || return 0
  now="$(hbdb_epoch_ms)"
  utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [ -n "$FAULT_T0_MS" ]; then
    t_s="$(awk -v a="$now" -v b="$FAULT_T0_MS" 'BEGIN { printf "%.3f", (a - b) / 1000 }')"
    tl_s="$(awk -v a="$now" -v b="$FAULT_T0_MS" -v w="$FAULT_WARMUP_MS" 'BEGIN { printf "%.3f", (a - b + w) / 1000 }')"
  fi
  [ -s "$FAULT_LOG" ] || echo "$FAULT_LOG_HEADER" >>"$FAULT_LOG"
  echo "$utc,$now,$t_s,$tl_s,$(echo "$FAULT_SPEC" | tr ',' '+'),$1,$2,$(echo "$3" | tr ',' ';')" >>"$FAULT_LOG"
}

# fault_should_abort: true when FAULT_STOP exists or FAULT_PARENT_PID is gone.
fault_should_abort() {
  if [ -n "$FAULT_STOP" ] && [ -e "$FAULT_STOP" ]; then
    return 0
  fi
  if [ -n "$FAULT_PARENT_PID" ] && ! kill -0 "$FAULT_PARENT_PID" 2>/dev/null; then
    log "fault injector: parent pid $FAULT_PARENT_PID is gone, aborting"
    return 0
  fi
  return 1
}

# fault_wait_until AT: return 0 once FAULT_T0_MS + AT s is reached, 1 if
# fault_should_abort first. Without FAULT_T0_MS, t=0 is now.
fault_wait_until() {
  local target now d
  hbdb_require_uint AT "$1"
  [ -n "$FAULT_T0_MS" ] || FAULT_T0_MS="$(hbdb_epoch_ms)"
  target=$((FAULT_T0_MS + 10#$1 * 1000))
  while :; do
    if fault_should_abort; then
      return 1
    fi
    now="$(hbdb_epoch_ms)"
    [ "$now" -lt "$target" ] || return 0
    d=$((target - now))
    [ "$d" -le 500 ] || d=500
    sleep "$(printf '%d.%03d' $((d / 1000)) $((d % 1000)))"
  done
}

# fault_wait_marker BENCH_LOG: wait until ycsbbench logged the start of its
# run phase, then set FAULT_T0_MS = that moment + FAULT_WARMUP_MS. Return 1
# if fault_should_abort first.
fault_wait_marker() {
  local run0
  while ! grep -q "$FAULT_RUN_MARKER" "$1" 2>/dev/null; do
    if fault_should_abort; then
      return 1
    fi
    sleep 0.1
  done
  run0="$(hbdb_epoch_ms)"
  FAULT_T0_MS=$((run0 + 10#$FAULT_WARMUP_MS))
  fault_log run_phase_start "" "ycsbbench run phase (timeline line 0); measurement t=0 is $FAULT_WARMUP_MS ms later"
}

# fault_crash NODES AT (F1): at second AT `docker stop` every node of NODES
# (comma- or space-separated) in one call (docker stops them in parallel).
# The container ids are looked up BEFORE the wait (each `docker compose ps`
# takes a few hundred ms), so the stop is issued right at AT. Each node is
# added to FAULT_STOPPED before the stop. Return 0 ok, 1 error, 2 aborted
# (fault_should_abort).
fault_crash() {
  local nodes node id ids="" names=""
  nodes="$(echo "$1" | tr ',+' '  ')"
  [ -n "$nodes" ] || die "fault_crash: no nodes"
  for node in $nodes; do
    id="$(hbdb_compose ps -q "$node" 2>/dev/null)"
    if [ -z "$id" ]; then
      log "fault_crash: no running container for $node"
      fault_log docker_stop_failed "$node" "no running container"
      return 1
    fi
    ids="$ids $id"
    names="$names $node=$id"
  done
  fault_wait_until "$2" || return 2
  for node in $nodes; do
    [ -z "$FAULT_STOPPED" ] || echo "$node" >>"$FAULT_STOPPED"
  done
  fault_log docker_stop "$(echo $nodes | tr ' ' '+')" "containers$names"
  log "fault F1: docker stop$(printf ' %s' $nodes)"
  # shellcheck disable=SC2086
  if ! docker stop $ids >/dev/null; then
    fault_log docker_stop_failed "$(echo $nodes | tr ' ' '+')" "docker stop failed"
    return 1
  fi
  fault_log stopped "$(echo $nodes | tr ' ' '+')" "docker stop returned"
  return 0
}

# fault_slow NODE AT MS (F3): at second AT give NODE an egress delay of MS ms
# on top of the base RTT delay (NET_BASE_DELAY_MS). Return as fault_crash.
fault_slow() {
  local node="$1" ms="$3" total
  hbdb_require_uint MS "$ms"
  fault_wait_until "$2" || return 2
  total="$(net_add_ms "$NET_BASE_DELAY_MS" "$ms")"
  fault_log netem_delay "$node" "delay ${total}ms = slow ${ms}ms + base ${NET_BASE_DELAY_MS}ms on $NET_DEV"
  log "fault F3: netem delay ${total}ms on $node"
  if ! net_delay "$node" "$total"; then
    fault_log netem_delay_failed "$node" "tc failed"
    return 1
  fi
  fault_log netem_applied "$node" "tc returned"
  return 0
}

# fault_silent_net_script PORTS: the shell script run in the node container.
# For every port it drops the node's outgoing TCP to that port (connections
# it opens to peers) and from that port (replies on connections peers opened
# to it); -C first, so a second call adds nothing. iptables-legacy is the
# fallback when the nft backend is unusable in the kernel.
fault_silent_net_script() {
  local p
  printf '%s\n' 'set -e' 'ipt=iptables' 'iptables -w -L OUTPUT -n >/dev/null 2>&1 || ipt=iptables-legacy'
  for p in $1; do
    printf '%s\n' "for d in --dport --sport; do \$ipt -w -C OUTPUT -p tcp \$d $p -j DROP 2>/dev/null || \$ipt -w -A OUTPUT -p tcp \$d $p -j DROP; done"
  done
  printf '%s\n' '$ipt -w -S OUTPUT'
}

# fault_silent_net NODE AT PORTS (F2, network level): at second AT drop
# NODE's consensus traffic on PORTS (space-separated, from
# hbdb_consensus_ports). Dies when PORTS is empty. Return as fault_crash.
fault_silent_net() {
  local node="$1" ports="$3" p
  [ -n "$(echo $ports)" ] ||
    die "fault_silent_net: no consensus ports for ${FAULT_BACKEND:-this backend} (hbdb_consensus_ports in lib.sh)"
  for p in $ports; do
    hbdb_require_uint PORT "$p"
  done
  fault_wait_until "$2" || return 2
  fault_log iptables_drop "$node" "tcp OUTPUT --dport/--sport $(echo $ports)"
  log "fault F2: dropping consensus ports $(echo $ports) on $node"
  if ! hbdb_compose exec -T "$node" sh -c "$(fault_silent_net_script "$ports")" >/dev/null; then
    fault_log iptables_drop_failed "$node" "iptables failed"
    return 1
  fi
  fault_log iptables_applied "$node" "iptables returned"
  return 0
}

# fault_inject: run the parsed fault (FAULT_KIND / FAULT_NODES / FAULT_AT).
fault_inject() {
  local node rc
  case "$FAULT_KIND" in
    none) return 0 ;;
    F1) fault_crash "$FAULT_NODES" "$FAULT_AT" ;;
    F2)
      for node in $FAULT_NODES; do
        fault_silent_net "$node" "$FAULT_AT" "$(hbdb_consensus_ports "$FAULT_BACKEND")" || return $?
      done
      ;;
    F3)
      for node in $FAULT_NODES; do
        fault_slow "$node" "$FAULT_AT" "$FAULT_SLOW_MS" || return $?
      done
      ;;
    *) die "fault_inject: unsupported FAULT_KIND '$FAULT_KIND'" ;;
  esac
}

# fault_run BENCH_LOG: wait for the run phase, then inject at FAULT_AT.
# Return 0 injected, 1 injection failed, 2 aborted (FAULT_STOP) before it.
fault_run() {
  fault_wait_marker "$1" || return 2
  fault_inject
}
