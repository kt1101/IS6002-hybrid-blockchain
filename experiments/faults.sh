# shellcheck shell=bash
# Container- and network-level fault injection (spec section 4, RQ3).
# Source it after lib.sh, do not execute it:
#   . "$HBDB_ROOT/experiments/faults.sh"      (also sources net.sh)
# Bash 3.2 compatible (runs on the macOS host).
#
# FAULT strings (run_one.sh FAULT=...):
#   none                 no fault
#   F1:node3,node4@60    crash: `docker stop` the nodes at second 60
#   F2:node4@60          silent: the node stays up but sends no consensus
#                        messages. raft, tendermint, bft2: its consensus
#                        traffic is dropped by iptables in its container
#                        (fault_silent_net). kafka: veritas --fault=silent
#                        (FaultPolicy.OnVote: no approve/abort is sent),
#                        activated at AT by fault_signal. kafka-orig (the
#                        unmodified original server) and tm-orig: no silent
#                        mode.
#   F3:node1@60          slow: netem delay FAULT_SLOW_MS (200) ms on the
#                        node's egress, on top of the RTT_MS/2 base delay
#   F4:node4@60          wrong state (kafka, raft, tendermint, bft2): veritas
#                        --fault=corrupt (FaultPolicy.OnApply appends
#                        -CORRUPT to every value the node applies, so its
#                        root digest and its Gets go wrong), activated at AT
#                        by fault_signal; at most f = floor((N-1)/3) nodes
#                        (N=4: 1, N=7: 2): f+1 corrupt nodes is F5-F4
#   F5-F2:node3,node4@60 beyond the threshold (BFT backends only: tendermint,
#   F5-F4:node3,node4@60 bft2): F2 (network) resp. F4 (corrupt) on exactly
#                        f+1 nodes, f = floor((N-1)/3) (N=4: 2 nodes, N=7: 3);
#                        F5-F4 also on kafka/raft when ATTEST_EVERY > 0 (phase
#                        3: it tests the attestation quorum)
#   F6:node4@60          read-path lie (phase 3; kafka, raft, tendermint,
#                        bft2): veritas --fault=readlie (ReadFault.OnRead
#                        appends -CORRUPT to every value the node returns to a
#                        Get/GetVerified; its state, votes, root and proofs stay
#                        honest, so the auditor and the certifier see nothing),
#                        activated at AT by fault_signal; at most f nodes
#   F6E:node4@60         as F6 with --fault=readlie-evasive: honest on reads
#                        the client asked to be signed
#
# FaultPolicy faults (F2 on kafka, F4, F5-F4, F6, F6E) are baked into the node's
# command line when the cluster is created: run_one.sh passes
# fault_node_faults (NODE_FAULTS, e.g. "node4=corrupt") to up.sh, and
# gen_compose.py starts those nodes' veritas with --fault=<kind> and a
# --fault-after that never fires (87600h). At AT fault_signal sends SIGUSR1 to
# the node's veritas (pid in HBDB_VERITAS_PIDFILE), which activates the fault
# at once; so the activation is tied to the measurement window like every
# other fault, independent of how long load and settle took. veritas then
# writes the policy to HBDB_FAULT_ACTIVE_FILE; fault_signal polls that file
# for up to FAULT_CONFIRM_MS and logs POLICY_confirmed, or
# POLICY_unconfirmed and fails (run_one.sh: exit 3, no CSV row). Several
# nodes are signalled in parallel.
# Kafka F2 withholds the node's votes only: its own proposals still count as
# its approve, so its own blocks keep committing (about 1/N of the
# throughput remains); the network F2 of raft/tendermint/bft2 silences the
# node completely.
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
# fault_parse sets FAULT_SPEC, FAULT_KIND (none|F1|F2|F3|F4|F5|F6|F6E), FAULT_SUB
# (F5: F2 or F4, else empty), FAULT_NODES (space-separated), FAULT_AT,
# FAULT_BACKEND and FAULT_POLICY (silent|corrupt|readlie|readlie-evasive when the fault is a veritas
# --fault activated by SIGUSR1, else empty).

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
FAULT_CONFIRM_MS="${FAULT_CONFIRM_MS:-5000}"
FAULT_SPEC=none
FAULT_KIND=none
FAULT_SUB=""
FAULT_POLICY=""
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
  local spec="$1" backend="$2" n="$3" dur="$4" rest nodes_part at node num seen="" count=0 ports f net_silent=0
  FAULT_SPEC="$spec"
  FAULT_BACKEND="$backend"
  FAULT_KIND=none
  FAULT_SUB=""
  FAULT_POLICY=""
  FAULT_NODES=""
  FAULT_AT=""
  [ "$spec" = none ] && return 0
  hbdb_require_uint N "$n"
  hbdb_require_uint DURATION_S "$dur"
  echo "$spec" | grep -Eq '^(F[0-9]+(-F[0-9]+)?|F6E):node[1-9][0-9]*([,+]node[1-9][0-9]*)*@[0-9]+$' ||
    die "FAULT='$spec': want none or F<k>:node<i>[,node<j>...]@<seconds>, e.g. F1:node4@60, F1:node3,node4@60, F3:node1@60, F5-F4:node3,node4@60"
  FAULT_KIND="${spec%%:*}"
  rest="${spec#*:}"
  nodes_part="${rest%@*}"
  at="${rest##*@}"
  case "$FAULT_KIND" in
    *-*)
      FAULT_SUB="${FAULT_KIND#*-}"
      FAULT_KIND="${FAULT_KIND%%-*}"
      [ "$FAULT_KIND" = F5 ] ||
        die "FAULT='$spec': only F5 takes a sub-kind (F5-F2, F5-F4), got $FAULT_KIND-$FAULT_SUB"
      case "$FAULT_SUB" in
        F2 | F4) ;;
        *) die "FAULT='$spec': F5 combines F2 or F4 (F5-F2, F5-F4), got F5-$FAULT_SUB" ;;
      esac
      ;;
  esac
  case "$FAULT_KIND" in
    F1 | F2 | F3 | F4 | F6 | F6E) ;;
    F5)
      [ -n "$FAULT_SUB" ] ||
        die "FAULT='$spec': F5 needs its kind: F5-F2 (f+1 silent nodes) or F5-F4 (f+1 corrupt nodes), e.g. F5-F4:node3,node4@60"
      ;;
    *) die "FAULT='$spec': unknown fault kind $FAULT_KIND (F1 crash, F2 silent, F3 slow, F4 wrong state, F5-F2/F5-F4 f+1 faulty, F6/F6E read-path lie)" ;;
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
        kafka) FAULT_POLICY=silent ;;
        kafka-orig)
          die "FAULT='$spec': BACKEND=kafka-orig is the unmodified original server, it has no silent mode (no FaultPolicy); use BACKEND=kafka" ;;
        *) net_silent=1 ;;
      esac
      [ "$count" -lt "$n" ] || die "FAULT='$spec': F2 may not silence every node (N=$n)"
      ;;
    F4)
      hbdb_backend_has_fault_policy "$backend" ||
        die "FAULT='$spec': F4 (wrong state) needs a veritas-core backend (kafka, raft, tendermint, bft2; veritas --fault=corrupt); BACKEND=$backend is an original server without FaultPolicy"
      f=$(((n - 1) / 3))
      [ "$count" -le "$f" ] ||
        die "FAULT='$spec': F4 corrupts at most f = floor((N-1)/3) = $f nodes (N=$n), got $count; f+1 corrupt nodes is F5-F4 (tendermint, bft2)"
      FAULT_POLICY=corrupt
      ;;
    F6 | F6E)
      hbdb_backend_has_fault_policy "$backend" ||
        die "FAULT='$spec': F6 (read-path lie) needs a veritas-core backend (kafka, raft, tendermint, bft2; veritas --fault=readlie); BACKEND=$backend is an original server without FaultPolicy"
      f=$(((n - 1) / 3))
      [ "$count" -le "$f" ] ||
        die "FAULT='$spec': $FAULT_KIND lies on reads on at most f = floor((N-1)/3) = $f nodes (N=$n), got $count"
      if [ "$FAULT_KIND" = F6 ]; then FAULT_POLICY=readlie; else FAULT_POLICY=readlie-evasive; fi
      ;;
    F5)
      # Phase 3 (E12): with attestations on (ATTEST_EVERY > 0), F5-F4 also
      # runs on the CFT core backends: it tests the attestation quorum, not
      # the consensus protocol.
      case "$backend" in
        tendermint | bft2) ;;
        kafka | raft)
          [ "$FAULT_SUB" = F4 ] && [ "${ATTEST_EVERY:-0}" != 0 ] ||
            die "FAULT='$spec': F5 (f+1 faulty nodes) is for the BFT backends only (tendermint, bft2), got BACKEND=$backend (F5-F4 on kafka/raft needs ATTEST_EVERY > 0)" ;;
        *) die "FAULT='$spec': F5 (f+1 faulty nodes) is for the BFT backends only (tendermint, bft2), got BACKEND=$backend" ;;
      esac
      [ "$n" -ge 4 ] || die "FAULT='$spec': F5 needs N >= 4 (f = floor((N-1)/3) >= 1), got N=$n"
      f=$(((n - 1) / 3))
      [ "$count" -eq $((f + 1)) ] ||
        die "FAULT='$spec': F5 hits exactly f+1 = $((f + 1)) nodes (N=$n, f=$f), got $count; e.g. FAULT=F5-$FAULT_SUB:$(fault_default_nodes "$n" $((f + 1)))@$at"
      case "$FAULT_SUB" in
        F2) net_silent=1 ;;
        F4) FAULT_POLICY=corrupt ;;
      esac
      ;;
    F3)
      hbdb_require_uint FAULT_SLOW_MS "$FAULT_SLOW_MS"
      [ "$FAULT_SLOW_MS" -ge 1 ] || die "FAULT_SLOW_MS must be >= 1"
      ;;
  esac
  if [ "$net_silent" = 1 ]; then
    ports="$(hbdb_consensus_ports "$backend")"
    [ -n "$ports" ] ||
      die "FAULT='$spec': BACKEND=$backend has no network-silent mode (hbdb_consensus_ports lists no consensus ports for it)"
  fi
  return 0
}

# fault_node_faults: the veritas --fault of the parsed fault per node, for
# up.sh / gen_compose.py --node-fault: "node3=corrupt node4=corrupt"; empty
# when the fault is not a FaultPolicy fault (F1, F3, network F2/F5-F2).
fault_node_faults() {
  local node out=""
  [ -n "$FAULT_POLICY" ] || return 0
  for node in $FAULT_NODES; do
    out="${out:+$out }$node=$FAULT_POLICY"
  done
  echo "$out"
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

# fault_signal_script: the shell script run in the node container: SIGUSR1
# to the veritas server, whose pid gen_compose.py writes to
# HBDB_VERITAS_PIDFILE; fails when the file or the process is missing.
fault_signal_script() {
  printf '%s\n' "p=\"\$(cat $HBDB_VERITAS_PIDFILE)\" && [ -n \"\$p\" ] && kill -USR1 \"\$p\""
}

# fault_confirm_script: the shell script that prints the node's
# HBDB_FAULT_ACTIVE_FILE (empty when missing): veritas writes its --fault
# spec there once SIGUSR1 has activated the fault.
fault_confirm_script() {
  printf '%s\n' "cat $HBDB_FAULT_ACTIVE_FILE 2>/dev/null || true"
}

# fault_confirm NODE POLICY: return 0 once NODE's active file reads POLICY,
# 1 if it does not within FAULT_CONFIRM_MS (polled every 0.5 s).
fault_confirm() {
  local node="$1" policy="$2" deadline got
  hbdb_require_uint FAULT_CONFIRM_MS "$FAULT_CONFIRM_MS"
  deadline=$(($(hbdb_epoch_ms) + 10#$FAULT_CONFIRM_MS))
  while :; do
    got="$(hbdb_compose exec -T "$node" sh -c "$(fault_confirm_script)" 2>/dev/null | tr -d ' \r\n')"
    [ "$got" = "$policy" ] && return 0
    [ "$(hbdb_epoch_ms)" -lt "$deadline" ] || return 1
    sleep 0.5
  done
}

# fault_signal NODE AT POLICY (F2 on kafka, F4, F5-F4, F6, F6E): at second AT send
# SIGUSR1 to NODE's veritas server, which runs with --fault=POLICY (armed by
# gen_compose.py, see the header) and activates the fault on it, then
# confirm the activation (fault_confirm). Logged as POLICY_on (the injection
# row, just before the signal), POLICY_applied once kill returned, then
# POLICY_confirmed, or POLICY_unconfirmed (return 1). Return as fault_crash.
fault_signal() {
  local node="$1" policy="$3"
  case "$policy" in
    silent | corrupt | readlie | readlie-evasive) ;;
    *) die "fault_signal: unknown policy '$policy'" ;;
  esac
  fault_wait_until "$2" || return 2
  fault_log "${policy}_on" "$node" "SIGUSR1 to veritas (--fault=$policy)"
  log "fault $FAULT_KIND: activating --fault=$policy on $node (SIGUSR1)"
  if ! hbdb_compose exec -T "$node" sh -c "$(fault_signal_script)" >/dev/null; then
    fault_log "${policy}_on_failed" "$node" "kill -USR1 failed"
    return 1
  fi
  fault_log "${policy}_applied" "$node" "kill returned"
  if ! fault_confirm "$node" "$policy"; then
    fault_log "${policy}_unconfirmed" "$node" "$HBDB_FAULT_ACTIVE_FILE does not read $policy after ${FAULT_CONFIRM_MS}ms"
    log "fault $FAULT_KIND: --fault=$policy on $node NOT confirmed within ${FAULT_CONFIRM_MS}ms"
    return 1
  fi
  fault_log "${policy}_confirmed" "$node" "$HBDB_FAULT_ACTIVE_FILE reads $policy"
  return 0
}

# fault_signal_all NODES AT POLICY: fault_signal every node of NODES
# (space-separated) in parallel once AT is reached, so their activations are
# not a docker exec apart. Return 2 if aborted before AT, else 1 if any node
# failed, else 0.
fault_signal_all() {
  local node pid pids="" rc=0 r
  fault_wait_until "$2" || return 2
  # Every child appends to FAULT_LOG: write the header before they start.
  if [ -n "$FAULT_LOG" ] && [ ! -s "$FAULT_LOG" ]; then
    echo "$FAULT_LOG_HEADER" >>"$FAULT_LOG"
  fi
  for node in $1; do
    fault_signal "$node" "$2" "$3" &
    pids="$pids $!"
  done
  for pid in $pids; do
    if wait "$pid"; then r=0; else r=$?; fi
    [ "$r" -eq 0 ] || [ "$rc" -ne 0 ] || rc=1
  done
  return "$rc"
}

# fault_inject: run the parsed fault (FAULT_KIND / FAULT_NODES / FAULT_AT).
# FaultPolicy faults signal their nodes in parallel (fault_signal_all); the
# network F2/F5-F2 and F3 hit the nodes one after the other (one exec each, a
# few hundred ms apart), all at or right after AT.
fault_inject() {
  local node rc
  if [ -n "$FAULT_POLICY" ]; then
    fault_signal_all "$FAULT_NODES" "$FAULT_AT" "$FAULT_POLICY"
    return $?
  fi
  case "$FAULT_KIND" in
    none) return 0 ;;
    F1) fault_crash "$FAULT_NODES" "$FAULT_AT" ;;
    F2 | F5)
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
