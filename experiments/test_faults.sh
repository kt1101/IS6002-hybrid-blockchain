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
COMPOSE_FAIL_NODE=""
CONFIRM_OUT=corrupt
CONFIRM_AT=0
PAR_HANDSHAKE=""
hbdb_compose() {
  echo "compose $*" >>"$CALLS"
  echo "compose_$1 $FAKE_NOW" >>"$TIMES"
  case "$1" in
    ps) [ "$3" = node9 ] || echo "cid_$3" ;;
    exec)
      if [ "${4:-}" = sh ] && [ "${5:-}" = -c ]; then
        case "$6" in
          *"$HBDB_FAULT_ACTIVE_FILE"*)
            # fault_confirm: the active file reads CONFIRM_OUT from
            # CONFIRM_AT (fake epoch ms) on.
            echo "$3" >>"$TMP_ROOT/confirms"
            [ "$FAKE_NOW" -lt "$CONFIRM_AT" ] || echo "$CONFIRM_OUT"
            return "$COMPOSE_EXEC_RC"
            ;;
          *"kill -USR1"*)
            printf '%s\n' "$6" >"$TMP_ROOT/signal_script"
            [ "$3" != "$COMPOSE_FAIL_NODE" ] || return 1
            if [ -n "$PAR_HANDSHAKE" ]; then
              # node4's signal unblocks node3's: only works in parallel.
              if [ "$3" = node4 ]; then
                : >"$TMP_ROOT/sig_node4"
              else
                k=0
                while [ ! -e "$TMP_ROOT/sig_node4" ] && [ "$k" -lt 50 ]; do
                  /bin/sleep 0.1
                  k=$((k + 1))
                done
                [ -e "$TMP_ROOT/sig_node4" ] || return 1
              fi
            fi
            return "$COMPOSE_EXEC_RC"
            ;;
        esac
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
  COMPOSE_FAIL_NODE=""
  CONFIRM_OUT=corrupt
  CONFIRM_AT=0
  PAR_HANDSHAKE=""
  FAULT_CONFIRM_MS=5000
  FAULT_PARENT_PID=""
  rm -f "$FAULT_LOG" "$FAULT_STOPPED" "$FAULT_STOP" "$TMP_ROOT/last_script" "$TMP_ROOT/signal_script" \
    "$TMP_ROOT/confirms" "$TMP_ROOT/sig_node4"
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
eq "ports bft2" "$(hbdb_consensus_ports bft2)" "7200 7202"
for b in kafka-orig tm-orig kafka; do
  eq "ports $b" "$(hbdb_consensus_ports "$b")" ""
done
# The constants must match gen_compose.py.
gc="$HERE/cluster/gen_compose.py"
eq "RAFT_PORT matches gen_compose" "$(sed -n 's/^RAFT_PORT = \([0-9]*\).*/\1/p' "$gc")" "$HBDB_RAFT_PORT"
eq "RAFT_FORWARD_PORT matches gen_compose" "$(sed -n 's/^RAFT_FORWARD_PORT = \([0-9]*\).*/\1/p' "$gc")" "$HBDB_RAFT_FORWARD_PORT"
eq "TM_P2P_PORT matches gen_compose" "$(sed -n 's/^TM_P2P_PORT = \([0-9]*\).*/\1/p' "$gc")" "$HBDB_TM_P2P_PORT"
eq "BFT2_PORT matches gen_compose" "$(sed -n 's/^BFT2_PORT = \([0-9]*\).*/\1/p' "$gc")" "$HBDB_BFT2_PORT"
grep -q '"cap_add"\] = \["NET_ADMIN"\]' "$gc" && ok || bad "gen_compose.py no longer gives the nodes NET_ADMIN"
eq "fault active file matches cmd/veritas" "$(sed -n 's/^const defaultFaultActiveFile = "\(.*\)".*/\1/p' "$HERE/../cmd/veritas/main.go")" "$HBDB_FAULT_ACTIVE_FILE"
eq "VERITAS_PIDFILE matches gen_compose" "$(sed -n 's/^VERITAS_PIDFILE = "\(.*\)".*/\1/p' "$gc")" "$HBDB_VERITAS_PIDFILE"
for b in kafka raft tendermint bft2; do
  hbdb_backend_has_fault_policy "$b" && ok || bad "$b has a FaultPolicy"
done
for b in kafka-orig tm-orig; do
  hbdb_backend_has_fault_policy "$b" && bad "$b has no FaultPolicy" || ok
done

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
fault_parse F2:node4@60 bft2 4 180
eq "F2 bft2" "$FAULT_KIND/$FAULT_NODES/$FAULT_BACKEND" "F2/node4/bft2"
fault_parse "F1:$(fault_default_nodes 7 2)@60" raft 7 180
eq "default nodes round-trip" "$FAULT_NODES" "node7 node6"
eq "F1 no policy" "$FAULT_POLICY/$(fault_node_faults)" "/"
fault_parse F2:node4@60 raft 4 180
eq "F2 raft is network-level" "$FAULT_POLICY/$(fault_node_faults)" "/"
# FaultPolicy faults (Task 15).
fault_parse F2:node4@20 kafka 4 40
eq "F2 kafka is --fault=silent" "$FAULT_KIND/$FAULT_POLICY/$FAULT_NODES" "F2/silent/node4"
eq "F2 kafka node faults" "$(fault_node_faults)" "node4=silent"
for b in kafka raft tendermint bft2; do
  fault_parse F4:node4@20 "$b" 4 40
  eq "F4 $b is --fault=corrupt" "$FAULT_KIND/$FAULT_SUB/$FAULT_POLICY/$(fault_node_faults)" "F4//corrupt/node4=corrupt"
done
fault_parse F4:node6,node7@20 raft 7 40
eq "F4 two nodes at N=7 (f=2)" "$(fault_node_faults)" "node6=corrupt node7=corrupt"
fault_parse F5-F4:node3,node4@20 tendermint 4 40
eq "F5-F4 tendermint" "$FAULT_KIND/$FAULT_SUB/$FAULT_POLICY/$(fault_node_faults)" "F5/F4/corrupt/node3=corrupt node4=corrupt"
fault_parse F5-F2:node4+node3@20 tendermint 4 40
eq "F5-F2 tendermint is network-level" "$FAULT_KIND/$FAULT_SUB/$FAULT_POLICY/$FAULT_NODES" "F5/F2//node4 node3"
fault_parse F5-F4:node5,node6,node7@60 tendermint 7 180
eq "F5-F4 N=7 hits 3 nodes" "$(fault_node_faults)" "node5=corrupt node6=corrupt node7=corrupt"
fault_parse F5-F4:node3,node4@20 bft2 4 40
eq "F5-F4 bft2" "$FAULT_KIND/$FAULT_SUB/$FAULT_POLICY/$(fault_node_faults)" "F5/F4/corrupt/node3=corrupt node4=corrupt"
fault_parse F5-F2:node3,node4@20 bft2 4 40
eq "F5-F2 bft2 is network-level" "$FAULT_KIND/$FAULT_SUB/$FAULT_POLICY/$(fault_node_faults)" "F5/F2//"
fault_parse F5-F4:node5,node6,node7@60 bft2 7 180
eq "F5-F4 bft2 N=7" "$(fault_node_faults)" "node5=corrupt node6=corrupt node7=corrupt"
# F6 read-path faults (phase 3): veritas --fault=readlie / readlie-evasive.
for b in kafka raft tendermint bft2; do
  fault_parse F6:node4@20 "$b" 4 40
  eq "F6 $b is --fault=readlie" "$FAULT_KIND/$FAULT_SUB/$FAULT_POLICY/$(fault_node_faults)" "F6//readlie/node4=readlie"
done
fault_parse F6E:node4@20 raft 4 40
eq "F6E is --fault=readlie-evasive" "$FAULT_KIND/$FAULT_POLICY/$(fault_node_faults)" "F6E/readlie-evasive/node4=readlie-evasive"
fault_parse F6:node6,node7@20 bft2 7 40
eq "F6 two nodes at N=7 (f=2)" "$(fault_node_faults)" "node6=readlie node7=readlie"
# F5-F4 on the CFT core backends once attestations are on (phase 3, E12).
for b in kafka raft; do
  ATTEST_EVERY=1 fault_parse F5-F4:node3,node4@20 "$b" 4 40
  eq "F5-F4 $b with ATTEST_EVERY=1" "$FAULT_KIND/$FAULT_SUB/$FAULT_POLICY/$(fault_node_faults)" "F5/F4/corrupt/node3=corrupt node4=corrupt"
done
# ---- lib.sh: hbdb_attest_quorum (phase 3) ----
eq "attest quorum N=4 f+1" "$(hbdb_attest_quorum 4 f+1)" 2
eq "attest quorum N=4 2f+1" "$(hbdb_attest_quorum 4 2f+1)" 3
eq "attest quorum N=7 f+1" "$(hbdb_attest_quorum 7 f+1)" 3
eq "attest quorum N=7 2f+1" "$(hbdb_attest_quorum 7 2f+1)" 5
eq "attest quorum auto" "$(hbdb_attest_quorum 4 auto)" 0
( hbdb_attest_quorum 4 3f+1 ) >/dev/null 2>&1 && bad "attest quorum 3f+1 accepted" || ok
( hbdb_attest_quorum x f+1 ) >/dev/null 2>&1 && bad "attest quorum N=x accepted" || ok
fault_parse none kafka 4 40
eq "none resets the policy" "$FAULT_KIND/$FAULT_SUB/$FAULT_POLICY/$(fault_node_faults)" "none///"

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
r="$(parse_rc F2:node1,node2,node3,node4@20 kafka 4 40)"
contains "F2 kafka all nodes" "$r" "may not silence every node"
r="$(parse_rc F2:node4@20 kafka-orig 4 40)"
contains "F2 kafka-orig" "$r" "unmodified original server"
r="$(parse_rc F2:node4@20 tm-orig 4 40)"
contains "F2 tm-orig" "$r" "no network-silent mode"
for b in kafka-orig tm-orig; do
  r="$(parse_rc F4:node4@20 "$b" 4 40)"
  contains "F4 $b" "$r" "F4 (wrong state) needs a veritas-core backend"
done
r="$(parse_rc F4:node1,node2,node3,node4@20 raft 4 40)"
contains "F4 all nodes" "$r" "F4 corrupts at most f = floor((N-1)/3) = 1 nodes (N=4), got 4"
r="$(parse_rc F4:node3,node4@20 tendermint 4 40)"
contains "F4 f+1 nodes is F5-F4" "$r" "f+1 corrupt nodes is F5-F4"
r="$(parse_rc F4:node5,node6,node7@20 raft 7 40)"
contains "F4 3 nodes at N=7" "$r" "at most f = floor((N-1)/3) = 2 nodes (N=7), got 3"
r="$(parse_rc F4:node3@20 kafka 3 40)"
contains "F4 N=3 (f=0)" "$r" "at most f = floor((N-1)/3) = 0 nodes"
r="$(parse_rc F6:node3,node4@20 raft 4 40)"
contains "F6 f+1 nodes" "$r" "F6 lies on reads on at most f = floor((N-1)/3) = 1 nodes (N=4), got 2"
for b in kafka-orig tm-orig; do
  r="$(parse_rc F6:node4@20 "$b" 4 40)"
  contains "F6 $b" "$r" "F6 (read-path lie) needs a veritas-core backend"
done
r="$(parse_rc F6E:node3,node4@20 bft2 4 40)"
contains "F6E f+1 nodes" "$r" "at most f = floor((N-1)/3) = 1 nodes (N=4), got 2"
r="$(parse_rc F7:node4@20 raft 4 40)"
contains "unknown kind F7" "$r" "unknown fault kind F7"
r="$(parse_rc F5:node3,node4@20 tendermint 4 40)"
contains "F5 without kind" "$r" "F5 needs its kind: F5-F2"
for b in kafka raft kafka-orig tm-orig; do
  r="$(parse_rc F5-F4:node3,node4@20 "$b" 4 40)"
  contains "F5 $b (not BFT)" "$r" "BFT backends only"
done
for b in kafka raft; do
  r="$(ATTEST_EVERY=1 parse_rc F5-F2:node3,node4@20 "$b" 4 40)"
  contains "F5-F2 $b stays BFT-only with attestations" "$r" "BFT backends only"
done
r="$(ATTEST_EVERY=1 parse_rc F5-F4:node3,node4@20 kafka-orig 4 40)"
contains "F5-F4 kafka-orig with attestations" "$r" "BFT backends only"
r="$(parse_rc F5-F4:node4@20 tendermint 4 40)"
contains "F5 one node at N=4" "$r" "exactly f+1 = 2 nodes (N=4, f=1), got 1; e.g. FAULT=F5-F4:node4,node3@20"
r="$(parse_rc F5-F4:node5,node6@20 tendermint 7 40)"
contains "F5 two nodes at N=7" "$r" "exactly f+1 = 3 nodes"
r="$(parse_rc F5-F4:node1,node2@20 tendermint 3 40)"
contains "F5 N=3" "$r" "F5 needs N >= 4"
r="$(parse_rc F5-F4:node4@20 bft2 4 40)"
contains "F5 bft2 one node at N=4" "$r" "exactly f+1 = 2 nodes"
r="$(parse_rc F2:node4@20 bft2 4 40)"
eq "F2 bft2 parses (network, ports 7200 7202)" "${r%%|*}" 0
r="$(parse_rc F5-F3:node3,node4@20 tendermint 4 40)"
contains "F5-F3" "$r" "F5 combines F2 or F4"
r="$(parse_rc F4-F2:node4@20 tendermint 4 40)"
contains "sub-kind on F4" "$r" "only F5 takes a sub-kind"
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
eq "rtt no backend: no extra service" "$NET_RTT_EXTRA" ""
# I5: the broker and the TSO get the same RTT_MS/2 egress delay.
reset
N=4
BACKEND=raft net_set_rtt 20 2>/dev/null
eq "rtt raft: nodes + tso" "$(grep -c 'tc qdisc replace dev eth0 root netem delay 10ms' "$CALLS")" 5
grep -q "^compose exec -T tso tc qdisc replace dev eth0 root netem delay 10ms limit 100000$" "$CALLS" && ok || bad "rtt raft tso"
grep -q "exec -T kafka " "$CALLS" && bad "rtt raft: kafka delayed" || ok
eq "rtt raft extra" "$NET_RTT_EXTRA" "tso"
reset
N=4
BACKEND=kafka net_set_rtt 20 2>/dev/null
eq "rtt kafka: nodes + kafka + tso" "$(grep -c 'tc qdisc replace dev eth0 root netem delay 10ms' "$CALLS")" 6
grep -q "^compose exec -T kafka tc qdisc replace dev eth0 root netem delay 10ms limit 100000$" "$CALLS" && ok || bad "rtt kafka broker"
eq "rtt kafka extra" "$NET_RTT_EXTRA" "kafka tso"
eq "rtt services kafka-orig" "$(net_rtt_services kafka-orig)" "kafka tso"
for b in tendermint bft2 tm-orig; do
  eq "rtt services $b" "$(net_rtt_services "$b")" ""
done
grep -q 'install -y --no-install-recommends iproute2' "$HERE/docker/Dockerfile.kafka" && ok || bad "Dockerfile.kafka lacks iproute2 (tc)"
reset
N=4
net_set_rtt 0 2>/dev/null
contains "rtt 0 removes qdisc" "$(cat "$CALLS")" "tc qdisc del dev eth0 root"
reset
N=2
COMPOSE_EXEC_RC=1
(net_set_rtt 20) 2>/dev/null; rc=$?
eq "rtt tc failure" "$rc" 1

# ---- lib.sh: docker stats sidecar, breakdown JSON, net_bytes_per_block ----
eq "stats header" "$HBDB_STATS_HEADER" "epoch_ms,utc,name,cpu_pct,mem_used,mem_limit,net_rx,net_tx,mem_used_bytes,net_rx_bytes,net_tx_bytes"
r="$(printf '%s\n' 'hbdb-node1-1,12.34%,123.5MiB / 768MiB,1.5MB / 2.25kB' 'hbdb-kafka-1,0.00%,0B / 0B,0B / 0B' 'junk' |
  hbdb_stats_lines 1000 2026-10-31T00:00:00Z)"
eq "stats line parsed" "$(echo "$r" | sed -n 1p)" "1000,2026-10-31T00:00:00Z,hbdb-node1-1,12.34,123.5MiB,768MiB,1.5MB,2.25kB,129499136,1500000,2250"
eq "stats zero line" "$(echo "$r" | sed -n 2p)" "1000,2026-10-31T00:00:00Z,hbdb-kafka-1,0.00,0B,0B,0B,0B,0,0,0"
eq "stats junk line dropped" "$(echo "$r" | grep -c .)" 2
r="$(printf '%s\n' 'c,--,-- / --,-- / --' | hbdb_stats_lines 1 u)"
eq "stats unparsable sizes left empty" "$r" "1,u,c,--,--,--,--,--,,,"
eq "stats GiB/GB" "$(printf '%s\n' 'x,1%,1.5GiB / 2GiB,3GB / 1TB' | hbdb_stats_lines 1 u | cut -d, -f9-11)" "1610612736,3000000000,1000000000000"
JS="$TMP_ROOT/snap.json"
printf '{\n  "node": 1,\n  "batch_ns": 7,\n  "blocks": 1234,\n  "batch_blocks": 99,\n  "dropped_blocks": 3\n}\n' >"$JS"
eq "json blocks (not batch_blocks)" "$(hbdb_json_int "$JS" blocks)" 1234
eq "json dropped" "$(hbdb_json_int "$JS" dropped_blocks)" 3
eq "json missing key" "$(hbdb_json_int "$JS" nope)" ""
eq "json missing file" "$(hbdb_json_int "$TMP_ROOT/none.json" blocks)" ""
# N4: exact tx counters of the two snapshots / node1's blocks between them.
eq "net bytes per block" "$(hbdb_net_bytes_per_block 3500 15700 6 10)" 3050
eq "net bytes per block rounds" "$(hbdb_net_bytes_per_block 0 10 0 3)" 3
eq "net bytes per block: large counters" "$(hbdb_net_bytes_per_block 98765432100 98766432100 1000 1100)" 10000
hbdb_net_bytes_per_block 1000 9000 5 5 >/dev/null && bad "net bytes per block with 0 blocks" || ok
hbdb_net_bytes_per_block 1000 9000 "" 5 >/dev/null && bad "net bytes per block without a snapshot" || ok
hbdb_net_bytes_per_block "" 9000 1 5 >/dev/null && bad "net bytes per block without a tx counter" || ok
hbdb_net_bytes_per_block 9000 1000 1 5 >/dev/null && bad "net bytes per block with shrinking counters" || ok
hbdb_net_bytes_per_block 1x 9000 1 5 >/dev/null && bad "net bytes per block with junk" || ok

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

# ---- FaultPolicy activation (F2 kafka, F4, F5-F4): SIGUSR1 at AT ----
reset
FAULT_T0_MS=$((FAKE_NOW + 10000))
fault_parse F4:node4@5 tendermint 4 40
FAULT_T0_MS=$((FAKE_NOW + 10000))
fault_inject 2>/dev/null; rc=$?
eq "F4 inject rc" "$rc" 0
eq "F4 signalled (and confirmed) at t0+AT" "$(grep '^compose_exec ' "$TIMES" | awk '{ print $2 }' | sort -u)" 1015000
grep -q '^compose exec -T node4 sh -c ' "$CALLS" && ok || bad "F4 exec call: $(cat "$CALLS")"
grep -q ',corrupt_on,node4,SIGUSR1 to veritas (--fault=corrupt)$' "$FAULT_LOG" && ok || bad "F4 on line: $(cat "$FAULT_LOG")"
grep -q ',corrupt_applied,node4,kill returned$' "$FAULT_LOG" && ok || bad "F4 applied line: $(cat "$FAULT_LOG")"
eq "F4 log actions" "$(cut -d, -f6 "$FAULT_LOG" | tr '\n' ' ')" "action corrupt_on corrupt_applied corrupt_confirmed "
grep -q ",corrupt_confirmed,node4,$HBDB_FAULT_ACTIVE_FILE reads corrupt$" "$FAULT_LOG" && ok || bad "F4 confirmed line: $(cat "$FAULT_LOG")"
eq "F4 one confirmation read" "$(wc -l <"$TMP_ROOT/confirms" | tr -d ' ')" 1
eq "F4 log t_s" "$(grep ',corrupt_on,' "$FAULT_LOG" | cut -d, -f3)" "5.000"
# The script run in the container: SIGUSR1 to the pid in the pid file.
fake_pidfile_dir="$TMP_ROOT/pid"
mkdir -p "$fake_pidfile_dir"
sleep_bin=/bin/sleep # the sleep function above is a stub
"$sleep_bin" 30 &
victim=$!
script="$(cat "$TMP_ROOT/signal_script")"
contains "signal script reads the pid file" "$script" "cat $HBDB_VERITAS_PIDFILE"
contains "signal script sends SIGUSR1" "$script" 'kill -USR1 "$p"'
echo "$victim" >"$fake_pidfile_dir/v.pid"
sh -c "$(echo "$script" | sed "s#$HBDB_VERITAS_PIDFILE#$fake_pidfile_dir/v.pid#")"; rc=$?
eq "signal script rc" "$rc" 0
wait "$victim" 2>/dev/null; vrc=$?
eq "signal script delivered SIGUSR1 (exit 128+USR1)" "$vrc" $((128 + $(kill -l USR1)))
rm -f "$fake_pidfile_dir/v.pid"
sh -c "$(echo "$script" | sed "s#$HBDB_VERITAS_PIDFILE#$fake_pidfile_dir/v.pid#")" 2>/dev/null; rc=$?
[ "$rc" -ne 0 ] && ok || bad "signal script without a pid file must fail"
# The confirmation script prints the active file, empty when missing.
cscript="$(fault_confirm_script)"
contains "confirm script reads the active file" "$cscript" "cat $HBDB_FAULT_ACTIVE_FILE"
echo corrupt >"$fake_pidfile_dir/active"
eq "confirm script output" "$(sh -c "$(echo "$cscript" | sed "s#$HBDB_FAULT_ACTIVE_FILE#$fake_pidfile_dir/active#")")" corrupt
rm -f "$fake_pidfile_dir/active"
out="$(sh -c "$(echo "$cscript" | sed "s#$HBDB_FAULT_ACTIVE_FILE#$fake_pidfile_dir/active#")")"; rc=$?
eq "confirm script without the file" "$rc/$out" "0/"
# kafka F2: silent_on on two nodes, confirmed on both.
reset
CONFIRM_OUT=silent
FAULT_T0_MS="$FAKE_NOW"
fault_parse F2:node3,node4@0 kafka 4 40
FAULT_T0_MS="$FAKE_NOW"
fault_inject 2>/dev/null; rc=$?
eq "F2 kafka inject rc" "$rc" 0
eq "F2 kafka signal execs" "$(grep -c '^compose exec -T node[34] sh -c .*kill -USR1' "$CALLS")" 2
eq "F2 kafka log" "$(cut -d, -f6,7 "$FAULT_LOG" | sort | tr '\n' ' ')" "action,node silent_applied,node3 silent_applied,node4 silent_confirmed,node3 silent_confirmed,node4 silent_on,node3 silent_on,node4 "
eq "F2 kafka log header once" "$(grep -c '^utc,' "$FAULT_LOG")" 1
[ -e "$TMP_ROOT/last_script" ] && bad "F2 kafka ran a non-signal script: $(cat "$TMP_ROOT/last_script")" || ok
# Parallel: node3's kill only succeeds once node4's has been issued.
reset
PAR_HANDSHAKE=1
FAULT_T0_MS="$FAKE_NOW"
fault_parse F5-F4:node3,node4@0 tendermint 4 40
FAULT_T0_MS="$FAKE_NOW"
fault_inject 2>/dev/null; rc=$?
eq "parallel signals rc" "$rc" 0
eq "parallel signals confirmed" "$(grep -c ',corrupt_confirmed,node[34],' "$FAULT_LOG")" 2
# One node fails: rc 1, the other is still activated and confirmed.
reset
COMPOSE_FAIL_NODE=node3
FAULT_T0_MS="$FAKE_NOW"
fault_parse F5-F4:node3,node4@0 tendermint 4 40
FAULT_T0_MS="$FAKE_NOW"
fault_inject 2>/dev/null; rc=$?
eq "one node failing rc" "$rc" 1
grep -q ',corrupt_on_failed,node3,' "$FAULT_LOG" && ok || bad "node3 failure not logged: $(cat "$FAULT_LOG")"
grep -q ',corrupt_confirmed,node4,' "$FAULT_LOG" && ok || bad "node4 not confirmed: $(cat "$FAULT_LOG")"
# Aborted before AT: nothing signalled.
reset
FAULT_T0_MS="$FAKE_NOW"
fault_parse F5-F4:node3,node4@5 tendermint 4 40
FAULT_T0_MS="$FAKE_NOW"
: >"$FAULT_STOP"
fault_inject 2>/dev/null; rc=$?
eq "signal_all aborted rc" "$rc" 2
grep -q '^compose exec' "$CALLS" && bad "aborted signal_all ran exec" || ok
# Confirmation that arrives late (1.2 s): polled every 0.5 s, confirmed.
reset
FAULT_T0_MS="$FAKE_NOW"
CONFIRM_AT=$((FAKE_NOW + 1200))
fault_signal node4 0 corrupt 2>/dev/null; rc=$?
eq "late confirm rc" "$rc" 0
eq "late confirm polls" "$(wc -l <"$TMP_ROOT/confirms" | tr -d ' ')" 4
eq "late confirm slept" "$SLEPT_MS" 1500
# Never confirmed (wrong policy in the file): unconfirmed, rc 1.
reset
FAULT_T0_MS="$FAKE_NOW"
CONFIRM_OUT=silent
FAULT_CONFIRM_MS=2000
fault_signal node4 0 corrupt 2>/dev/null; rc=$?
eq "unconfirmed rc" "$rc" 1
grep -q ",corrupt_unconfirmed,node4,$HBDB_FAULT_ACTIVE_FILE does not read corrupt after 2000ms$" "$FAULT_LOG" && ok || bad "unconfirmed line: $(cat "$FAULT_LOG")"
grep -q ',corrupt_confirmed,' "$FAULT_LOG" && bad "unconfirmed run logged confirmed" || ok
eq "unconfirmed waited FAULT_CONFIRM_MS" "$SLEPT_MS" 2000
# F5-F4: both nodes corrupt; F5-F2: iptables on both nodes.
reset
FAULT_T0_MS="$FAKE_NOW"
fault_parse F5-F4:node3,node4@0 tendermint 4 40
FAULT_T0_MS="$FAKE_NOW"
fault_inject 2>/dev/null
eq "F5-F4 log" "$(grep -c ',corrupt_applied,node[34],' "$FAULT_LOG")" 2
reset
fault_parse F5-F2:node3,node4@0 tendermint 4 40
FAULT_T0_MS="$FAKE_NOW"
fault_inject 2>/dev/null; rc=$?
eq "F5-F2 inject rc" "$rc" 0
eq "F5-F2 iptables on both" "$(grep -c ',iptables_applied,node[34],' "$FAULT_LOG")" 2
grep -q 'iptables' "$TMP_ROOT/last_script" && ok || bad "F5-F2 did not run iptables"
# Failure and abort.
reset
FAULT_T0_MS="$FAKE_NOW"
COMPOSE_EXEC_RC=1
fault_signal node4 0 corrupt 2>/dev/null; rc=$?
eq "signal failure rc" "$rc" 1
grep -q ',corrupt_on_failed,node4,' "$FAULT_LOG" && ok || bad "signal failure logged"
reset
FAULT_T0_MS="$FAKE_NOW"
: >"$FAULT_STOP"
fault_signal node4 5 corrupt; rc=$?
eq "signal aborted rc" "$rc" 2
grep -q '^compose exec' "$CALLS" && bad "aborted signal ran exec" || ok
(fault_signal node4 0 bogus) 2>/dev/null; rc=$?
eq "signal unknown policy dies" "$rc" 1

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
contains "run_one F2 kafka parses" "$r" "missing $empty_ycsb/a.load.dat"
r="$(run_validate BACKEND=raft FAULT=F4:node4@20)"
contains "run_one F4 parses" "$r" "missing $empty_ycsb/a.load.dat"
r="$(run_validate BACKEND=tendermint FAULT=F5-F4:node3,node4@20)"
contains "run_one F5-F4 parses" "$r" "missing $empty_ycsb/a.load.dat"
r="$(run_validate BACKEND=kafka-orig FAULT=F4:node4@20)"
contains "run_one F4 kafka-orig rejected" "$r" "needs a veritas-core backend"
r="$(run_validate BACKEND=raft FAULT=F5-F4:node3,node4@20)"
contains "run_one F5 raft rejected" "$r" "BFT backends only"
r="$(run_validate BACKEND=raft ATTEST_EVERY=1 FAULT=F5-F4:node3,node4@20)"
contains "run_one F5-F4 raft with attestations parses" "$r" "missing $empty_ycsb/a.load.dat"
r="$(run_validate BACKEND=raft ATTEST_EVERY=1 ATTEST_QUORUM=f+1 READ_MODE=certified FAILOVER=1 SIGN_READS=on-failure FAULT=F6:node4@20)"
contains "run_one phase 3 knobs parse" "$r" "missing $empty_ycsb/a.load.dat"
r="$(run_validate BACKEND=raft READ_MODE=quorum)"
contains "run_one READ_MODE=quorum parses" "$r" "missing $empty_ycsb/a.load.dat"
r="$(run_validate BACKEND=raft ATTEST_QUORUM=2f+1)"
contains "run_one ATTEST_QUORUM needs attestations" "$r" "ATTEST_QUORUM=2f+1 needs ATTEST_EVERY > 0"
r="$(run_validate BACKEND=raft ATTEST_EVERY=1 ATTEST_QUORUM=3)"
contains "run_one ATTEST_QUORUM bad value" "$r" "ATTEST_QUORUM must be auto, f+1 or 2f+1"
r="$(run_validate BACKEND=raft FAILOVER=2)"
contains "run_one FAILOVER bad value" "$r" "FAILOVER must be 0 or 1"
r="$(run_validate BACKEND=raft SIGN_READS=sometimes)"
contains "run_one SIGN_READS bad value" "$r" "SIGN_READS must be off, on-failure or always"
r="$(run_validate BACKEND=kafka-orig SIGN_READS=always)"
contains "run_one SIGN_READS needs a core backend" "$r" "need a core backend"
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
# up.sh also records the NODE_FAULTS it was given (the armed --fault nodes).
printf '#!/bin/sh\necho up >>"$STUB_STATE/cluster.log"\necho "${NODE_FAULTS-unset}" >"$STUB_STATE/node_faults"\necho "${ATTEST_QUORUM_N-unset}" >"$STUB_STATE/attest_quorum"\nexit 0\n' >"$SB/experiments/cluster/up.sh"
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
  stats)
    # One line per container in run_one.sh's format; every node's tx grows
    # by 100 kB per call (STUB_STATS_FAIL=1: print nothing, exit 1).
    [ -z "${STUB_STATS_FAIL:-}" ] || exit 1
    k=$(($(cat "$STUB_STATE/stats_n" 2>/dev/null || echo 0) + 1))
    echo "$k" >"$STUB_STATE/stats_n"
    for a; do
      case "$a" in
        c*) echo "hbdb-$(svc_of "$a")-1,1.00%,10.5MiB / 768MiB,1.5MB / $((k * 100))kB" ;;
      esac
    done
    sleep 0.2
    exit 0 ;;
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
      logs)
        # STUB_LOG_NODE's log holds STUB_LOG_LINE; every node logs a raft
        # leader line when STUB_LEADER_LOG=1.
        for svc; do :; done
        echo "$svc | started"
        [ "$svc" != "${STUB_LOG_NODE:-}" ] || echo "$svc | ${STUB_LOG_LINE:-}"
        [ -z "${STUB_LEADER_LOG:-}" ] || [ "$svc" != node1 ] ||
          echo "$svc | 2026-10-31T00:00:01.000Z [INFO]  raft: entering leader state: leader=\"Node at 172.28.0.11:7100 [Leader]\""
        exit 0 ;;
      exec)
        shift 2 # exec -T
        svc="$1"
        shift
        if [ "$svc" != client ]; then
          echo "$svc $*" >>"$STUB_STATE/node_execs"
          case "$*" in
            *hbdb-fault-active*) echo "${STUB_CONFIRM-corrupt}" ;;
            *"kill -USR2"*)
              # veritas writes breakdown snapshot k with 10*k applied blocks.
              [ "${STUB_SNAP_RC:-0}" = 0 ] || exit "$STUB_SNAP_RC"
              k=$(($(cat "$STUB_STATE/snap_$svc" 2>/dev/null || echo 0) + 1))
              echo "$k" >"$STUB_STATE/snap_$svc"
              printf '{\n  "node": %s,\n  "batch_ns": 5,\n  "blocks": %d,\n  "batch_blocks": 99\n}\n' "${svc#node}" $((k * 10)) \
                >"$STUB_RESULTS/breakdown-$svc.snap$k.json"
              # its eth0 tx counter: 1 MB more per snapshot (+ node index)
              echo $((k * 1000000 + ${svc#node}))
              exit 0 ;;
            *statistics/tx_bytes*)
              # the kafka broker's counter: 0.5 MB more per read
              k=$(($(cat "$STUB_STATE/tx_$svc" 2>/dev/null || echo 0) + 1))
              echo "$k" >"$STUB_STATE/tx_$svc"
              echo $((k * 500000))
              exit 0 ;;
          esac
          exit "${STUB_EXEC_RC:-0}"
        fi
        if [ "$1" = auditor ]; then
          echo "$*" >>"$STUB_STATE/auditor_once"
          n=$(($(cat "$STUB_STATE/audit_n" 2>/dev/null || echo 0) + 1))
          echo "$n" >"$STUB_STATE/audit_n"
          # STUB_AUDIT_DIVERGE=k: the first k --once calls say DIVERGENT.
          if [ "$n" -le "${STUB_AUDIT_DIVERGE:-0}" ]; then echo "DIVERGENT node3"; exit 1; fi
          [ -z "${STUB_AUDIT_FAIL:-}" ] || { echo "auditor: node2: connection refused" >&2; exit 2; }
          echo CONSISTENT
          exit 0
        fi
        if [ "$1" = sh ]; then
          # run_one.sh start_audit: `sh -c SCRIPT sh PIDFILE ADDRS OUT` starts
          # the periodic auditor, `sh -c 'kill ...' sh PIDFILE` stops it.
          case "$3" in
            *auditor*)
              echo "$$" >"$STUB_RESULTS${5#/results}"
              echo "$$" >"$STUB_STATE/audit.pid"
              trap 'echo exited >"$STUB_STATE/audit.exited"; exit 0' TERM
              while :; do
                echo "$(date +%s).000," >>"$STUB_RESULTS${7#/results}"
                sleep 0.2
              done ;;
            *kill*) kill -TERM "$(cat "$STUB_RESULTS${5#/results}")"; exit 0 ;;
          esac
          exit 0
        fi
        out="" tl="" wtl="" ops="" tags=""
        for a; do
          case "$a" in
            --out=*) out="$STUB_RESULTS${a#--out=/results}" ;;
            --timeline=*) tl="$STUB_RESULTS${a#--timeline=/results}" ;;
            --wtimeline=*) wtl="$STUB_RESULTS${a#--wtimeline=/results}" ;;
            --ops=*) ops="$STUB_RESULTS${a#--ops=/results}" ;;
            --tags=*) tags="${a#--tags=}" ;;
          esac
        done
        echo "ycsbbench: loaded 1 records"
        echo "ycsbbench: run phase started (warmup x, duration y)"
        sleep "${STUB_BENCH_SECS:-1}"
        # STUB_TL: the timeline, space-separated (default 5 5 5).
        printf '%s\n' ${STUB_TL:-5 5 5} >"$tl"
        printf '2\n2\n2\n' >"$wtl"
        # STUB_WERR / STUB_WABORT: failed / aborted writes in the ops sidecar
        # (of 200 ok); STUB_NO_OPS=1: no sidecar (an old ycsbbench).
        # STUB_REJECTED: a rejected_reads column (phase 3) with that count on
        # the read line.
        if [ -n "${STUB_NO_OPS:-}" ]; then :
        elif [ -n "${STUB_REJECTED:-}" ]; then
          printf 'op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted,rejected_reads\nread,300,150.00,1.000,1.000,2.000,0,0,%s\nwrite,200,100.00,500.000,450.000,900.000,%s,%s,0\n' \
            "$STUB_REJECTED" "${STUB_WERR:-0}" "${STUB_WABORT:-0}" >"$ops"
        else
          printf 'op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted\nread,300,150.00,1.000,1.000,2.000,0,0\nwrite,200,100.00,500.000,450.000,900.000,%s,%s\n' \
            "${STUB_WERR:-0}" "${STUB_WABORT:-0}" >"$ops"
        fi
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
# The host guard reads ps and vm_stat: STUB_PS_CPU is the %CPU of the one
# process outside the Docker VM; vm_stat's Swapouts grows by STUB_SWAP_STEP
# pages per call (state in $STUB_STATE).
cat >"$SB/bin/ps" <<'EOF'
#!/bin/sh
echo "650.0 /System/Library/Frameworks/Virtualization.framework/XPCServices/com.apple.Virtualization.VirtualMachine"
echo "  4.0 /Applications/Docker.app/Contents/MacOS/com.docker.backend"
echo "${STUB_PS_CPU:-50.0} /Applications/Some App.app/Contents/MacOS/Some App"
EOF
cat >"$SB/bin/vm_stat" <<'EOF'
#!/bin/sh
f="$STUB_STATE/swapouts"
n="$(cat "$f" 2>/dev/null || echo 1000)"
echo "Mach Virtual Memory Statistics: (page size of 16384 bytes)"
echo "Pages free:                               5000."
echo "Pages occupied by compressor:             1000."
echo "Swapouts:                            $n."
echo $((n + ${STUB_SWAP_STEP:-0})) >"$f"
EOF
chmod +x "$SB/bin/ps" "$SB/bin/vm_stat"
SB_RAW="$SB/results/raw"
# itest NAME ENV...: run the sandboxed run_one.sh; sets it_rc, it_err.
itest() {
  local name="$1"
  shift
  rm -rf "$SB/state"
  mkdir -p "$SB/state"
  it_err="$(env PATH="$SB/bin:$PATH" STUB_STATE="$SB/state" STUB_RESULTS="$SB_RAW" STUB_HEADER="$HBDB_CSV_HEADER" \
    YCSB_DIR="$SB/ycsb" BACKEND=raft N=4 WARMUP=0s AUDIT_WAIT_SECS=0 "$@" "$SB/experiments/run_one.sh" 2>&1 >/dev/null)"
  it_rc=$?
}
it_tag() { # it_tag EXP SUFFIX: the single results/raw/EXP/*SUFFIX file
  ls "$SB_RAW/$1"/*"$2" 2>/dev/null | head -n 1
}
raw_tag() { # raw_tag EXP: the single .faults file of the experiment
  ls "$SB_RAW/$1"/*.faults 2>/dev/null
}

# A: RTT only -> .faults keeps the netem_rtt record; netem with limit.
itest rtt EXP=it_rtt RTT_MS=20 DURATION=2s STUB_BENCH_SECS=0.3
eq "itest rtt rc" "$it_rc" 0
f="$(raw_tag it_rtt)"
[ -n "$f" ] && ok || bad "itest rtt: no .faults file: $it_err"
grep -q ',netem_rtt,node1-node4,egress delay 10ms on every node and on tso (RTT_MS=20)$' "$f" 2>/dev/null && ok || bad "itest rtt: no netem_rtt line: $(cat "$f" 2>/dev/null)"
eq "itest rtt tc calls" "$(grep -c 'exec -T node[1-4] tc qdisc replace dev eth0 root netem delay 10ms limit 100000$' "$SB/state/calls")" 4
eq "itest rtt tc on the tso (raft)" "$(grep -c 'exec -T tso tc qdisc replace dev eth0 root netem delay 10ms limit 100000$' "$SB/state/calls")" 1
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
eq "itest f1 row fields" "$(echo "$row" | awk -F, '{ print NF }')" 23
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

# G: FAULT != none starts the periodic auditor in the client container and
# stops it (and removes its pid/stop files) when the bench ends.
itest audit EXP=it_au FAULT=F1:node4@1 DURATION=2s STUB_BENCH_SECS=2
eq "itest audit rc" "$it_rc" 0
a="$(ls "$SB_RAW"/it_au/*.audit 2>/dev/null)"
[ -n "$a" ] && [ -s "$a" ] && ok || bad "itest audit: no .audit lines: $it_err"
grep -Eq '^[0-9]+\.000,$' "$a" 2>/dev/null && ok || bad "itest audit line format: $(cat "$a" 2>/dev/null)"
grep 'exec -T client sh -c .*exec auditor --addrs=.*--interval=1s' "$SB/state/calls" |
  grep -q ' sh /results/it_au/\..*\.audit\.pid 172\.28\.0\.11:1990,.* /results/it_au/.*\.audit$' && ok ||
  bad "itest audit start call: $(grep auditor "$SB/state/calls")"
[ -e "$SB/state/audit.exited" ] && ok || bad "itest audit: the auditor was not terminated"
kill -0 "$(cat "$SB/state/audit.pid" 2>/dev/null)" 2>/dev/null && bad "itest audit: auditor still running" || ok
eq "itest audit temp files removed" "$(ls -a "$SB_RAW/it_au" | grep -c -e '\.audit\.pid$' -e '\.audit\.stop$' -e '\.audit\.err$')" 0

# H: a failed bench (exit 3) stops the auditor too.
itest auditfail EXP=it_aufail FAULT=F1:node4@0 DURATION=2s STUB_BENCH_SECS=1 STUB_BENCH_RC=1
eq "itest audit bench failure rc" "$it_rc" 3
[ -e "$SB/state/audit.exited" ] && ok || bad "itest audit bench failure: the auditor was not terminated"
kill -0 "$(cat "$SB/state/audit.pid" 2>/dev/null)" 2>/dev/null && bad "itest audit bench failure: auditor still running" || ok

# I: no fault -> no auditor; original backends are not audited.
itest noaudit EXP=it_noau DURATION=2s STUB_BENCH_SECS=1
eq "itest no-fault rc" "$it_rc" 0
grep -q 'exec auditor' "$SB/state/calls" && bad "itest no-fault: the periodic auditor was started" || ok
grep -q 'exec -T client auditor --addrs=.* --once$' "$SB/state/calls" && ok || bad "itest no-fault: no auditor --once (validity gate)"
eq "itest no-fault audit file" "$(ls "$SB_RAW"/it_noau/*.audit 2>/dev/null)" ""
itest origaudit EXP=it_orig BACKEND=kafka-orig FAULT=F1:node4@0 DURATION=2s STUB_BENCH_SECS=1
eq "itest kafka-orig rc" "$it_rc" 0
contains "itest kafka-orig audit skipped" "$it_err" "audit skipped"
grep -q 'auditor' "$SB/state/calls" && bad "itest kafka-orig: the auditor was started" || ok

# J: F4 through run_one: node4 is created with --fault armed (NODE_FAULTS to
# up.sh) and signalled at marker + WARMUP + AT; the audit runs.
itest f4 EXP=it_f4 BACKEND=tendermint FAULT=F4:node4@1 DURATION=2s STUB_BENCH_SECS=2
eq "itest f4 rc" "$it_rc" 0
eq "itest f4 NODE_FAULTS to up.sh" "$(cat "$SB/state/node_faults" 2>/dev/null)" "node4=corrupt"
grep -q '^node4 sh -c .*kill -USR1' "$SB/state/node_execs" 2>/dev/null && ok || bad "itest f4 signal exec: $(cat "$SB/state/node_execs" 2>/dev/null)"
f="$(raw_tag it_f4)"
eq "itest f4 log actions" "$(cut -d, -f6 "$f" 2>/dev/null | tr '\n' ' ')" "action run_phase_start corrupt_on corrupt_applied corrupt_confirmed "
t_s="$(grep ',corrupt_on,' "$f" 2>/dev/null | cut -d, -f3)"
awk -v t="$t_s" 'BEGIN { exit !(t >= 1.0 && t < 1.6) }' && ok || bad "itest f4 t_s=$t_s (want ~1)"
eq "itest f4 fault column" "$(tail -n 1 "$SB_RAW/it_f4.csv" | cut -d, -f11)" "F4:node4@1"
grep -q 'auditor' "$SB/state/calls" && ok || bad "itest f4: the auditor was not started"
# K: F5-F4 arms both nodes; F1 arms none.
itest f5 EXP=it_f5 BACKEND=tendermint FAULT=F5-F4:node3,node4@0 DURATION=2s STUB_BENCH_SECS=1
eq "itest f5 rc" "$it_rc" 0
eq "itest f5 NODE_FAULTS" "$(cat "$SB/state/node_faults" 2>/dev/null)" "node3=corrupt node4=corrupt"
eq "itest f5 fault column" "$(tail -n 1 "$SB_RAW/it_f5.csv" | cut -d, -f11)" "F5-F4:node3+node4@0"
itest f1nf EXP=it_f1nf FAULT=F1:node4@0 DURATION=2s STUB_BENCH_SECS=1
eq "itest f1 NODE_FAULTS empty" "$(cat "$SB/state/node_faults" 2>/dev/null)" ""
# bft2 (Task 13 + 15): F4 and F5-F4 arm veritas and signal it at AT; F5-F2
# drops the HotStuff replica port on both nodes.
itest f4b EXP=it_f4b BACKEND=bft2 FAULT=F4:node4@0 DURATION=2s STUB_BENCH_SECS=1
eq "itest f4 bft2 rc" "$it_rc" 0
eq "itest f4 bft2 NODE_FAULTS" "$(cat "$SB/state/node_faults" 2>/dev/null)" "node4=corrupt"
grep -q '^node4 sh -c .*cat /tmp/hbdb-veritas.pid.*kill -USR1' "$SB/state/node_execs" 2>/dev/null && ok || bad "itest f4 bft2 signal exec: $(cat "$SB/state/node_execs" 2>/dev/null)"
eq "itest f4 bft2 log actions" "$(cut -d, -f6 "$(raw_tag it_f4b)" 2>/dev/null | tr '\n' ' ')" "action run_phase_start corrupt_on corrupt_applied corrupt_confirmed "
itest f5b EXP=it_f5b BACKEND=bft2 FAULT=F5-F4:node3,node4@0 DURATION=2s STUB_BENCH_SECS=1
eq "itest f5-f4 bft2 rc" "$it_rc" 0
eq "itest f5-f4 bft2 NODE_FAULTS" "$(cat "$SB/state/node_faults" 2>/dev/null)" "node3=corrupt node4=corrupt"
eq "itest f5-f4 bft2 signals" "$(grep -c '^node[34] sh -c .*kill -USR1' "$SB/state/node_execs" 2>/dev/null)" 2
itest f5nb EXP=it_f5nb BACKEND=bft2 FAULT=F5-F2:node3,node4@0 DURATION=2s STUB_BENCH_SECS=1
eq "itest f5-f2 bft2 rc" "$it_rc" 0
eq "itest f5-f2 bft2 NODE_FAULTS empty" "$(cat "$SB/state/node_faults" 2>/dev/null)" ""
eq "itest f5-f2 bft2 iptables" "$(grep -c ',iptables_applied,node[34],' "$(raw_tag it_f5nb)" 2>/dev/null)" 2
grep -q ',iptables_drop,node3,tcp OUTPUT --dport/--sport 7200 7202$' "$(raw_tag it_f5nb)" 2>/dev/null && ok || bad "itest f5-f2 bft2 port: $(cat "$(raw_tag it_f5nb)" 2>/dev/null)"

# L: a failed signal (no pid file in the container) is exit 3, no row.
itest f4fail EXP=it_f4fail BACKEND=kafka FAULT=F2:node4@0 DURATION=2s STUB_BENCH_SECS=1 STUB_EXEC_RC=1
eq "itest f2 kafka signal failure rc" "$it_rc" 3
grep -q ',silent_on_failed,node4,' "$(raw_tag it_f4fail)" 2>/dev/null && ok || bad "itest f2 kafka failure not logged"
[ -e "$SB_RAW/it_f4fail.csv" ] && bad "itest f2 kafka failure wrote a CSV row" || ok

# M: the activation is never confirmed -> exit 3, no row.
itest unconf EXP=it_unconf BACKEND=tendermint FAULT=F4:node4@0 DURATION=2s STUB_BENCH_SECS=2 STUB_CONFIRM=none FAULT_CONFIRM_MS=600
eq "itest unconfirmed rc" "$it_rc" 3
grep -q ',corrupt_unconfirmed,node4,' "$(raw_tag it_unconf)" 2>/dev/null && ok || bad "itest unconfirmed not logged: $(cat "$(raw_tag it_unconf)" 2>/dev/null)"
[ -e "$SB_RAW/it_unconf.csv" ] && bad "itest unconfirmed wrote a CSV row" || ok
contains "itest unconfirmed msg" "$it_err" "fault injection F4:node4@0 failed"
# kafka F2 confirmed through the active file reading silent.
itest kf2 EXP=it_kf2 BACKEND=kafka FAULT=F2:node4@0 DURATION=2s STUB_BENCH_SECS=2 STUB_CONFIRM=silent
eq "itest kafka f2 rc" "$it_rc" 0
eq "itest kafka f2 NODE_FAULTS" "$(cat "$SB/state/node_faults" 2>/dev/null)" "node4=silent"
grep -q ',silent_confirmed,node4,' "$(raw_tag it_kf2)" 2>/dev/null && ok || bad "itest kafka f2 not confirmed"

# ---- final-review fixes: sidecars, net_bytes_per_block, validity gate ----
# N: a fault-free core run keeps .ops.csv/.wtimeline/.stats/.snaps, fills
# net_bytes_per_block from the two node1 snapshots, audits once.
itest sidecars EXP=it_side DURATION=2s STUB_BENCH_SECS=2
eq "itest sidecars rc" "$it_rc" 0
o="$(it_tag it_side .ops.csv)"
eq "itest ops.csv header" "$(head -n 1 "$o" 2>/dev/null)" "op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,aborted"
eq "itest ops.csv kinds" "$(cut -d, -f1 "$o" 2>/dev/null | tr '\n' ' ')" "op read write "
grep -q -- '--wtimeline=/results/it_side/.*\.wtimeline --ops=/results/it_side/.*\.ops\.csv' "$SB/state/calls" && ok || bad "itest: ycsbbench --wtimeline/--ops"
[ -s "$(it_tag it_side .wtimeline)" ] && ok || bad "itest: no .wtimeline"
s="$(it_tag it_side .stats)"
eq "itest stats header" "$(head -n 1 "$s" 2>/dev/null)" "$HBDB_STATS_HEADER"
grep -Eq '^[0-9]+,[0-9T:Z-]+,hbdb-node3-1,1.00,10.5MiB,768MiB,1.5MB,[0-9]+kB,11010048,1500000,[0-9]+$' "$s" 2>/dev/null && ok ||
  bad "itest stats line: $(sed -n 2p "$s" 2>/dev/null)"
grep -q ',hbdb-client-1,' "$s" 2>/dev/null && ok || bad "itest stats: client container not sampled"
eq "itest stats: one line per container per sample" "$(($(grep -c . "$s") - 1))" "$((5 * $(cat "$SB/state/stats_n" 2>/dev/null || echo 0)))"
p="$(it_tag it_side .snaps)"
eq "itest snaps" "$(cut -d, -f1,3 "$p" 2>/dev/null | tr '\n' ' ')" "1,ok 2,ok "
# N4: exact eth0 tx counters read in the same exec as the SIGUSR2.
eq "itest snaps tx" "$(cut -d, -f4,5 "$p" 2>/dev/null | tr '\n' ' ')" \
  "4000010,node1=1000001+node2=1000002+node3=1000003+node4=1000004 8000010,node1=2000001+node2=2000002+node3=2000003+node4=2000004 "
eq "itest snapshots signalled on every node twice" "$(grep -c '^node[1-4] sh -c t=.*cat /sys/class/net/eth0/statistics/tx_bytes.*cat /tmp/hbdb-veritas.pid.*kill -USR2' "$SB/state/node_execs" 2>/dev/null)" 8
eq "itest snapshots collected" "$(ls "$SB_RAW/it_side/"*.breakdown/ 2>/dev/null | grep -c 'snap[12]\.json$')" 8
nb="$(tail -n 1 "$SB_RAW/it_side.csv" 2>/dev/null | cut -d, -f22)"
eq "itest net_bytes_per_block (4 x 1 MB / 10 blocks)" "$nb" 400000
eq "itest row has 23 fields" "$(tail -n 1 "$SB_RAW/it_side.csv" | awk -F, '{ print NF }')" 23
eq "itest final audit" "$(cat "$(it_tag it_side .final_audit)" 2>/dev/null)" "CONSISTENT"
grep -q '^auditor --addrs=172.28.0.11:1990,172.28.0.12:1990,172.28.0.13:1990,172.28.0.14:1990 --once$' "$SB/state/auditor_once" 2>/dev/null && ok ||
  bad "itest auditor --once call: $(cat "$SB/state/auditor_once" 2>/dev/null)"
contains "itest sampler/net log" "$it_err" "net_bytes_per_block=400000 (4000000 tx bytes, node1 applied 10 blocks"
# kafka: the broker's counter joins the sum (read in the same snapshot step).
itest kside EXP=it_kside BACKEND=kafka DURATION=2s STUB_BENCH_SECS=1
eq "itest kafka nbpb rc" "$it_rc" 0
eq "itest kafka nbpb includes the broker (4.5 MB / 10 blocks)" "$(tail -n 1 "$SB_RAW/it_kside.csv" | cut -d, -f22)" 450000
contains "itest kafka snaps detail" "$(cat "$(it_tag it_kside .snaps)" 2>/dev/null)" "+kafka=500000"
# N2: no net_bytes_per_block for a fault run.
itest nbf EXP=it_nbf FAULT=F1:node4@0 DURATION=2s STUB_BENCH_SECS=1
eq "itest fault run rc" "$it_rc" 0
eq "itest fault run: net_bytes_per_block empty" "$(tail -n 1 "$SB_RAW/it_nbf.csv" | cut -d, -f22)" ""
contains "itest fault run: nbpb note" "$it_err" "net_bytes_per_block left empty: FAULT=F1:node4@0"
grep -q 'kill -0 "$parent" 2>/dev/null || exit 0$' "$HERE/run_one.sh" && ok || bad "run_one sampler parent guard"
grep -q 'kill -0 "$parent" 2>/dev/null || exit 0 # run_one.sh is gone' "$HERE/run_one.sh" && ok || bad "run_one watchdog parent guard"

# O: write errors above 1 % of the writes fail a fault-free run; 1 % passes;
# aborted writes (gRPC Aborted, by design) never count (N1).
itest werr EXP=it_werr DURATION=2s STUB_BENCH_SECS=0.5 STUB_WERR=3
eq "itest write errors 3/203 rc" "$it_rc" 3
contains "itest write errors msg" "$it_err" "3 of 203 writes failed (not counting 0 aborted"
[ -e "$SB_RAW/it_werr.csv" ] && bad "itest write errors wrote a row" || ok
itest werr2 EXP=it_werr2 DURATION=2s STUB_BENCH_SECS=0.5 STUB_WERR=2
eq "itest write errors 2/202 rc" "$it_rc" 0
itest wab EXP=it_wab DURATION=2s STUB_BENCH_SECS=0.5 STUB_WABORT=40
eq "itest 40 aborted of 240 writes pass" "$it_rc" 0
contains "itest aborted note" "$it_err" "40 of 240 writes were aborted"
itest wab2 EXP=it_wab2 DURATION=2s STUB_BENCH_SECS=0.5 STUB_WABORT=40 STUB_WERR=2
eq "itest 2 errors + 40 aborted of 242: 0.8% passes" "$it_rc" 0
itest wab3 EXP=it_wab3 DURATION=2s STUB_BENCH_SECS=0.5 STUB_WABORT=40 STUB_WERR=3
eq "itest 3 errors + 40 aborted of 243: 1.2% fails" "$it_rc" 3
itest werrf EXP=it_werrf FAULT=F1:node4@0 DURATION=2s STUB_BENCH_SECS=1 STUB_WERR=150
eq "itest write errors in a fault run are not checked" "$it_rc" 0
[ -e "$SB/state/auditor_once" ] && bad "itest fault run ran auditor --once" || ok
itest noops EXP=it_noops DURATION=2s STUB_BENCH_SECS=0.5 STUB_NO_OPS=1
eq "itest missing ops sidecar rc" "$it_rc" 3
contains "itest missing ops sidecar msg" "$it_err" "wrote no usable"

# P: zero seconds in the measurement window.
# N3: the default is 5 consecutive zero seconds.
itest zero3 EXP=it_zero3 DURATION=5s STUB_BENCH_SECS=0.5 STUB_TL="5 0 0 0 5"
eq "itest 3 zero seconds pass at the default 5" "$it_rc" 0
itest zero4 EXP=it_zero4 DURATION=6s STUB_BENCH_SECS=0.5 STUB_TL="5 0 0 0 0 5"
eq "itest 4 zero seconds pass at the default 5" "$it_rc" 0
itest zero5 EXP=it_zero5 DURATION=7s STUB_BENCH_SECS=0.5 STUB_TL="5 0 0 0 0 0 5"
eq "itest 5 zero seconds rc" "$it_rc" 3
contains "itest 5 zero seconds msg" "$it_err" "5 consecutive measurement second(s) with 0 successful ops"
itest zero3s EXP=it_zero3s DURATION=5s STUB_BENCH_SECS=0.5 STUB_TL="5 0 0 0 5" VALID_ZERO_RUN_SECS=3
eq "itest VALID_ZERO_RUN_SECS=3 rejects 3 zero seconds" "$it_rc" 3
itest zero1 EXP=it_zero1 DURATION=5s STUB_BENCH_SECS=0.5 STUB_TL="5 0 5 0 5"
eq "itest isolated zero seconds pass" "$it_rc" 0
itest zero1s EXP=it_zero1s DURATION=5s STUB_BENCH_SECS=0.5 STUB_TL="5 0 5 0 5" VALID_ZERO_RUN_SECS=1
eq "itest VALID_ZERO_RUN_SECS=1 rejects one zero second" "$it_rc" 3
itest zerow EXP=it_zerow WARMUP=2s DURATION=3s STUB_BENCH_SECS=0.5 STUB_TL="0 0 0 5 5 5"
eq "itest zero seconds in the warmup do not count" "$it_rc" 0
itest short EXP=it_short DURATION=8s STUB_BENCH_SECS=0.5 STUB_TL="5 5 5"
eq "itest timeline shorter than the window rc" "$it_rc" 3

# Q: fatal node log lines.
itest fatal EXP=it_fatal DURATION=2s STUB_BENCH_SECS=0.5 STUB_LOG_NODE=node3 STUB_LOG_LINE="core: node 3: FATAL apply at height 9, stopping node: boom"
eq "itest FATAL apply rc" "$it_rc" 3
contains "itest FATAL apply msg" "$it_err" "node3 log has"
itest buf EXP=it_buf BACKEND=tendermint DURATION=2s STUB_BENCH_SECS=0.5 STUB_LOG_NODE=node2 STUB_LOG_LINE="abci: buffer is full"
eq "itest buffer is full rc" "$it_rc" 3
itest bft2f EXP=it_bft2f BACKEND=bft2 DURATION=2s STUB_BENCH_SECS=0.5 STUB_LOG_NODE=node4 STUB_LOG_LINE="bft2-node 4: replica failed, exiting"
eq "itest bft2 replica failed rc" "$it_rc" 3
itest okl EXP=it_okl DURATION=2s STUB_BENCH_SECS=0.5 STUB_LOG_NODE=node2 STUB_LOG_LINE="core: node 2: applied block"
eq "itest harmless log line" "$it_rc" 0

# R: auditor --once: DIVERGENT three times fails, once is retried, no verdict warns.
itest div EXP=it_div DURATION=2s STUB_BENCH_SECS=0.5 STUB_AUDIT_DIVERGE=3
eq "itest persistent DIVERGENT rc" "$it_rc" 3
contains "itest persistent DIVERGENT msg" "$it_err" "auditor --once: DIVERGENT node3 (3 tries)"
eq "itest DIVERGENT tries" "$(grep -c . "$SB/state/auditor_once" 2>/dev/null)" 3
itest div1 EXP=it_div1 DURATION=2s STUB_BENCH_SECS=0.5 STUB_AUDIT_DIVERGE=1
eq "itest one-off DIVERGENT is retried" "$it_rc" 0
eq "itest one-off DIVERGENT tries" "$(grep -c . "$SB/state/auditor_once" 2>/dev/null)" 2
itest auf EXP=it_auf DURATION=2s STUB_BENCH_SECS=0.5 STUB_AUDIT_FAIL=1
eq "itest auditor without verdict: row stands" "$it_rc" 0
contains "itest auditor without verdict: warning" "$it_err" "gave no verdict"

# S: original backends: validity gate yes, no auditor, no net_bytes_per_block.
itest orig EXP=it_orig2 BACKEND=kafka-orig DURATION=2s STUB_BENCH_SECS=1
eq "itest kafka-orig fault-free rc" "$it_rc" 0
[ -e "$SB/state/auditor_once" ] && bad "itest kafka-orig ran auditor --once" || ok
eq "itest kafka-orig net_bytes_per_block empty" "$(tail -n 1 "$SB_RAW/it_orig2.csv" | cut -d, -f22)" ""
grep -q 'kill -USR2' "$SB/state/node_execs" 2>/dev/null && bad "itest kafka-orig got SIGUSR2" || ok

# T: a failed snapshot or no stats leaves net_bytes_per_block empty, the row stands.
itest nosnap EXP=it_nosnap DURATION=2s STUB_BENCH_SECS=1 STUB_SNAP_RC=1
eq "itest failed snapshot rc" "$it_rc" 0
eq "itest failed snapshot: net empty" "$(tail -n 1 "$SB_RAW/it_nosnap.csv" | cut -d, -f22)" ""
contains "itest failed snapshot: warning" "$it_err" "net_bytes_per_block left empty"
eq "itest failed snapshot: no second snapshot" "$(cut -d, -f1,3 "$(it_tag it_nosnap .snaps)" 2>/dev/null | tr '\n' ' ')" "1,failed "
itest nostats EXP=it_nostats DURATION=2s STUB_BENCH_SECS=1.5 STUB_STATS_FAIL=1
eq "itest docker stats failing rc" "$it_rc" 0
n_stats="$(grep -c '^docker stats' "$SB/state/calls")"
awk -v n="$n_stats" 'BEGIN { exit !(n >= 1 && n <= 8) }' && ok || bad "itest docker stats failing: $n_stats calls in ~1.5 s (busy loop?)"
eq "itest docker stats failing: cpu empty, nbpb from the exact counters" "$(tail -n 1 "$SB_RAW/it_nostats.csv" | cut -d, -f20,22)" ",400000"

# U: raft fault runs record the leader changes.
itest leaders EXP=it_lead FAULT=F1:node1@0 DURATION=2s STUB_BENCH_SECS=1 STUB_LEADER_LOG=1
eq "itest leader fault rc" "$it_rc" 0
grep -q '^node1 node1 | .*entering leader state' "$(it_tag it_lead .leaders)" 2>/dev/null && ok || bad "itest leaders file: $(cat "$(it_tag it_lead .leaders)" 2>/dev/null)"
itest noleaders EXP=it_nolead FAULT=F1:node4@0 DURATION=2s STUB_BENCH_SECS=1
contains "itest no leader line warns" "$it_err" "no 'entering leader state' line"

# V: ATTEST_EVERY > 0 keeps the "core: diverged" lines of the node logs.
itest diverged EXP=it_div FAULT=F4:node4@0 ATTEST_EVERY=1 DURATION=2s STUB_BENCH_SECS=1 STUB_LOG_NODE=node4 STUB_LOG_LINE="2026-11-07T10:00:01.450000000Z core: diverged at height 12 (node 4)"
eq "itest diverged rc" "$it_rc" 0
grep -q '^node4 node4 | 2026-11-07T10:00:01.450000000Z core: diverged at height 12' "$(it_tag it_div .diverged)" 2>/dev/null && ok || bad "itest diverged file: $(cat "$(it_tag it_div .diverged)" 2>/dev/null) $it_err"
eq "itest diverged: only node4" "$(wc -l <"$(it_tag it_div .diverged)" | tr -d ' ')" 1
itest nodiverged EXP=it_nodiv FAULT=none DURATION=2s STUB_BENCH_SECS=1
eq "itest attest off: no .diverged" "$(it_tag it_nodiv .diverged)" ""

# W (phase 3): the accountability sidecars and the validity gate.
itest evict EXP=it_evict FAULT=F6:node4@0 ATTEST_EVERY=1 DURATION=2s STUB_BENCH_SECS=1 STUB_CONFIRM=readlie STUB_LOG_NODE=node2 \
  STUB_LOG_LINE="2026-11-09T10:00:01.250000000Z core: evicted node 4 at height 812 (bad_read)"
eq "itest evict rc" "$it_rc" 0
grep -q '^node2 node2 | 2026-11-09T10:00:01.250000000Z core: evicted node 4 at height 812 (bad_read)$' "$(it_tag it_evict .evict)" 2>/dev/null && ok ||
  bad "itest evict file: $(cat "$(it_tag it_evict .evict)" 2>/dev/null) $it_err"
eq "itest evict: .certs exists and is empty" "$(wc -c <"$(it_tag it_evict .certs)" 2>/dev/null | tr -d ' ')" 0
itest certs EXP=it_certs FAULT=F4:node4@0 ATTEST_EVERY=1 DURATION=2s STUB_BENCH_SECS=1 STUB_LOG_NODE=node1 \
  STUB_LOG_LINE="2026-11-09T10:00:02.000000000Z core: certified height 812 root 0a1b"
grep -q '^node1 node1 | .*core: certified height 812 root 0a1b$' "$(it_tag it_certs .certs)" 2>/dev/null && ok || bad "itest certs file: $(cat "$(it_tag it_certs .certs)" 2>/dev/null)"
itest evictgate EXP=it_evgate FAULT=none ATTEST_EVERY=1 DURATION=2s STUB_BENCH_SECS=1 STUB_LOG_NODE=node3 \
  STUB_LOG_LINE="2026-11-09T10:00:01.000000000Z core: evicted node 2 at height 9 (divergent_root)"
eq "itest fault-free run with an eviction is invalid" "$it_rc" 3
contains "itest eviction gate message" "$it_err" "invalid run: 1 eviction(s) without a fault"
itest rejgate EXP=it_rejgate FAULT=none ATTEST_EVERY=1 READ_MODE=certified DURATION=2s STUB_BENCH_SECS=1 STUB_REJECTED=3
eq "itest fault-free run with rejected reads is invalid" "$it_rc" 3
contains "itest rejected-reads gate message" "$it_err" "invalid run: 3 read(s) rejected by the client without a fault"
itest rejok EXP=it_rejok FAULT=none ATTEST_EVERY=1 READ_MODE=certified DURATION=2s STUB_BENCH_SECS=1 STUB_REJECTED=0
eq "itest rejected_reads=0 passes" "$it_rc" 0
itest p3flags EXP=it_p3 ATTEST_EVERY=1 ATTEST_QUORUM=f+1 READ_MODE=certified FAILOVER=1 SIGN_READS=always DURATION=2s STUB_BENCH_SECS=1
eq "itest phase 3 knobs rc" "$it_rc" 0
grep -q -- 'ycsbbench .* --read-mode=certified .*--failover --sign-reads=always --attest-quorum=2' "$SB/state/calls" && ok || bad "itest: ycsbbench --failover/--sign-reads/--attest-quorum: $(grep ycsbbench "$SB/state/calls" | head -n 1)"
eq "itest ATTEST_QUORUM=f+1 reaches up.sh as 2" "$(cat "$SB/state/attest_quorum" 2>/dev/null)" 2
itest p3default EXP=it_p3d DURATION=2s STUB_BENCH_SECS=1
grep -q -- '--failover\|--sign-reads\|--attest-quorum' "$SB/state/calls" && bad "itest: phase 3 bench flags without FAILOVER/SIGN_READS/ATTEST_QUORUM" || ok
eq "itest default ATTEST_QUORUM reaches up.sh as 0" "$(cat "$SB/state/attest_quorum" 2>/dev/null)" 0

# Host guard: a quiet host gets a host-runs.csv line `ok` and a row; the
# Docker VM's own CPU does not count.
itest hostok EXP=it_hostok DURATION=2s STUB_BENCH_SECS=1 HOST_SAMPLE_SECS=0
eq "itest host guard quiet host rc" "$it_rc" 0
eq "itest host guard quiet host verdict" "$(tail -n 1 "$SB_RAW/it_hostok/host-runs.csv" 2>/dev/null | cut -d, -f4-7)" "50.0,50.0,0,ok"
eq "itest host guard quiet host row" "$(tail -n +2 "$SB_RAW/it_hostok.csv" 2>/dev/null | wc -l | tr -d ' ')" 1
eq "itest host guard samples header" "$(head -n 1 "$(it_tag it_hostok .host)" 2>/dev/null)" "epoch_ms,load1,other_cpu_pct,swapouts"
# A busy host: refused before the lock and before any docker call (exit 4).
itest hostbusy EXP=it_hostbusy DURATION=2s STUB_BENCH_SECS=1 STUB_PS_CPU=350.0 HOST_BUSY_WAIT_SECS=0
eq "itest host guard busy host rc" "$it_rc" 4
contains "itest host guard busy host message" "$it_err" "refusing to run (exit 4)"
eq "itest host guard busy host touched no docker" "$(cat "$SB/state/calls" 2>/dev/null)" ""
eq "itest host guard busy host lock free" "$(ls -d "$SB_RAW/.hbdb.lock" 2>/dev/null)" ""
# Swapping during the run: noisy, no row (exit 3), whatever the FAULT.
itest hostswap EXP=it_hostswap DURATION=2s STUB_BENCH_SECS=1 STUB_SWAP_STEP=5000 HOST_SAMPLE_SECS=0
eq "itest host guard swapping rc" "$it_rc" 3
eq "itest host guard swapping verdict" "$(tail -n 1 "$SB_RAW/it_hostswap/host-runs.csv" 2>/dev/null | cut -d, -f7)" "noisy"
eq "itest host guard swapping no row" "$(cat "$SB_RAW/it_hostswap.csv" 2>/dev/null | wc -l | tr -d ' ')" 0
# HOST_GUARD=0: none of it.
itest hostoff EXP=it_hostoff DURATION=2s STUB_BENCH_SECS=1 STUB_PS_CPU=350.0 HOST_GUARD=0
eq "itest host guard off rc" "$it_rc" 0
eq "itest host guard off no host-runs.csv" "$(ls "$SB_RAW/it_hostoff/host-runs.csv" 2>/dev/null)" ""

# up.sh turns NODE_FAULTS into gen_compose.py --node-fault flags.
grep -q 'node_fault_args="$node_fault_args --node-fault $nf"' "$HERE/cluster/up.sh" && ok || bad "up.sh --node-fault"
grep -q -- '--kafka-mem "$KAFKA_MEM" $core_args $node_fault_args' "$HERE/cluster/up.sh" && ok || bad "up.sh passes node_fault_args"

# The recorded fault value must be CSV/--tags safe and the tag file-name safe.
grep -q 'fault_rec="$(echo "$FAULT" | tr '"','"' '"'+'"')"' "$HERE/run_one.sh" && ok || bad "run_one fault_rec"
grep -q 'fault=$fault_rec,' "$HERE/run_one.sh" && ok || bad "run_one tags use fault_rec"

echo "test_faults.sh: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
