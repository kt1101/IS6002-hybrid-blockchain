#!/usr/bin/env bash
# Inject or heal one fault on a node of the live demo's docker cluster
# (spec docs/superpowers/specs/2026-11-02-live-demo-design.md 5.2). Run by
# cmd/demo (demo/cluster/docker.go); also usable by hand:
#
#   HBDB_BACKEND=bft2 experiments/demo_fault.sh <action> <nodeX>
#
#   crash    docker stop the node (fault_crash); cannot be healed
#   silent   kafka: SIGUSR1 to the node's veritas, which must have been armed
#            with --fault=silent (demo_up.sh arms node4); cannot be healed.
#            raft, tendermint, bft2: iptables DROP of the consensus ports
#            (fault_silent_net)
#   slow     netem delay FAULT_SLOW_MS (200) ms on the node's egress
#   corrupt  SIGUSR1 to the node's veritas, which must have been armed with
#            --fault=corrupt (demo_up.sh arms node3); cannot be healed
#   heal     remove the iptables DROP rules silent added (the same rules with
#            -D) and reset the netem delay to NET_BASE_DELAY_MS (0 = delete
#            the qdisc)
#
# Faults act at once (AT=0 of faults.sh: its start time defaults to now).
# Env: HBDB_BACKEND (required), DEMO_ARMED_CORRUPT (nodes armed for corrupt,
# default node3), DEMO_ARMED_SILENT (kafka: default node4), both
# comma-separated, FAULT_SLOW_MS=200, NET_BASE_DELAY_MS=0,
# DEMO_DRY_RUN=1 (print the docker commands instead of running them).
# Exit codes: 0 done, 1 failed, 2 usage, 4 not supported (message on stderr).
# Bash 3.2 compatible (runs on the macOS host).
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/experiments/lib.sh"
. "$HBDB_ROOT/experiments/faults.sh"
# Faults act now and are not logged: an inherited environment must not delay,
# abort or log them (faults.sh reads these globals).
FAULT_T0_MS=""
FAULT_STOP=""
FAULT_PARENT_PID=""
FAULT_LOG=""
FAULT_STOPPED=""

usage() {
  echo "usage: HBDB_BACKEND=<backend> $HBDB_SCRIPT <crash|silent|slow|corrupt|heal> <nodeX>" >&2
  exit 2
}

# unsupported MSG: exit 4, MSG as the last stderr line (the demo shows it).
unsupported() {
  echo "$HBDB_SCRIPT: $*" >&2
  exit 4
}

[ "$#" -eq 2 ] || usage
action="$1"
node="$2"
case "$action" in
  crash | silent | slow | corrupt | heal) ;;
  *) usage ;;
esac
echo "$node" | grep -Eq '^node[1-9][0-9]*$' || usage
BACKEND="${HBDB_BACKEND:-}"
[ -n "$BACKEND" ] || die "HBDB_BACKEND is required (one of: $HBDB_ALL_BACKENDS)"
hbdb_in_list "$BACKEND" "$HBDB_ALL_BACKENDS" || die "unknown HBDB_BACKEND '$BACKEND' (one of: $HBDB_ALL_BACKENDS)"
FAULT_BACKEND="$BACKEND"

ARMED_CORRUPT="${DEMO_ARMED_CORRUPT-node3}"
if [ -n "${DEMO_ARMED_SILENT+x}" ]; then
  ARMED_SILENT="$DEMO_ARMED_SILENT"
elif [ "$BACKEND" = kafka ]; then
  ARMED_SILENT=node4
else
  ARMED_SILENT=""
fi

# DEMO_DRY_RUN=1: docker and docker compose print what they would run (on the
# script's original stdout, which the callers' >/dev/null do not reach).
DRY_POLICY=""
if [ "${DEMO_DRY_RUN:-}" = 1 ]; then
  exec 3>&1
  docker() { echo "docker $*" >&3; }
  hbdb_compose() {
    case "$1" in
      ps) echo "dry-run-$3" ;; # container id lookup
      *)
        # fault_confirm reads the active file: report the policy as active.
        if [ "$1" = exec ] && [ "$4" = sh ] && [ "$5" = -c ] && [ "$6" = "$(fault_confirm_script)" ]; then
          echo "$DRY_POLICY"
          return 0
        fi
        echo "docker compose -p $HBDB_PROJECT $*" >&3
        ;;
    esac
  }
fi

# in_csv NODE CSV
in_csv() {
  case ",$2," in
    *",$1,"*) return 0 ;;
    *) return 1 ;;
  esac
}

# fault_silent_heal_script PORTS: the shell script run in the node container:
# the rules fault_silent_net_script adds, deleted (-D) until none is left;
# missing rules are not an error.
fault_silent_heal_script() {
  local p
  printf '%s\n' 'ipt=iptables' 'iptables -w -L OUTPUT -n >/dev/null 2>&1 || ipt=iptables-legacy'
  for p in $1; do
    printf '%s\n' "for d in --dport --sport; do while \$ipt -w -D OUTPUT -p tcp \$d $p -j DROP 2>/dev/null; do :; done; done"
  done
  printf '%s\n' 'true'
}

# policy_fault POLICY ARMED: arm check, then fault_signal now.
policy_fault() {
  hbdb_backend_has_fault_policy "$BACKEND" ||
    unsupported "$1 is not available on BACKEND=$BACKEND (no veritas --fault)"
  in_csv "$node" "$2" ||
    unsupported "$1 is not available on $node: it was not started with --fault=$1 (armed: ${2:-none}); restart with experiments/demo_up.sh"
  DRY_POLICY="$1"
  FAULT_KIND="$action"
  fault_signal "$node" 0 "$1"
}

run() {
  case "$action" in
    crash)
      fault_crash "$node" 0
      ;;
    slow)
      fault_slow "$node" 0 "$FAULT_SLOW_MS"
      ;;
    corrupt)
      policy_fault corrupt "$ARMED_CORRUPT"
      ;;
    silent)
      if [ "$BACKEND" = kafka ]; then
        policy_fault silent "$ARMED_SILENT"
      else
        local ports
        ports="$(hbdb_consensus_ports "$BACKEND")"
        [ -n "$ports" ] || unsupported "silent is not available on BACKEND=$BACKEND (no consensus ports to drop)"
        fault_silent_net "$node" 0 "$ports"
      fi
      ;;
    heal)
      local ports rc=0
      if [ -z "$(hbdb_compose ps -q "$node" 2>/dev/null)" ]; then
        unsupported "heal is not available on $node: it is not running (a crashed node stays down in docker mode)"
      fi
      ports="$(hbdb_consensus_ports "$BACKEND")"
      if [ -n "$ports" ]; then
        log "heal: removing the consensus-port DROP rules ($ports) on $node"
        hbdb_compose exec -T "$node" sh -c "$(fault_silent_heal_script "$ports")" >/dev/null || rc=1
      fi
      log "heal: netem delay on $node back to ${NET_BASE_DELAY_MS}ms"
      net_delay "$node" "$NET_BASE_DELAY_MS" || rc=1
      return "$rc"
      ;;
  esac
}

rc=0
run || rc=$?
[ "$rc" -eq 0 ] || die "$action $node failed (rc=$rc)"
