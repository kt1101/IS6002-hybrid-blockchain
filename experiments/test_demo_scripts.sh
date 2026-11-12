#!/usr/bin/env bash
# Tests for experiments/demo_up.sh and experiments/demo_fault.sh. No Docker
# needed: both run with DEMO_DRY_RUN=1, which prints the docker commands.
#
#   experiments/test_demo_scripts.sh        (prints "ok", exit 0 = all passed)
#
# Bash 3.2 compatible, like the scripts it tests.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UP="$HERE/demo_up.sh"
FAULT="$HERE/demo_fault.sh"
pass=0
fail=0

ok() { pass=$((pass + 1)); }
bad() {
  fail=$((fail + 1))
  echo "FAIL: $*" >&2
}
eq() {
  if [ "$2" = "$3" ]; then ok; else bad "$1: got '$2', want '$3'"; fi
}
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

# fault BACKEND ARGS...: run demo_fault.sh dry; sets OUT (stdout+stderr) and RC.
fault() {
  local backend="$1"
  shift
  OUT="$(DEMO_DRY_RUN=1 HBDB_BACKEND="$backend" bash "$FAULT" "$@" 2>&1)"
  RC=$?
}

bash -n "$UP" && ok || bad "bash -n demo_up.sh"
bash -n "$FAULT" && ok || bad "bash -n demo_fault.sh"

# ---- demo_fault.sh ----
fault bft2 crash node3
eq "crash rc" "$RC" 0
contains "crash" "$OUT" "docker stop"
contains "crash names the node's container" "$OUT" "node3"

fault bft2 silent node3
eq "silent rc" "$RC" 0
contains "silent iptables" "$OUT" "iptables"
contains "silent port 7200" "$OUT" "7200"
contains "silent drops" "$OUT" "-j DROP"

fault raft silent node2
contains "raft silent ports" "$OUT" "7100"
contains "raft silent forward port" "$OUT" "7101"

fault tendermint silent node2
contains "tendermint silent port" "$OUT" "26656"

fault bft2 heal node3
eq "heal rc" "$RC" 0
contains "heal deletes the rules" "$OUT" "-D OUTPUT"
contains "heal port" "$OUT" "7200"
contains "heal resets netem" "$OUT" "tc qdisc del dev eth0 root"

fault bft2 slow node1
eq "slow rc" "$RC" 0
contains "slow netem" "$OUT" "netem delay 200ms"

OUT="$(DEMO_DRY_RUN=1 HBDB_BACKEND=bft2 NET_BASE_DELAY_MS=5 bash "$FAULT" heal node1 2>&1)"
contains "heal restores the base delay" "$OUT" "netem delay 5ms"

fault bft2 corrupt node3
eq "corrupt armed rc" "$RC" 0
contains "corrupt armed signals" "$OUT" "kill -USR1"

fault bft2 corrupt node2
eq "corrupt unarmed rc" "$RC" 4
contains "corrupt unarmed message" "$OUT" "not started with --fault=corrupt"
lacks "corrupt unarmed runs nothing" "$OUT" "kill -USR1"

OUT="$(DEMO_DRY_RUN=1 HBDB_BACKEND=bft2 DEMO_ARMED_CORRUPT=node2,node3 bash "$FAULT" corrupt node2 2>&1)"
eq "armed list rc" "$?" 0

fault kafka silent node4
eq "kafka silent armed rc" "$RC" 0
contains "kafka silent signals" "$OUT" "kill -USR1"
lacks "kafka silent uses no iptables" "$OUT" "iptables -w -A"

fault kafka silent node2
eq "kafka silent unarmed rc" "$RC" 4

fault kafka heal node4
eq "kafka heal rc" "$RC" 0
lacks "kafka heal has no consensus ports" "$OUT" "-D OUTPUT"
contains "kafka heal resets netem" "$OUT" "tc qdisc del"

fault kafka-orig corrupt node3
eq "kafka-orig corrupt rc" "$RC" 4
fault tm-orig silent node3
eq "tm-orig silent rc" "$RC" 4

fault bft2 explode node3
eq "unknown action rc" "$RC" 2
fault bft2 crash
eq "missing node rc" "$RC" 2
fault bft2 crash host3
eq "bad node rc" "$RC" 2
OUT="$(DEMO_DRY_RUN=1 bash "$FAULT" crash node3 2>&1 </dev/null)"
RC=$?
eq "no backend rc" "$RC" 1

# ---- demo_up.sh ----
OUT="$(DEMO_DRY_RUN=1 BACKEND=bft2 N=4 bash "$UP" 2>&1)"
eq "up rc" "$?" 0
contains "up publishes ports" "$OUT" "PUBLISH_PORTS=1"
contains "up commit mode" "$OUT" "COMMIT_MODE=sync"
contains "up batch flush" "$OUT" "BATCH_FLUSH=200ms"
contains "up arms corrupt" "$OUT" 'NODE_FAULTS="node3=corrupt"'
contains "up runs up.sh" "$OUT" "cluster/up.sh"
contains "up runs health.sh" "$OUT" "cluster/health.sh"
contains "up prints the demo command" "$OUT" "go run ./cmd/demo --mode=docker --backend=bft2 --n=4"

OUT="$(DEMO_DRY_RUN=1 BACKEND=kafka N=4 bash "$UP" 2>&1)"
contains "kafka arms silent too" "$OUT" 'NODE_FAULTS="node3=corrupt node4=silent"'

OUT="$(DEMO_DRY_RUN=1 BACKEND=raft N=2 bash "$UP" 2>&1)"
contains "small N arms nothing" "$OUT" 'NODE_FAULTS=""'
contains "small N tells the demo" "$OUT" "--armed-corrupt=none"

OUT="$(DEMO_DRY_RUN=1 bash "$UP" 2>&1)"
eq "up without BACKEND rc" "$?" 1
OUT="$(DEMO_DRY_RUN=1 BACKEND=kafka-orig bash "$UP" 2>&1)"
eq "up kafka-orig rc" "$?" 1

if [ "$fail" -ne 0 ]; then
  echo "$fail failed, $pass passed" >&2
  exit 1
fi
echo ok
