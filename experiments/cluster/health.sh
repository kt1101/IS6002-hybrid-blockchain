#!/usr/bin/env bash
# Wait until the running cluster serves requests. Exit 0 when every probe
# passed within HEALTH_TIMEOUT seconds (overall), 1 otherwise.
#
# Env: BACKEND (required), N=4, HEALTH_TIMEOUT=60.
# Probes run from the client container (cmd/hbdbprobe), in parallel within a
# phase, each retrying until the remaining time is used up:
#   kafka-orig: phase 1  Verify("health") on every node (veritas/server.go
#                        Verify works: it returns a sparse-Merkle proof, also
#                        for absent keys);
#               phase 2  one Set+Get round trip through node1 + TSO (a block
#                        only commits once every node approved it, so this
#                        proves the whole ordering/commit pipeline).
#               The phases are sequential because the server's Merkle tree is
#               not synchronized: a Verify racing with a commit could crash it.
#   tm-orig:    phase 2 only: Set+Get on every node (veritastm Verify returns
#               nil,nil, a gRPC marshal error, so it cannot be used).
#   kafka:      phase 1  Verify("health") on every node (core.Server Verify
#                        takes root and proof under the executor lock, so it
#                        is safe at any time; it is what the auditor calls);
#               phase 2  Set+Get through EVERY node + TSO (client versions):
#                        each Set is a block proposed by that node, so this
#                        checks every node's producer, consumer and vote path
#                        (a block needs every node's approve read back from the
#                        topic) and, in sync mode, that Set returns only once
#                        the block is applied on the serving node.
#   raft:       as kafka: phase 1 Verify on every node, phase 2 Set+Get
#               through EVERY node + TSO; a follower's Set is forwarded to
#               the leader, so this also waits for a leader election and
#               checks every node's forward path and raft apply.
#   tendermint: phase 1 Verify on every node (core.Server, as kafka), phase 2
#               Set+Get through EVERY node WITHOUT a TSO (--version-mode=
#               ordered); each Set is a block broadcast to that node's
#               tendermint, so this also waits for tendermint to produce
#               blocks and checks every node's RPC, mempool and ABCI path.
#   bft2:       as tendermint (Verify on every node, then Set+Get through
#               EVERY node without a TSO); each Set is a block submitted to
#               that node's bft2-node, which proposes it in its next view as
#               HotStuff leader, so this waits for the replicas to connect
#               and checks every node's local API, proposal and commit path.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

N="${N:-4}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-60}"
hbdb_check_backend
hbdb_check_n "$N"
hbdb_require_uint HEALTH_TIMEOUT "$HEALTH_TIMEOUT"
[ -f "$HBDB_COMPOSE_FILE" ] || die "no compose file at $HBDB_COMPOSE_FILE (run up.sh first)"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/hbdb-health.XXXXXX")"
pids=""
names=""
trap 'for p in $pids; do kill "$p" 2>/dev/null || true; done; rm -rf "$tmp"' EXIT

start=$SECONDS
failed=""

# remaining: seconds left of HEALTH_TIMEOUT (at least 1, so a late phase
# still gets one attempt; the overall budget is then exceeded by <= 1 s).
remaining() {
  local r=$((HEALTH_TIMEOUT - (SECONDS - start)))
  [ "$r" -ge 1 ] || r=1
  echo "$r"
}

# probe NAME TIMEOUT ARGS... : start one hbdbprobe in the background.
probe() {
  local name="$1" t="$2"
  shift 2
  hbdb_compose exec -T client hbdbprobe --timeout="${t}s" "$@" >"$tmp/$name.log" 2>&1 &
  pids="$pids $!"
  names="$names $name"
}

# wait_all: wait for the probes started since the last call; record failures.
wait_all() {
  local p name
  # shellcheck disable=SC2086
  set -- $names
  for p in $pids; do
    name="$1"
    shift
    if wait "$p"; then
      log "ok: $name"
    else
      failed="$failed $name"
      log "FAILED: $name"
      sed 's/^/    /' "$tmp/$name.log" | tail -n 20 >&2
    fi
  done
  pids=""
  names=""
}

fail() {
  log "cluster not healthy within ${HEALTH_TIMEOUT}s:$failed"
  hbdb_compose ps -a >&2 || true
  exit 1
}

log "probing $BACKEND cluster (N=$N, timeout ${HEALTH_TIMEOUT}s)"

if [ "$BACKEND" = kafka-orig ] || [ "$BACKEND" = kafka ] || [ "$BACKEND" = raft ] || [ "$BACKEND" = tendermint ] || [ "$BACKEND" = bft2 ]; then
  t="$(remaining)"
  i=1
  while [ "$i" -le "$N" ]; do
    probe "node$i-verify" "$t" --addr="$(hbdb_node_ip "$i"):$HBDB_VERITAS_PORT" --mode=verify
    i=$((i + 1))
  done
  wait_all
  [ -z "$failed" ] || fail
fi

t="$(remaining)"
case "$BACKEND" in
  kafka-orig)
    probe "node1-setget" "$t" --addr="$(hbdb_node_ip 1):$HBDB_VERITAS_PORT" --mode=setget --tso-addr="$HBDB_TSO_ADDR"
    ;;
  tm-orig | tendermint | bft2)
    i=1
    while [ "$i" -le "$N" ]; do
      probe "node$i-setget" "$t" --addr="$(hbdb_node_ip "$i"):$HBDB_VERITAS_PORT" --mode=setget
      i=$((i + 1))
    done
    ;;
  kafka | raft)
    i=1
    while [ "$i" -le "$N" ]; do
      probe "node$i-setget" "$t" --addr="$(hbdb_node_ip "$i"):$HBDB_VERITAS_PORT" --mode=setget --tso-addr="$HBDB_TSO_ADDR"
      i=$((i + 1))
    done
    ;;
esac
wait_all
[ -z "$failed" ] || fail

log "cluster healthy after $((SECONDS - start))s"
