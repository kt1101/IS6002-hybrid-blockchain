# shellcheck shell=bash
# Network delay (netem) on the node containers: E5 RTT and the F3 slow node.
# Source it after lib.sh, do not execute it:
#   . "$HBDB_ROOT/experiments/net.sh"
# Bash 3.2 compatible (runs on the macOS host).
#
# Every node container has iproute2 (docker/Dockerfile.node) and NET_ADMIN
# (gen_compose.py cap_add), so `tc` runs in it via `docker compose exec`.
# The qdisc sits on the egress of the container's only interface (eth0 on
# network hbdb); loopback (redis, the tendermint <-> ABCI socket) is not
# delayed. Everything lives in the container's network namespace, so it is
# gone when down.sh removes the containers (every run starts a new cluster).
#
#   net_set_rtt RTT_MS   netem delay RTT_MS/2 on the egress of node1..node$N:
#                        node <-> node round trips get RTT_MS (RTT_MS/2 each
#                        way); client/TSO/kafka <-> node get RTT_MS/2, since
#                        only the nodes are delayed.
#   net_delay NODE MS    set NODE's egress delay to MS ms (replace: works with
#                        or without an earlier qdisc, so it is idempotent);
#                        MS=0 removes the qdisc.
#
# NET_BASE_DELAY_MS: the delay net_set_rtt put on every node (0 = none);
# fault_slow (faults.sh) adds its delay on top of it.

NET_DEV="${HBDB_NET_DEV:-eth0}"
NET_BASE_DELAY_MS="${NET_BASE_DELAY_MS:-0}"

# net_half_ms RTT_MS: RTT_MS / 2 in ms, as tc accepts it (10, 10.5).
net_half_ms() {
  awk -v r="$1" 'BEGIN { printf "%g\n", r / 2 }'
}

# net_add_ms A B: A + B (ms, possibly fractional).
net_add_ms() {
  awk -v a="$1" -v b="$2" 'BEGIN { printf "%g\n", a + b }'
}

net_delay() {
  local node="$1" ms="$2"
  case "$ms" in
    '' | *[!0-9.]* | *.*.*) die "net_delay: delay must be a number of ms, got '$ms'" ;;
  esac
  if awk -v m="$ms" 'BEGIN { exit !(m == 0) }'; then
    # No delay: drop our qdisc if there is one (none is fine).
    hbdb_compose exec -T "$node" sh -c "tc qdisc del dev $NET_DEV root 2>/dev/null || true"
  else
    hbdb_compose exec -T "$node" tc qdisc replace dev "$NET_DEV" root netem delay "${ms}ms"
  fi
}

net_set_rtt() {
  local rtt="$1" half i
  hbdb_require_uint RTT_MS "$rtt"
  hbdb_check_n "${N:-}"
  half="$(net_half_ms "$rtt")"
  i=1
  while [ "$i" -le "$N" ]; do
    net_delay "node$i" "$half" || { log "net_set_rtt: tc failed on node$i"; return 1; }
    i=$((i + 1))
  done
  NET_BASE_DELAY_MS="$half"
  log "netem: egress delay ${half}ms on node1..node$N (RTT_MS=$rtt)"
}
