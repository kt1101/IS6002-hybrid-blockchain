#!/usr/bin/env bash
# PRE_DOWN_HOOK of E2 (run_one.sh runs it after a successful run, before
# down.sh): read the Kafka counters of the shared log and append one row to
# results/raw/e2-kafka-ops.csv (the Kafka read/write operation counts of the
# paper's Figure 8).
#
# Same logic as scripts/get_kafka_ops.sh, on the live broker instead of a
# kafka-counters.log: for every node's consumer group (group = the node's
# signature, node<i>) `kafka-consumer-groups.sh --describe` prints one line
# for the topic shared-log,
#     GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
# CURRENT-OFFSET (field 4) = messages the node has consumed = its Kafka reads,
# LOG-END-OFFSET (field 5) = messages in the topic = Kafka writes (the same
# for every group; the original script averages it over the nodes, so do we).
#
# CSV (header written on the first row):
#   ts,exp,backend,n,clients,rep,reads,writes,current_offsets,log_end_offsets
# reads = sum of the CURRENT-OFFSETs over the N nodes (all nodes read every
# message), writes = mean of the LOG-END-OFFSETs (2 decimals);
# current_offsets / log_end_offsets are the per-node values, '+'-joined.
# A group that has committed no offset yet shows '-' in kafka-consumer-groups:
# the row is still written, with '-' in current_offsets for that node and an
# EMPTY reads (unknown, not 0), and the hook logs a warning.
# The counters cover the whole life of the cluster (a fresh one per run): the
# load phase, settle, warmup and measurement, not only the measurement window.
# Consumers commit their offsets every 5 s (auto-commit), so the hook first
# waits KAFKA_OPS_WAIT seconds (default 6) for the last commit.
#
# Env from run_one.sh: EXP BACKEND N CLIENTS REP (TAG). Own env:
#   KAFKA_OPS_CSV   output file (default results/raw/e2-kafka-ops.csv)
#   KAFKA_OPS_WAIT  seconds to wait before reading (default 6)
# Does nothing for backends other than kafka. Exit 1 on any failure (run_one.sh
# only warns: the run's CSV row stands); no partial row is written.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

KAFKA_OPS_CSV="${KAFKA_OPS_CSV:-$HBDB_RESULTS_RAW/e2-kafka-ops.csv}"
KAFKA_OPS_WAIT="${KAFKA_OPS_WAIT:-6}"
KAFKA_OPS_HEADER="ts,exp,backend,n,clients,rep,reads,writes,current_offsets,log_end_offsets"
KAFKA_TOPIC=shared-log

[ "${BACKEND:-}" = kafka ] || exit 0
hbdb_require_uint N "${N:-}"
hbdb_require_uint CLIENTS "${CLIENTS:-}"
hbdb_require_uint REP "${REP:-}"
hbdb_require_uint KAFKA_OPS_WAIT "$KAFKA_OPS_WAIT"
[ -n "${EXP:-}" ] || die "EXP is not set"

# parse_group TOPIC: read kafka-consumer-groups --describe output on stdin and
# print "<current> <log-end>" of the line for TOPIC (a '-' current offset, i.e.
# nothing committed yet, stays '-'); fail if there is no such line or a value
# is not a number.
parse_group() {
  awk -v t="$1" '
    $2 == t && !seen {
      cur = $4; end = $5
      if ((cur != "-" && cur !~ /^[0-9]+$/) || end !~ /^[0-9]+$/) exit 2
      print cur, end
      seen = 1
    }
    END { if (!seen) exit 1 }
  '
}

sleep "$KAFKA_OPS_WAIT"

cur_list=""
end_list=""
reads=0
unknown=""
writes_sum=0
i=1
while [ "$i" -le "$N" ]; do
  # The describe tool lives in the kafka service (working dir /opt/kafka).
  out="$(hbdb_compose exec -T kafka sh -c \
    'KAFKA_HEAP_OPTS=-Xmx128m exec /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group "$1"' \
    sh "node$i")" || die "kafka-consumer-groups --describe --group node$i failed"
  pair="$(printf '%s\n' "$out" | parse_group "$KAFKA_TOPIC")" ||
    die "no usable $KAFKA_TOPIC line for group node$i in: $(printf '%s' "$out" | tr '\n' '|')"
  cur="${pair% *}"
  end="${pair#* }"
  cur_list="${cur_list:+$cur_list+}$cur"
  end_list="${end_list:+$end_list+}$end"
  if [ "$cur" = "-" ]; then
    unknown="${unknown:+$unknown }node$i"
  else
    reads=$((reads + cur))
  fi
  writes_sum=$((writes_sum + end))
  i=$((i + 1))
done
if [ -n "$unknown" ]; then
  log "warning: no committed offset yet for $unknown: reads left empty (unknown) in the row"
  reads=""
fi
writes="$(awk -v s="$writes_sum" -v n="$N" 'BEGIN { printf "%.2f", s / n }')"

mkdir -p "$(dirname "$KAFKA_OPS_CSV")"
if [ -s "$KAFKA_OPS_CSV" ]; then
  [ "$(head -n 1 "$KAFKA_OPS_CSV")" = "$KAFKA_OPS_HEADER" ] || die "$KAFKA_OPS_CSV has a different header; refusing to append"
else
  echo "$KAFKA_OPS_HEADER" >"$KAFKA_OPS_CSV"
fi
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$EXP,$BACKEND,$N,$CLIENTS,$REP,$reads,$writes,$cur_list,$end_list" >>"$KAFKA_OPS_CSV"
log "kafka ops: $EXP n=$N clients=$CLIENTS rep=$REP reads=${reads:-unknown} writes=$writes -> $KAFKA_OPS_CSV"
