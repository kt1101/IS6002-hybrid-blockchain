#!/usr/bin/env bash
# Stop the cluster and delete its containers, network and volumes, so no data
# survives into the next run. Safe to call when nothing is running.
set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/lib.sh"

if [ -f "$HBDB_COMPOSE_FILE" ]; then
  log "docker compose down -v (project $HBDB_PROJECT)"
  hbdb_compose down -v --remove-orphans --timeout 5 >&2 || log "warning: compose down failed, falling back to label cleanup"
fi

# Leftovers from a run whose compose file was regenerated or removed: anything
# labelled with our project name.
label="label=com.docker.compose.project=$HBDB_PROJECT"
ids="$(docker ps -aq --filter "$label")"
if [ -n "$ids" ]; then
  log "removing leftover containers: $(echo $ids)"
  # shellcheck disable=SC2086
  docker rm -f -v $ids >/dev/null
fi
ids="$(docker volume ls -q --filter "$label")"
if [ -n "$ids" ]; then
  log "removing leftover volumes: $(echo $ids)"
  # shellcheck disable=SC2086
  docker volume rm $ids >/dev/null
fi
ids="$(docker network ls -q --filter "$label")"
if [ -n "$ids" ]; then
  log "removing leftover networks: $(echo $ids)"
  # shellcheck disable=SC2086
  docker network rm $ids >/dev/null
fi
log "cluster down"
