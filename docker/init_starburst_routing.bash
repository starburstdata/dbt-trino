#!/bin/bash

# move to wherever we are so docker things work
cd "$(dirname "${BASH_SOURCE[0]}")"
cd ..

set -exo pipefail

docker network create dbt-net || true
docker compose -f docker-compose-starburst-routing.yml up -d --quiet-pull
timeout 5m bash -c -- 'while ! docker compose -f docker-compose-starburst-routing.yml logs sep1 sep2 2>&1 | grep -c "SERVER STARTED" | grep -q "^2$"; do sleep 2; done'

# sep1/sep2 logging SERVER STARTED does not mean the portal is up - it is a
# separate container (e.g. it fails fast if it has no license for the
# gateway module) and dbt talks to it, not to sep1/sep2 directly.
timeout 5m bash -c -- '
  while true; do
    status=$(docker compose -f docker-compose-starburst-routing.yml ps -q portal | xargs -r docker inspect -f "{{.State.Status}}")
    if [ "$status" = "exited" ]; then
      echo "portal container exited; logs:" >&2
      docker compose -f docker-compose-starburst-routing.yml logs portal >&2
      exit 1
    fi
    curl -sf http://localhost:8090/status/livez >/dev/null 2>&1 && exit 0
    sleep 2
  done
'
