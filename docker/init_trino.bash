#!/bin/bash

# move to wherever we are so docker things work
cd "$(dirname "${BASH_SOURCE[0]}")"
cd ..

set -exo pipefail

docker-compose -f docker-compose-trino.yml build
docker-compose -f docker-compose-trino.yml up -d
timeout 5m bash -c -- 'while ! docker-compose -f docker-compose-trino.yml logs trino 2>&1 | tail -n 1 | grep "SERVER STARTED"; do sleep 2; done'
