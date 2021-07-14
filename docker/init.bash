#!/bin/bash

# move to wherever we are so docker things work
cd "$(dirname "${BASH_SOURCE[0]}")"
cd ..

set -exo pipefail

docker-compose build
docker-compose -f docker/util.yml build
docker-compose up -d trino
docker-compose -f docker/util.yml run --rm util wait_for_up trino 8080
