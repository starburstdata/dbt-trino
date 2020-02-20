#!/bin/bash

# move to wherever we are so docker things work
cd "$(dirname "${BASH_SOURCE[0]}")"

set -exo pipefail

docker-compose build
docker-compose -f util.yml build
docker-compose up -d presto
docker-compose -f util.yml run --rm util wait_for_up presto 8080