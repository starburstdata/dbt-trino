#!/bin/bash

# move to wherever we are so docker things work
cd "$(dirname "${BASH_SOURCE[0]}")"

set -exo pipefail
docker run \
    --network="dbt-net" \
    -v $PWD/dbt:/root/.dbt \
    dbt-presto \
    sh -c "dbt init test && cd test && dbt run"
