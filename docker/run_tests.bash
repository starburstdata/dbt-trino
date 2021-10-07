#!/bin/bash

# move to wherever we are so docker things work
cd "$(dirname "${BASH_SOURCE[0]}")"

set -exo pipefail
# run the dim_order_dates model two times in order to test incremental functionality
docker run \
    --network="dbt-net" \
    -v $PWD/dbt:/root/.dbt \
    dbt-trino \
    "cd /jaffle_shop \
        && dbt seed \
        && dbt run \
        && dbt run --model dim_order_dates \
        && dbt test"
