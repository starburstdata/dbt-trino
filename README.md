<p float="left">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" width="45%" />
  <img src="https://trino.io/assets/trino-og.png" width="50%" />
</p>

[![Build Status](https://github.com/starburstdata/dbt-trino/actions/workflows/ci.yml/badge.svg)](https://github.com/starburstdata/dbt-trino/actions/workflows/ci.yml?query=workflow%3A%22dbt-trino+tests%22+branch%3Amaster+event%3Apush) [![Trino Slack](https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358)](https://trino.io/slack.html)

# dbt-trino

## Introduction

[dbt](https://docs.getdbt.com/docs/introduction) is a data transformation workflow tool that lets teams quickly and collaboratively deploy analytics code, following software engineering best practices like modularity, CI/CD, testing, and documentation. It enables anyone who knows SQL to build production-grade data pipelines.

One frequently asked question in the context of using `dbt` tool is:

> Can I connect my dbt project to two databases?

(see the answered [question](https://docs.getdbt.com/faqs/connecting-to-two-dbs-not-allowed) on the dbt website).

**TL;DR** `dbt` stands for transformation as in `T` within `ELT` pipelines, it doesn't move data from source to a warehouse.

`dbt-trino` adapter uses [Trino](https://trino.io/) as a underlying query engine to perform query federation across disperse data sources. Trino connects to multiple and diverse data sources ([available connectors](https://trino.io/docs/current/connector.html)) via one dbt connection and process SQL queries at scale. Transformations defined in dbt are passed to Trino which handles these SQL transformation queries and translates them to queries specific to the systems it connects to create tables or views and manipulate data.

This repository represents a fork of the [dbt-presto](https://github.com/dbt-labs/dbt-presto) with adaptations to make it work with Trino.

### Compatibility

This dbt plugin has been tested against `Trino` version `366` and `Starburst Enterprise` version `365-e`.

## Installation

This dbt adapter can be installed via pip:

```sh
$ pip install dbt-trino
```

### Configuring your profile

A dbt profile can be configured to run against Trino using the following configuration:

| Option             | Description                                                                                                  | Required?                                                                          | Example                          |
| ------------------ | ------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- | -------------------------------- |
| method             | The Trino authentication method to use                                                                       | Optional (default is `none`)                                                       | `none` or `kerberos`             |
| user               | Username for authentication                                                                                  | Required                                                                           | `commander`                      |
| password           | Password for authentication                                                                                  | Optional (required if `method` is `ldap` or `kerberos`)                            | `none` or `abc123`               |
| http_headers       | HTTP Headers to send alongside requests to Trino, specified as a yaml dictionary of (header, value) pairs.   | Optional                                                                           | `X-Trino-Client-Info: dbt-trino` |
| http_scheme        | The HTTP scheme to use for requests to Trino                                                                 | Optional (default is `http`, or `https` for `method: kerberos` and `method: ldap`) | `https` or `http`                |
| session_properties | Sets Trino session properties used in the connection                                                         | Optional                                                                           | `query_max_run_time: 5d`         |
| database           | Specify the database to build models into                                                                    | Required                                                                           | `analytics`                      |
| schema             | Specify the schema to build models into. Note: it is not recommended to use upper or mixed case schema names | Required                                                                           | `public`                         |
| host               | The hostname to connect to                                                                                   | Required                                                                           | `127.0.0.1`                      |
| port               | The port to connect to the host on                                                                           | Required                                                                           | `8080`                           |
| threads            | How many threads dbt should use                                                                              | Optional (default is `1`)                                                          | `8`                              |

**Example profiles.yml entry:**

```yaml
my-trino-db:
  target: dev
  outputs:
    dev:
      type: trino
      user: commander
      host: 127.0.0.1
      port: 8080
      database: analytics
      schema: public
      threads: 8
      http_scheme: http
      session_properties:
        query_max_run_time: 5d
        exchange_compression: True
```

For reference on which session properties can be set on the the dbt profile do execute

```sql
SHOW SESSION;
```

on your Trino instance.

## Usage Notes

#### Supported Functionality

Due to the nature of Trino, not all core `dbt` functionality is supported.
The following features of dbt are not implemented in `dbt-trino`:

- Snapshot

Also, note that upper or mixed case schema names will cause catalog queries to fail.
Please only use lower case schema names with this adapter.

#### Required configuration

dbt fundamentally works by dropping and creating tables and views in databases.
As such, the following Trino configs must be set for dbt to work properly on Trino:

```properties
hive.metastore-cache-ttl=0s
hive.metastore-refresh-interval = 5s
hive.allow-drop-table=true
hive.allow-rename-table=true
```

#### Incremental models

The incremental strategy currently supported by this adapter is to append new records
without updating/overwriting any existing data from the target model.

```jinja2
{{
    config(materialized = 'incremental')
}}
```

#### Incremental overwrite on hive models

In case that the target incremental model is being accessed with
[hive](https://trino.io/docs/current/connector/hive.html) Trino connector, an `insert overwrite`
functionality can be achieved when using:

```
<hive-catalog-name>.insert-existing-partitions-behavior=OVERWRITE
```

setting on the Trino hive connector configuration.

Below is a sample hive profile entry to deal with `OVERWRITE` functionality for the hive connector called `minio`:

```yaml
trino-incremental-hive:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      user: admin
      password:
      catalog: minio
      schema: tiny
      host: localhost
      port: 8080
      http_scheme: http
      session_properties:
        minio.insert_existing_partitions_behavior: OVERWRITE
      threads: 1
```

Existing partitions in the target model that match the staged data will be overwritten.
The rest of the partitions will be simply appended to the target model.

NOTE that this functionality works on incremental models that use partitioning:

```jinja2
{{
    config(
        materialized = 'incremental',
        properties={
          "format": "'PARQUET'",
          "partitioned_by": "ARRAY['day']",
        }
    )
}}
```

#### Use table properties to configure connector specifics

Trino connectors use table properties to configure connector specifics.

Check the Trino connector documentation for more information.

```jinja2
{{
  config(
    materialized='table',
    properties={
      "format": "'PARQUET'",
      "partitioning": "ARRAY['bucket(id, 2)']",
    }
  )
}}
```

#### Seeds

Seeds are CSV files in your dbt project (typically in your data directory), that dbt can load into your data warehouse using the dbt seed command.

For dbt-trino batch_size is defined in macro `trino__get_batch_size()` and default value is `1000`.
In order to override default value define within your project a macro like the following:

```jinja2
{% macro default__get_batch_size() %}
  {{ return(10000) }}
{% endmacro %}
```

#### Generating lineage flow in docs

In order to generate lineage flow in docs use `ref` function in the place of table names in the query. It builts dependencies between models and allows to create DAG with data flow. Refer to examples [here](https://docs.getdbt.com/docs/building-a-dbt-project/building-models#building-dependencies-between-models).

```sh
dbt docs generate          # generate docs
dbt docs serve --port 8081 # starts local server (by default docs server runs on 8080 port, it may cause conflict with Trino in case of local development)
```

## Development

### Running tests

Tests can be executed against Trino or Starburst server. To run all tests alongside with building required docker images and server initialization run:

```sh
make dbt-trino-tests
make dbt-starburst-tests
```

Build dbt container locally:

```sh
./docker/dbt/build.sh
```

Run Trino or Starburst server locally:

```sh
./docker/init_trino.bash
./docker/init_starburst.bash
```

Run tests against Trino or Starburst:

```sh
./docker/run_tests.bash
```

Run the locally-built docker image (from docker/dbt/build.sh):

```sh
export DBT_PROJECT_DIR=$HOME/... # wherever the dbt project you want to run is
docker run -it --mount "type=bind,source=$HOME/.dbt/,target=/root/.dbt" --mount="type=bind,source=$DBT_PROJECT_DIR,target=/usr/app" --network dbt-net dbt-trino /bin/bash
```

### Running integration tests

Install the libraries required for development in order to be able to run the dbt tests:

```sh
pip install -r dev_requirements.txt
```

Run from the base directory of the project the command:

```sh
tox
```

or

```sh
pytest test/integration/trino.dbtspec
```

### Release process

Before doing a release dbt's version requires updating.
In order to bump dbt-trino and dbt-core version run:

```sh
bumpversion --config-file .bumpversion-dbt.cfg patch --new-version <new-version>
bumpversion --config-file .bumpversion.cfg patch --new-version <new-version> --allow-dirty
```

Additionally bump dbt-core version in dbt testing image `docker/dbt/Dockerfile`.

Next step is to merge bump commit and making sure that test suite pass.

Finally to release `dbt-trino` to PyPi and GitHub trigger release workflow `release.yml`.

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected
to follow the [PyPA Code of Conduct](https://www.pypa.io/en/latest/code-of-conduct/).
