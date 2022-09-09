<p float="left">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" width="45%" />
  <img src="https://trino.io/assets/trino-og.png" width="50%" />
</p>

[![Build Status](https://github.com/starburstdata/dbt-trino/actions/workflows/ci.yml/badge.svg)](https://github.com/starburstdata/dbt-trino/actions/workflows/ci.yml?query=workflow%3A%22dbt-trino+tests%22+branch%3Amaster+event%3Apush) [![db-presto-trino Slack](https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358)](https://getdbt.slack.com/channels/db-presto-trino)

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

This dbt plugin has been tested against `Trino` version `393` and `Starburst Enterprise` version `393-e.1`.

## Installation

This dbt adapter can be installed via pip:

```sh
$ pip install dbt-trino
```

### Configuring your profile

A dbt profile can be configured to run against Trino using the following configuration:

| Option                         | Description                                                                                                  | Required?                                                                                                        | Example                          |
|--------------------------------|--------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|----------------------------------|
| method                         | The Trino authentication method to use                                                                       | Optional (default is `none`, supported methods are `ldap`, `kerberos`, `jwt`, `oauth` or `certificate`)          | `none` or `kerberos`             |
| user                           | Username for authentication                                                                                  | Optional (required if `method` is `none`, `ldap` or `kerberos`)                                                  | `commander`                      |
| password                       | Password for authentication                                                                                  | Optional (required if `method` is `ldap`)                                                                        | `none` or `abc123`               |
| keytab                         | Path to keytab for kerberos authentication                                                                   | Optional (may be required if `method` is `kerberos`)                                                             | `/tmp/trino.keytab`              |
| krb5_config                    | Path to config for kerberos authentication                                                                   | Optional (may be required if `method` is `kerberos`)                                                             | `/tmp/krb5.conf`                 |
| principal                      | Principal for kerberos authentication                                                                        | Optional (may be required if `method` is `kerberos`)                                                             | `trino@EXAMPLE.COM`              |
| service_name                   | Service name for kerberos authentication                                                                     | Optional (default is `trino`)                                                                                    | `abc123`                         |
| jwt_token                      | JWT token for authentication                                                                                 | Optional (required if `method` is `jwt`)                                                                         | `none` or `abc123`               |
| client_certificate             | Path to client certificate to be used for certificate based authentication                                   | Optional (required if `method` is `certificate`)                                                                 | `/tmp/tls.crt`                   |
| client_private_key             | Path to client private key to be used for certificate based authentication                                   | Optional (required if `method` is `certificate`)                                                                 | `/tmp/tls.key`                   |
| http_headers                   | HTTP Headers to send alongside requests to Trino, specified as a yaml dictionary of (header, value) pairs.   | Optional                                                                                                         | `X-Trino-Client-Info: dbt-trino` |
| http_scheme                    | The HTTP scheme to use for requests to Trino                                                                 | Optional (default is `http`, or `https` for `method: kerberos`, `ldap` or `jwt`)                                 | `https` or `http`                |
| cert                           | The full path to a certificate file for authentication with trino                                            | Optional                                                                                                         |                                  |
| session_properties             | Sets Trino session properties used in the connection                                                         | Optional                                                                                                         | `query_max_run_time: 5d`         |
| database                       | Specify the database to build models into                                                                    | Required                                                                                                         | `analytics`                      |
| schema                         | Specify the schema to build models into. Note: it is not recommended to use upper or mixed case schema names | Required                                                                                                         | `public`                         |
| host                           | The hostname to connect to                                                                                   | Required                                                                                                         | `127.0.0.1`                      |
| port                           | The port to connect to the host on                                                                           | Required                                                                                                         | `8080`                           |
| threads                        | How many threads dbt should use                                                                              | Optional (default is `1`)                                                                                        | `8`                              |
| prepared_statements_enabled    | Enable usage of Trino prepared statements (used in `dbt seed` commands)                                      | Optional (default is `true`)                                                                                     | `true` or `false`                |
| retries                        | Configure how many times a database operation is retried when connection issues arise                        | Optional (default is `3`)                                                                                        | `10`                             |

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

**Example profiles.yml entry for kerberos authentication:**
```yaml
my-trino-db:
  target: dev
  outputs:
    dev:
      type: trino
      method: kerberos
      user: commander
      keytab: /tmp/trino.keytab
      krb5_config: /tmp/krb5.conf
      principal: trino@EXAMPLE.COM
      host: trino.example.com
      port: 443
      database: analytics
      schema: public
```

For reference on which session properties can be set on the the dbt profile do execute

```sql
SHOW SESSION;
```

on your Trino instance.

## Usage Notes

#### Supported Functionality

Note that upper or mixed case schema names will cause catalog queries to fail.
Please only use lower case schema names with this adapter.

#### Supported authentication types

- none - No authentication
- [ldap](https://trino.io/docs/current/security/authentication-types.html) - Specify username in `user` and password in `password`
- [kerberos](https://trino.io/docs/current/security/kerberos.html) - Specify username in `user`
- [jwt](https://trino.io/docs/current/security/jwt.html) - Specify JWT token in `jwt_token`
- [certificate](https://trino.io/docs/current/security/certificate.html) - Specify a client certificate in `client_certificate` and private key in `client_private_key`
- [oauth](https://trino.io/docs/current/security/oauth2.html) - It is recommended to install keyring to cache the OAuth2 token over multiple dbt invocations by running `pip install 'trino[external-authentication-token-cache]'`, keyring is not installed by default.

See also: https://trino.io/docs/current/security/authentication-types.html

#### Required configuration

dbt fundamentally works by dropping and creating tables and views in databases.
As such, the following Trino configs must be set for dbt to work properly on Trino:

```properties
hive.metastore-cache-ttl=0s
hive.metastore-refresh-interval = 5s
hive.allow-drop-table=true
hive.allow-rename-table=true
```

#### Session properties per model

In some specific cases, there may be needed tuning through the Trino session properties only 
for a specific dbt model.
In such cases, using the [dbt hooks](https://docs.getdbt.com/reference/resource-configs/pre-hook-post-hook)
may come to the rescue:

```
{{
  config(
    pre_hook="set session query_max_run_time='10m'"
  )
}}
```

#### Materializations

##### Table

`dbt-trino` supports two modes in `table` materialization `rename` and `drop` configured using `on_table_exists`.

- `rename` - creates intermediate table, then renames the target to backup one and renames intermediate to target one.
- `drop` - drops and recreates a table. It overcomes table rename limitation in AWS Glue.


By default `table` materialization uses `on_table_exists = 'rename'`, see an examples below how to change it.

In model add:
```jinja2
{{
  config(
    materialized = 'table',
    on_table_exists = 'drop`
  )
}}
```

or in `dbt_project.yaml`:

```yaml
models:
  path:
    materialized: table
    +on_table_exists: drop
```

Using `table` materialization and `on_table_exists = 'rename'` with AWS Glue may result in below error:

```
TrinoUserError(type=USER_ERROR, name=NOT_SUPPORTED, message="Table rename is not yet supported by Glue service")
```

##### View

Adapter supports two security modes in `view` materialization `DEFINER` and `INVOKER` configured using `view_security`.

See [Trino docs](https://trino.io/docs/current/sql/create-view.html#security) for more details about security modes in views.

By default `view` materialization uses `view_security = 'definer'`, see an examples below how to change it.

In model add:
```jinja2
{{
  config(
    materialized = 'view',
    view_security = 'invoker'
  )
}}
```

or in `dbt_project.yaml`:

```yaml
models:
  path:
    materialized: view
    +view_security: invoker
```


##### Incremental

Using an incremental model limits the amount of data that needs to be transformed, vastly reducing the runtime of your transformations. This improves performance and reduces compute costs.

```jinja2
{{
    config(
      materialized = 'incremental', 
      unique_key='<optional>',
      incremental_strategy='<optional>',)
}}
select * from {{ ref('events') }}
{% if is_incremental() %}
  where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
```

###### `append` (default)

The default incremental strategy is `append`. `append` only adds the new records based on the condition specified in the `is_incremental()` conditional block.

```jinja2
{{
    config(
      materialized = 'incremental')
}}
select * from {{ ref('events') }}
{% if is_incremental() %}
  where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
```

###### `delete+insert`

Through the `delete+insert` incremental strategy, you can instruct dbt to use a two-step incremental approach. It will first delete the records detected through the configured `is_incremental()` block and re-insert them.

```jinja2
{{
    config(
      materialized = 'incremental',
      unique_key='user_id',
      incremental_strategy='delete+insert',
      )
}}
select * from {{ ref('users') }}
{% if is_incremental() %}
  where updated_ts > (select max(updated_ts) from {{ this }})
{% endif %}
```

###### `merge`

Through the `merge` incremental strategy, dbt-trino constructs a [`MERGE` statement](https://trino.io/docs/current/sql/merge.html) which `INSERT`s new and `UPDATE`s existing records based on the unique key (specified by `unique_key`).  
If `unique_key` is not unique `delete+insert` strategy can be used.
Note that some connectors in Trino have limited or no support for `MERGE`.

```jinja2
{{
    config(
      materialized = 'incremental',
      unique_key='user_id',
      incremental_strategy='merge',
      )
}}
select * from {{ ref('users') }}
{% if is_incremental() %}
  where updated_ts > (select max(updated_ts) from {{ this }})
{% endif %}
```

###### Incremental overwrite on hive models

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

##### Snapshots

Commonly, analysts need to "look back in time" at some previous state of data in their mutable tables. While some source data systems are built in a way that makes accessing historical data possible, this is often not the case. dbt provides a mechanism, snapshots, which records changes to a mutable table over time.

Snapshots implement type-2 Slowly Changing Dimensions over mutable source tables. These Slowly Changing Dimensions (or SCDs) identify how a row in a table changes over time. Imagine you have an orders table where the status field can be overwritten as the order is processed. [See also the dbt docs about snapshots](https://docs.getdbt.com/docs/building-a-dbt-project/snapshots).

An example is given below.

```jinja2
{% snapshot orders_snapshot %}
{{
    config(
        target_database='analytics',
        target_schema='snapshots',
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}
select * from {{ source('jaffle_shop', 'orders') }}
{% endsnapshot %}
```

Note that the Snapshot feature depends on the `current_timestamp` macro. In some connectors the standard precision (`TIMESTAMP(3) WITH TIME ZONE`) is not supported by the connector eg. Iceberg.

If necessary, you can override the standard precision by providing your own version of the `trino__current_timestamp()` macro as in following example:

```jinja2
{% macro trino__current_timestamp() %}
    current_timestamp(6)
{% endmacro %}
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

#### Persist docs

Persist docs optionally persist resource descriptions as column and relation comments in the database. By default, documentation persistence is disabled, but it can be enabled for specific resources or groups of resources as needed.

Column-level comments are not supported in Trino views. Detailed documentation can be found [here](https://docs.getdbt.com/reference/resource-configs/persist_docs).

#### Generating lineage flow in docs

In order to generate lineage flow in docs use `ref` function in the place of table names in the query. It builts dependencies between models and allows to create DAG with data flow. Refer to examples [here](https://docs.getdbt.com/docs/building-a-dbt-project/building-models#building-dependencies-between-models).

```sh
dbt docs generate          # generate docs
dbt docs serve --port 8081 # starts local server (by default docs server runs on 8080 port, it may cause conflict with Trino in case of local development)
```

#### Using Custom schemas

By default, all dbt models are built in the schema specified in your target. But sometimes you wish to build some of the models in a custom schema. In order to do so, use the `schema` configuration key to specify a custom schema for a model. See [here](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas) for the documentation. It is important to note that by default, dbt will generate the schema name for a model by concatenating the custom schema to the target schema, as in: `<target_schema>_<custom_schema>`. 


#### Prepared statements

The `dbt seed` feature uses [Trino's prepared statements](https://trino.io/docs/current/sql/prepare.html).

Python's http client has a hardcoded limit of 65536 bytes for a header line.

When executing a prepared statement with a large number of parameters, you might encounter following error:

`requests.exceptions.ConnectionError: ('Connection aborted.', LineTooLong('got more than 65536 bytes when reading header line'))`.

The prepared statements can be disabled by setting `prepared_statements_enabled` to `true` in your dbt profile (reverting back to the legacy behavior using Python string interpolation). This flag may be removed in later releases.

## Contributing

- Want to report a bug or request a feature? Let us know on [Slack](http://community.getdbt.com/) in the [#db-presto-trino](https://getdbt.slack.com/channels/db-presto-trino) channel, or open [an issue](https://github.com/starburstdata/dbt-trino/issues/new)
- Want to help us build dbt-trino? Check out the [Contributing Guide](https://github.com/starburstdata/dbt-trino/blob/HEAD/CONTRIBUTING.md)

### Release process

Before doing a release, it is required to bump the dbt-trino version by triggering release workflow `version-bump.yml`. The major and minor part of the dbt version are used to associate dbt-trino's version with the dbt version.

Next step is to merge the bump PR and making sure that test suite pass.

Finally, to release `dbt-trino` to PyPi and GitHub trigger release workflow `release.yml`.

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected
to follow the [PyPA Code of Conduct](https://www.pypa.io/en/latest/code-of-conduct/).
