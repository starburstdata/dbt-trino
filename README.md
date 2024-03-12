# dbt-trino

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/starburstdata/dbt-trino/master/assets/images/Starburst_Logo_White%2BBlue.svg" width="98%">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/starburstdata/dbt-trino/master/assets/images/Starburst_Logo_Black%2BBlue.svg" width="98%">
  <img alt="Starburst" src="https://raw.githubusercontent.com/starburstdata/dbt-trino/master/assets/images/Starburst_Logo_Black%2BBlue.svg">
</picture>
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/starburstdata/dbt-trino/master/assets/images/dbt-signature_tm_light.svg" width="45%">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/starburstdata/dbt-trino/master/assets/images/dbt-signature_tm.svg" width="45%">
  <img alt="dbt" src="https://raw.githubusercontent.com/starburstdata/dbt-trino/master/assets/images/dbt-signature_tm.svg">
</picture>
&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/starburstdata/dbt-trino/master/assets/images/trino-logo-dk-bg.svg" width="50%">
  <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/starburstdata/dbt-trino/master/assets/images/trino-logo-w-bk.svg" width="50%">
  <img alt="trino" src="https://raw.githubusercontent.com/starburstdata/dbt-trino/master/assets/images/trino-logo-w-bk.svg">
</picture>

[![Build Status](https://github.com/starburstdata/dbt-trino/actions/workflows/ci.yml/badge.svg)](https://github.com/starburstdata/dbt-trino/actions/workflows/ci.yml?query=workflow%3A%22dbt-trino+tests%22+branch%3Amaster+event%3Apush) [![db-presto-trino Slack](https://img.shields.io/static/v1?logo=slack&logoColor=959DA5&label=Slack&labelColor=333a41&message=join%20conversation&color=3AC358)](https://getdbt.slack.com/channels/db-presto-trino)

## Introduction

[dbt](https://docs.getdbt.com/docs/introduction) is a data transformation workflow tool that lets teams quickly and collaboratively deploy analytics code, following software engineering best practices like modularity, CI/CD, testing, and documentation. It enables anyone who knows SQL to build production-grade data pipelines.

One frequently asked question in the context of using `dbt` tool is:

> Can I connect my dbt project to two databases?

(see the answered [question](https://docs.getdbt.com/faqs/connecting-to-two-dbs-not-allowed) on the dbt website).

**TL;DR** `dbt` stands for transformation as in `T` within `ELT` pipelines, it doesn't move data from source to a warehouse.

`dbt-trino` adapter uses [Trino](https://trino.io/) as a underlying query engine to perform query federation across disperse data sources. Trino connects to multiple and diverse data sources ([available connectors](https://trino.io/docs/current/connector.html)) via one dbt connection and process SQL queries at scale. Transformations defined in dbt are passed to Trino which handles these SQL transformation queries and translates them to queries specific to the systems it connects to create tables or views and manipulate data.

This repository represents a fork of the [dbt-presto](https://github.com/dbt-labs/dbt-presto) with adaptations to make it work with Trino.

## Compatibility

This dbt plugin has been tested against `Trino` version `439`, `Starburst Enterprise` version `435-e.1` and `Starburst Galaxy`.

## Setup & Configuration

For information on installing and configuring your profile to authenticate to Trino or Starburst, please refer to [Starburst and Trino Setup](https://docs.getdbt.com/reference/warehouse-setups/trino-setup) in the dbt docs.

### Trino- and Starburst-specific configuration

For Trino- and Starburst-specific configuration, you can refer to [Starburst (Trino) configurations](https://docs.getdbt.com/reference/resource-configs/trino-configs) on the dbt docs site.

## Contributing

- Want to report a bug or request a feature? Let us know on [Slack](http://community.getdbt.com/) in the [#db-presto-trino](https://getdbt.slack.com/channels/db-presto-trino) channel, or open [an issue](https://github.com/starburstdata/dbt-trino/issues/new)
- Want to help us build dbt-trino? Check out the [Contributing Guide](https://github.com/starburstdata/dbt-trino/blob/HEAD/CONTRIBUTING.md)

### Release process
First 5 steps are ONLY relevant for bumping __minor__ version:
1. Create `1.x.latest` branch from the latest tag corresponding to current minor version, e.g. `git checkout -b 1.6.latest v1.6.2` (when bumping to 1.7). Push branch to remote. This branch will be used for potential backports.
2. Create new branch (Do not push below commits to `1.x.latest`). Add a new entry in `.changes/0.0.0.md` that points to the newly created latest branch.
3. Run `changie merge` to update `README.md`. After that, remove changie files and folders related to current minor version. Commit.
4. Bump version of `dbt-tests-adapter`. Commit.
5. Merge these 2 commits into the master branch. Add a `Skip Changlelog` label to the PR.

Continue with the next steps for a __minor__ version bump. Start from this point for a __patch__ version bump:
1. Run `Version Bump` workflow. The major and minor part of the dbt version are used to associate dbt-trino's version with the dbt version.
2. Merge the bump PR. Make sure that test suite pass.
3. Run `dbt-trino release` workflow to release `dbt-trino` to PyPi and GitHub.

### Backport process

Sometimes it is necessary to backport some changes to some older versions. In that case, create branch from `x.x.latest` branch. There is a `x.x.latest` for each minor version, e.g. `1.3.latest`. Make a fix and open PR back to `x.x.latest`. Create changelog by `changie new` as ususal, as separate changlog for each minor version is kept on every `x.x.latest` branch.
After merging, to make a release of that version, just follow instructions from **Release process** section, but run every workflow on `x.x.latest` branch.

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected
to follow the [PyPA Code of Conduct](https://www.pypa.io/en/latest/code-of-conduct/).
