# Contributing to `dbt-trino`

## Getting the code

### How to contribute?

You can contribute to `dbt-trino` by forking the `dbt-trino` repository. For a detailed overview on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. Fork the `dbt-trino` repository
2. Clone your fork locally
3. Check out a new branch for your proposed changes
4. Push changes to your fork
5. Open a pull request against `starburstdata/dbt-trino` from your forked repository

## Setting up an environment

There are some tools that will be helpful to you in developing locally. While this is the list relevant for `dbt-trino` development, many of these tools are used commonly across open-source python projects.

### Tools

These are the tools used in `dbt-trino` development and testing:

- [`tox`](https://tox.readthedocs.io/en/latest/) to manage virtualenvs across python versions. We currently target the latest patch releases for Python 3.9, 3.10, 3.11, 3.12, and 3.13
- [`pytest`](https://docs.pytest.org/en/latest/) to define, discover, and run tests
- [`flake8`](https://flake8.pycqa.org/en/latest/) for code linting
- [`black`](https://github.com/psf/black) for code formatting
- [`isort`](https://pycqa.github.io/isort/) for sorting imports
- [`mypy`](https://mypy.readthedocs.io/en/stable/) for static type checking
- [`pre-commit`](https://pre-commit.com) to easily run those checks
- [`changie`](https://changie.dev/) to create changelog entries, without merge conflicts
- [`make`](https://users.cs.duke.edu/~ola/courses/programming/Makefiles/Makefiles.html) to run multiple setup or test steps in combination. Don't worry too much, nobody _really_ understands how `make` works, and our Makefile aims to be super simple.
- [GitHub Actions](https://github.com/features/actions) for automating tests and checks, once a PR is pushed to the `dbt-trino` repository

A deep understanding of these tools in not required to effectively contribute to `dbt-trino`, but we recommend checking out the attached documentation if you're interested in learning more about each one.

#### Virtual environments

We strongly recommend using virtual environments when developing code in `dbt-trino`. We recommend creating this virtualenv
in the root of the `dbt-trino` repository. To create a new virtualenv, run:
```sh
python3 -m venv env
source env/bin/activate
```

This will create and activate a new Python virtual environment.

#### Docker and `docker compose`

Docker and `docker compose` are both used in testing. Specific instructions for you OS can be found [here](https://docs.docker.com/get-docker/).

## Running `dbt-trino` in development

### Installation

First make sure that you set up your `virtualenv` as described in [Setting up an environment](#setting-up-an-environment).  Also ensure you have the latest version of pip installed with `pip install --upgrade pip`. Next, install `dbt-trino` (and its dependencies) with:

```sh
pip install -e . -r dev_requirements.txt
```

When installed in this way, any changes you make to your local copy of the source code will be reflected immediately in your next `dbt` run.

### Running `dbt-trino`

With your virtualenv activated, the `dbt` script should point back to the source code you've cloned on your machine. You can verify this by running `which dbt`. This command should show you a path to an executable in your virtualenv.

Configure your [profile](https://docs.getdbt.com/docs/configure-your-profile) as necessary to connect to your target databases. It may be a good idea to add a new profile pointing to a local Trino instance if appropriate.

## Testing

Once you're able to manually test that your code change is working as expected, it's important to run existing automated tests, as well as adding some new ones. These tests will ensure that:
- Your code changes do not unexpectedly break other established functionality
- Your code changes can handle all known edge cases
- The functionality you're adding will _keep_ working in the future

### Initial setup

To be able to run the tests locally you will need a Trino or Starburst instance.

```sh
# to start Trino
make start-trino
# to start Starburst
make start-starburst
```

### Testing against Starburst Galaxy

Some tests only run against a real Galaxy account: the `persist_docs` Data Discovery sync, and the
query routing tests. They read their settings from `test.env`. Copy `test.env.example` to
`test.env` (gitignored) and work through the steps below to fill it in.

**Prerequisites:** a Starburst Galaxy account you can administer, with a running cluster and
catalogs named `iceberg`, `delta`, and `hive`.

#### 1. API credentials

In the Galaxy UI, create an OAuth API token at
`https://<account>.galaxy.starburst.io/api-auth-token`. The secret is shown once. Fill in:

```
DBT_TESTS_STARBURST_GALAXY_API_URL=https://<account>.galaxy.starburst.io
DBT_TESTS_STARBURST_GALAXY_CLIENT_ID=<client id>
DBT_TESTS_STARBURST_GALAXY_SECRET_KEY=<secret>
```

To check that the token works:

```sh
python scripts/galaxy_test_setup.py status
```

That lists the account's clusters, which is also where you get a cluster hostname for
`DBT_TESTS_STARBURST_GALAXY_HOST` (pick a cluster attached to all 3 required catalogs).

#### 2. Service account

The tests connect as a service account, use the following scripts to create a service account or 
create one manually using the UI.

```sh
python scripts/galaxy_test_setup.py create-service-account --name dbt-tests
python scripts/galaxy_test_setup.py service-accounts        # list what already exists
```

It prints the two lines to paste into `test.env`. The password is only returned at creation; for an
account that already exists, `issue-password --name <username>` issues a new one.

Use the username exactly as Galaxy reports it - the `@<account>.galaxy.starburst.io` suffix is
required, and it is not always what you typed when creating the account:

```
DBT_TESTS_STARBURST_GALAXY_USER=dbt-tests@<account>.galaxy.starburst.io/accountadmin
DBT_TESTS_STARBURST_GALAXY_PASSWORD=<password>
```

The trailing `/accountadmin` is optional and selects the role to connect under, defaulting to the
service account's default role. Whichever role ends up active needs full access to the `iceberg`
catalog, since the tests create schemas and tables in it. The role also matters for routing - see
step 3.

#### 3. Query routing rules

The routing tests prove that two models with different `client_tags` are executed by two different
clusters, so the account has to be able to do that:

- **Smart routing enabled** on the account. Its routing endpoint goes in
  `..._ROUTING_HOST` as `<account>.routing.trino.galaxy.starburst.io`.
- **Two clusters** besides the one handling everything else. Use
  `python scripts/galaxy_test_setup.py clone-cluster --from <existing> --name <new>` to copy an
  existing cluster's region, catalogs and sizing or use the UI to create the cluster(s). Every
  cluster involved needs the `iceberg` catalog attached: the models are written from the
  routed clusters and read back through the profile connection.
- **Three routing rules**, created in the UI under Admin > Routing rules - they cannot be managed
  through the public API. A rule matches when the query carries every tag the rule lists *and* the
  connecting role matches, with the first match winning:

  | Order | Role                 | Query tags | Cluster |
  | --- |--- | --- | --- |
  | 1 | the role from step 2 | value of `..._ROUTING_TAG_A` | one cluster |
  | 2 | the role from step 2 | value of `..._ROUTING_TAG_B` | a *different* cluster |
  | 3 | public | none | a third cluster |

  Rules 1 and 2 must name the role the test user connects *as*, not merely one it holds. Connect
  under a different role and neither matches, everything falls through to rule 3, and both models
  end up on the same cluster.

  Rule 3 is the catch-all, and its cluster must differ from the other two: the routing test runs a
  model per tag plus one configuring no tags, and asserts three different clusters executed them,
  which is what catches a model inheriting the routing of the model before it.

```
DBT_TESTS_STARBURST_GALAXY_ROUTING_HOST=<account>.routing.trino.galaxy.starburst.io
DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_A=dbt-cluster-a
DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_B=dbt-cluster-b
```

#### 4. Verify, then run

```sh
make galaxy-routing-probe
```

This runs a query per tag through the routing endpoint and reports which cluster served each,
failing if two tags land on the same one. Rule changes can take a few minutes to take effect, and
the first query against a suspended cluster has to start it. Once it passes:

```sh
make dbt-galaxy-tests                                                    # the whole suite
python -m pytest tests/functional/adapter/test_query_routing.py \
  --profile starburst_galaxy                                             # just the routing tests
```

### Test commands

There are a few methods for running tests locally.

#### Makefile

There are multiple targets in the Makefile to run common test suites and code
checks, most notably:

```sh
# Runs integration tests on Trino
make dbt-trino-tests
# Runs integration tests on Starburst
make dbt-starburst-tests
```
> These make targets assume you have a local installation of a recent version of [`tox`](https://tox.readthedocs.io/en/latest/) for unit/integration testing and pre-commit for code quality checks,
> unless you use choose a Docker container to run tests. Run `make help` for more info.

#### `pre-commit`
[`pre-commit`](https://pre-commit.com) takes care of running all code-checks for formatting and linting. Run `make dev` to install `pre-commit` in your local environment.  Once this is done you can use any of the linter-based make targets as well as a git pre-commit hook that will ensure proper formatting and linting.

#### `tox`

[`tox`](https://tox.readthedocs.io/en/latest/) takes care of managing virtualenvs and install dependencies in order to run tests. You can also run tests in parallel, for example, you can run unit tests for Python 3.9, 3.10, 3.11, 3.12, and 3.13 checks in parallel with `tox -p`. Also, you can run unit tests for specific python versions with `tox -e py39`. The configuration for these tests in located in `tox.ini`.

#### `pytest`

Finally, you can also run a specific test or group of tests using [`pytest`](https://docs.pytest.org/en/latest/) directly. With a virtualenv active and dev dependencies installed you can do things like:

```sh
# run all unit tests in a file
python3 -m pytest tests/unit/utils.py
# run a specific unit test
python3 -m pytest tests/unit/test_adapter.py::TestTrinoAdapter::test_acquire_connection
# run integration tests
python3 -m pytest tests/functional
```

> See [pytest usage docs](https://docs.pytest.org/en/6.2.x/usage.html) for an overview of useful command-line options.

The catalog in the dbt profile can be setup through [pytest markers](https://docs.pytest.org/en/7.1.x/example/markers.html#registering-markers), if no marker has been specified the memory catalog is used.

For example if you want to set the dbt profile to connect to the Delta Lake catalog, annotate your test with `@pytest.mark.delta`, (supported markers are `postgresql`, `delta` or `iceberg`).

```
@pytest.mark.delta
def test_run_seed_test(self, project):
  ...
```

## Adding CHANGELOG Entry

We use [changie](https://changie.dev) to generate `CHANGELOG` entries. **Note:** Do not edit the `CHANGELOG.md` directly. Your modifications will be lost.

Follow the steps to [install `changie`](https://changie.dev/guide/installation/) for your system.

Once changie is installed and your PR is created, simply run `changie new` and changie will walk you through the process of creating a changelog entry.  Commit the file that's created and your changelog entry is complete!

You don't need to worry about which `dbt-trino` version your change will go into. Just create the changelog entry with `changie`, and open your PR against the `master` branch. 

## Submitting a Pull Request

A `dbt-trino` maintainer will review your PR. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Automated tests run via GitHub Actions. If you're a first-time contributor, all tests (including code checks and unit tests) will require a maintainer to approve. Changes in the `dbt-trino` repository trigger integration tests against Trino and Starburst.

Once all tests are passing and your PR has been approved, a `dbt-trino` maintainer will merge your changes into the master branch. And that's it! Happy developing :tada:
