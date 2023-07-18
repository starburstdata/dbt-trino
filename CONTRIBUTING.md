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

- [`tox`](https://tox.readthedocs.io/en/latest/) to manage virtualenvs across python versions. We currently target the latest patch releases for Python 3.8, 3.9, 3.10, and 3.11
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
pip install -r dev_requirements.txt
pip install -e .
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

[`tox`](https://tox.readthedocs.io/en/latest/) takes care of managing virtualenvs and install dependencies in order to run tests. You can also run tests in parallel, for example, you can run unit tests for Python 3.8, Python 3.9, Python 3.10, and Python 3.11 checks in parallel with `tox -p`. Also, you can run unit tests for specific python versions with `tox -e py39`. The configuration for these tests in located in `tox.ini`.

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
