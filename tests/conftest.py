import os

import pytest
import trino

from tests.env_file import load_test_env

load_test_env()

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="trino_starburst", type=str)


# Skip tests for profiles marked with @pytest.mark.skip_profile
# See pytest docs for skipping based on command-line options:
# https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
def pytest_collection_modifyitems(config, items):
    profile_type = config.getoption("--profile")
    for item in items:
        if skip_profile_marker := item.get_closest_marker("skip_profile"):
            if profile_type in skip_profile_marker.args:
                skip_profile = pytest.mark.skip(reason=f"skipped on {profile_type} profile")
                item.add_marker(skip_profile)


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "trino_starburst":
        target = get_trino_starburst_target()
    elif profile_type == "starburst_galaxy":
        target = get_galaxy_target()
    elif profile_type == "starburst_portal":
        target = get_starburst_portal_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")

    prepared_statements_disabled = request.node.get_closest_marker("prepared_statements_disabled")
    if prepared_statements_disabled:
        target.update({"prepared_statements_enabled": False})

    if request.node.get_closest_marker("single_thread"):
        target.update({"threads": 1})

    if request.node.get_closest_marker("query_routing"):
        if profile_type == "starburst_galaxy":
            target.update(galaxy_routing_target())
        elif profile_type == "starburst_portal":
            target.update(portal_routing_target())

    postgresql = request.node.get_closest_marker("postgresql")
    iceberg = request.node.get_closest_marker("iceberg")
    delta = request.node.get_closest_marker("delta")
    hive = request.node.get_closest_marker("hive")

    if sum(bool(x) for x in (postgresql, iceberg, delta)) > 1:
        raise ValueError("Only one of postgresql, iceberg, delta can be specified as a marker")

    if postgresql:
        target.update({"catalog": "postgresql"})

    if delta:
        target.update({"catalog": "delta"})

    if iceberg:
        target.update({"catalog": "iceberg"})

    if hive:
        target.update({"catalog": "hive"})

    return target


def get_trino_starburst_target():
    return {
        "type": "trino",
        "method": "none",
        "threads": 4,
        "host": "localhost",
        "port": 8080,
        "user": "admin",
        "password": "",
        "roles": {
            "hive": "admin",
        },
        "catalog": "memory",
        "schema": "default",
        "timezone": "UTC",
    }


def get_galaxy_target():
    return {
        "type": "trino",
        "method": "ldap",
        "threads": 4,
        "retries": 5,
        "host": os.environ.get("DBT_TESTS_STARBURST_GALAXY_HOST"),
        "port": 443,
        "user": os.environ.get("DBT_TESTS_STARBURST_GALAXY_USER"),
        "password": os.environ.get("DBT_TESTS_STARBURST_GALAXY_PASSWORD"),
        # Tests carrying a catalog marker override this with the marker's catalog.
        "catalog": os.environ.get("DBT_TESTS_STARBURST_GALAXY_CATALOG", "iceberg"),
        "schema": "default",
        "timezone": "UTC",
    }


def get_starburst_portal_target():
    return {
        "type": "trino",
        "method": "none",
        "threads": 4,
        "host": "localhost",
        "port": 8090,
        "user": "admin",
        "catalog": "memory",
        "schema": "default",
        "timezone": "UTC",
    }


def portal_routing_target():
    """Target overrides for the Starburst Control Plane (Portal) query routing tests.

    Requires `docker-compose-starburst-routing.yml` to be running, with sep1 and
    sep2 registered as backends in different routing groups and a CLIENT_TAGS
    routing rule per tag pointing at the matching group - see
    scripts/starburst_portal_test_setup.py and CONTRIBUTING.md.
    """
    tag_a = os.environ.get("DBT_TESTS_STARBURST_ROUTING_TAG_A")
    tag_b = os.environ.get("DBT_TESTS_STARBURST_ROUTING_TAG_B")

    if not all([tag_a, tag_b]):
        pytest.skip(
            "query routing tests need DBT_TESTS_STARBURST_ROUTING_TAG_A and "
            "DBT_TESTS_STARBURST_ROUTING_TAG_B, pointing at a Portal with a "
            "CLIENT_TAGS routing rule per tag"
        )

    return {}


def galaxy_routing_target():
    """Target overrides for the Starburst Galaxy query routing tests.

    Routing only happens on an account's routing endpoint, so those tests need a
    host of their own, plus two client tags that the account's routing rules send
    to two different clusters.

    Statements dbt runs outside a model - metadata queries, seeds, macros - carry
    the profile's tags, and a query matching no rule is rejected rather than
    routed somewhere sensible. So the account needs a catch-all: either a rule on
    the connecting role with no tags, or a rule for a tag of its own, which is
    what the optional default tag is for.
    """
    host = os.environ.get("DBT_TESTS_STARBURST_GALAXY_ROUTING_HOST")
    tag_a = os.environ.get("DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_A")
    tag_b = os.environ.get("DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_B")
    default_tag = os.environ.get("DBT_TESTS_STARBURST_GALAXY_ROUTING_DEFAULT_TAG")

    if not all([host, tag_a, tag_b]):
        pytest.skip(
            "query routing tests need DBT_TESTS_STARBURST_GALAXY_ROUTING_HOST, "
            "DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_A and "
            "DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_B, pointing at an account with "
            "smart routing enabled and a rule per tag"
        )

    target = {"host": host}
    if default_tag:
        target["client_tags"] = [default_tag]
    return target


@pytest.fixture(scope="class")
def trino_connection(dbt_profile_target):
    if dbt_profile_target["method"] == "ldap":
        return trino.dbapi.connect(
            host=dbt_profile_target["host"],
            port=dbt_profile_target["port"],
            auth=trino.auth.BasicAuthentication(
                dbt_profile_target["user"], dbt_profile_target["password"]
            ),
            catalog=dbt_profile_target["catalog"],
            schema=dbt_profile_target["schema"],
            http_scheme="https",
        )
    else:
        return trino.dbapi.connect(
            host=dbt_profile_target["host"],
            port=dbt_profile_target["port"],
            user=dbt_profile_target["user"],
            catalog=dbt_profile_target["catalog"],
            schema=dbt_profile_target["schema"],
        )


def get_engine_type(trino_connection):
    conn = trino_connection
    if "galaxy.starburst.io" in conn.host:
        return "starburst_galaxy"
    cur = conn.cursor()
    cur.execute("SELECT version()")
    version = cur.fetchone()
    if "-e" in version[0]:
        return "starburst_enterprise"
    else:
        return "trino"


@pytest.fixture(autouse=True)
def skip_by_engine_type(request, trino_connection):
    engine_type = get_engine_type(trino_connection)
    if request.node.get_closest_marker("skip_engine"):
        for skip_engine_type in request.node.get_closest_marker("skip_engine").args:
            if skip_engine_type == engine_type:
                pytest.skip(f"skipped on {engine_type} engine")
