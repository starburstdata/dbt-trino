import os

import pytest
import trino

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="trino_starburst", type=str)


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "trino_starburst":
        target = get_trino_starburst_target()
    elif profile_type == "starburst_galaxy":
        target = get_galaxy_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")

    prepared_statements_disabled = request.node.get_closest_marker("prepared_statements_disabled")
    if prepared_statements_disabled:
        target.update({"prepared_statements_enabled": False})

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
        "catalog": "iceberg",
        "schema": "default",
        "timezone": "UTC",
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on {profile_type} profile")


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
