import pytest
import trino

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target(request):
    target = {
        "type": "trino",
        "method": "none",
        "threads": 1,
        "host": "localhost",
        "port": 8080,
        "user": "admin",
        "password": "",
        "catalog": "memory",
        "schema": "default",
    }

    prepared_statements_disabled = request.node.get_closest_marker("prepared_statements_disabled")
    if prepared_statements_disabled:
        target.update({"prepared_statements_enabled": False})

    postgresql = request.node.get_closest_marker("postgresql")
    iceberg = request.node.get_closest_marker("iceberg")
    delta = request.node.get_closest_marker("delta")

    if sum(bool(x) for x in (postgresql, iceberg, delta)) > 1:
        raise ValueError("Only one of postgresql, iceberg, delta can be specified as a marker")

    if postgresql:
        target.update({"catalog": "postgresql"})

    if delta:
        target.update({"catalog": "delta"})

    if iceberg:
        target.update({"catalog": "iceberg"})

    return target


@pytest.fixture(scope="class")
def trino_connection(dbt_profile_target):
    return trino.dbapi.connect(
        host=dbt_profile_target["host"],
        port=dbt_profile_target["port"],
        user=dbt_profile_target["user"],
        catalog=dbt_profile_target["catalog"],
        schema=dbt_profile_target["schema"],
    )


def get_engine_type():
    conn = trino.dbapi.connect(host="localhost", port=8080, user="dbt-trino")
    cur = conn.cursor()
    cur.execute("SELECT version()")
    version = cur.fetchone()
    if "-e" in version[0]:
        return "starburst"
    else:
        return "trino"
