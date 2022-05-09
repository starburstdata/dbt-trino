import pytest
import trino

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target(request):
    target = {
        'type': 'trino',
        'method': 'none',
        'threads': 1,
        'host': 'localhost',
        'port': 8080,
        'user': 'admin',
        'password': '',
        'catalog': 'memory',
        'schema': 'default'
    }

    marker = request.node.get_closest_marker("prepared_statements_disabled")
    if marker:
        target.update({
            'prepared_statements_enabled': False
        })

    return target

@pytest.fixture(scope="class")
def trino_connection(dbt_profile_target):
    return trino.dbapi.connect(
        host=dbt_profile_target['host'],
        port=dbt_profile_target['port'],
        user=dbt_profile_target['user'],
        catalog=dbt_profile_target['catalog'],
        schema=dbt_profile_target['schema']
    )
