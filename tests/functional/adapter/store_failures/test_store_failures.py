import pytest
from dbt.tests.adapter.store_test_failures_tests import basic
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    TestStoreTestFailures,
)
from dbt.tests.util import run_dbt

from tests.functional.adapter.store_failures.fixtures import (
    seed_csv,
    table_model,
    table_profile_yml,
)


class TestStoreFailuresTable:
    @property
    def schema(self):
        return "default"

    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "store_failures_tests",
            "quoting": {
                "database": False,
                "schema": False,
                "identifier": True,
            },
            "models": {
                "+materialized": "table",
            },
            "tests": {
                "+store_failures": True,
            },
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model,
            "table_store_failures.yml": table_profile_yml,
        }

    @pytest.fixture(autouse=True)
    def teardown_method(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_dbt_test__audit"
            )
            project.adapter.drop_schema(relation)

    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 5
        # test tests 2nd times
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 5


class TestTrinoTestStoreTestFailures(TestStoreTestFailures):
    pass


class TestStoreTestFailuresAsInteractions(basic.StoreTestFailuresAsInteractions):
    pass


class TestStoreTestFailuresAsProjectLevelOff(basic.StoreTestFailuresAsProjectLevelOff):
    pass


class TestStoreTestFailuresAsProjectLevelView(basic.StoreTestFailuresAsProjectLevelView):
    pass


class TestStoreTestFailuresAsGeneric(basic.StoreTestFailuresAsGeneric):
    pass


class TestStoreTestFailuresAsProjectLevelEphemeral(basic.StoreTestFailuresAsProjectLevelEphemeral):
    pass


class TestStoreTestFailuresAsExceptions(basic.StoreTestFailuresAsExceptions):
    pass
