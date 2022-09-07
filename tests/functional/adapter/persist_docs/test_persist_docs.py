import pytest
from dbt.tests.util import run_dbt

from tests.functional.adapter.persist_docs.fixtures import (
    incremental_model,
    incremental_profile_yml,
    seed_csv,
    table_model,
    table_profile_yml,
    view_model,
    view_profile_yml,
)


@pytest.mark.iceberg
class TestPersistDocsBase:
    """
    Testing persist_docs functionality
    """

    @property
    def schema(self):
        return "default"

    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }


class TestPersistDocsTable(TestPersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "persist_docs_tests",
            "models": {"+persist_docs": {"relation": True, "columns": True}},
            "seeds": {
                "+column_types": {"date": "timestamp(6)"},
                "+persist_docs": {"relation": True, "columns": True},
            },
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model,
            "table_persist_docs.yml": table_profile_yml,
        }

    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 4


# Comments on views are not supported in Trino engine
# https://github.com/trinodb/trino/issues/10705
# TODO Galaxy tests skipped on type=USER_ERROR, name=PERMISSION_DENIED,
# message="Access Denied: Cannot comment view"
@pytest.mark.skip_profile("starburst_galaxy")
class TestPersistDocsView(TestPersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "persist_docs_tests",
            "models": {
                "+persist_docs": {"relation": True},
                "+materialized": "view",
                "+view_security": "definer",
            },
            "seeds": {
                "+column_types": {"date": "timestamp(6)"},
                "+persist_docs": {"relation": True},
            },
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": view_model,
            "view_persist_docs.yml": view_profile_yml,
        }

    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 4


class TestPersistDocsIncremental(TestPersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "persist_docs_tests",
            "models": {"+persist_docs": {"relation": True, "columns": True}},
            "seeds": {
                "+column_types": {"date": "timestamp(6)"},
                "+persist_docs": {"relation": True, "columns": True},
            },
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model.sql": incremental_model,
            "incremental_persist_docs.yml": incremental_profile_yml,
        }

    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 4
