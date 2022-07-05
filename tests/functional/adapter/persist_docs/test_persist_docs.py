import pytest
from dbt.tests.util import run_dbt, check_relations_equal, run_sql_with_adapter

from tests.functional.adapter.persist_docs.fixtures import (
    seed_csv,
    table_model,
    view_model,
    incremental_model,
    profile_yml
)


class TestPersistDocs:
    """
    Testing persist_docs functionality
    """

    @property
    def schema(self):
        return "default"

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {
                "+persist_docs": {
                    "relation": True,
                    "columns": True
                }
            }
        }

    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model,
            "view_model.sql": view_model,
            "incremental_model.sql": incremental_model,
            "persist_docs.yml": profile_yml,
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "table_model"])

        for table in ["table_model", "view_model", "incremental_model"]:
            results = self.run_sql_with_adapter(
                "describe memory.default.{table}".format(table=table),
                fetch="all",
            )

            for result in results:
                if result[0] == "Comment":
                    assert result[1].startswith(f"test model description")
                if result[0] == "id":
                    assert result[2].startswith("id Column description")
                if result[0] == "name":
                    assert result[2].startswith("Some stuff here and then a call to")
