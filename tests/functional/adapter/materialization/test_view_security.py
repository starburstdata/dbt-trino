import pytest
from dbt.tests.util import run_dbt, check_relations_equal

from tests.functional.adapter.materialization.fixtures import (
    seed_csv,
    model_sql,
    profile_yml,
)


class TestViewSecurity:
    """
    Testing view_security = 'invoker' configuration for view materialization,
    using dbt seed, run and tests commands and validate data load correctness.
    """

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example",
            "models": {
                "+materialized": "view",
                "+view_security": "invoker"
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
            "materialization.sql": model_sql,
            "materialization.yml": profile_yml,
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        # run models
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "materialization"])
