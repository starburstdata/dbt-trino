import pytest
from dbt.tests.util import run_dbt, check_relations_equal
from conftest import dbt_profile_target

from tests.functional.adapter.materialization.fixtures import (
    seed_csv,
    model_sql,
    profile_yml,
)


class PreparedStatementsBase:
    """
    Testing prepared_statements_enabled profile configuration using dbt 
    seed, run and tests commands and validate data load correctness.
    """

    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "example"
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

    def retrieve_num_prepared_statements(self, trino_connection):
        cur = trino_connection.cursor()
        cur.execute("select query from system.runtime.queries order by query_id desc limit 3")
        result = cur.fetchall()
        return len(list(filter(lambda rec: 'EXECUTE' in rec[0], result)))

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def run_seed_with_prepared_statements(self, project, trino_connection, expected_num_prepared_statements):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Check if the seed command is using prepared statements
        assert self.retrieve_num_prepared_statements(trino_connection) == expected_num_prepared_statements

        # run models
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "materialization"])

@pytest.mark.prepared_statements_disabled
@pytest.mark.skip_profile("starburst_galaxy")
class TestPreparedStatementsDisabled(PreparedStatementsBase):
    def test_run_seed_with_prepared_statements_disabled(self, project, trino_connection):
        self.run_seed_with_prepared_statements(project, trino_connection, 0)


@pytest.mark.skip_profile("starburst_galaxy")
class TestPreparedStatementsEnabled(PreparedStatementsBase):
    def test_run_seed_with_prepared_statements_enabled(self, project, trino_connection):
        self.run_seed_with_prepared_statements(project, trino_connection, 1)
