import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.functional.adapter.materialization.fixtures import seed_csv


class BaseSeedOnTableExists:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv}

    @staticmethod
    def count_rows(project):
        return project.run_sql(
            f'select count(*) from "{project.database}"."{project.test_schema}"."seed"',
            fetch="one",
        )[0]


class TestSeedOnTableExistsDefault(BaseSeedOnTableExists):
    """
    Without on_table_exists, rebuilding a seed keeps dropping the existing table.
    """

    def test_seed_full_refresh_drops(self, project):
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results, logs = run_dbt_and_capture(
            ["--debug", "seed", "--full-refresh"], expect_pass=True
        )
        assert len(results) == 1
        assert f'drop table if exists "{project.database}"."{project.test_schema}"."seed"' in logs
        assert "create or replace table" not in logs
        assert self.count_rows(project) == 4


class BaseSeedOnTableExistsReplace(BaseSeedOnTableExists):
    """
    With on_table_exists = `replace`, rebuilding a seed uses CREATE OR REPLACE TABLE
    and never drops the existing table.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "seed_replace",
            "seeds": {"+on_table_exists": "replace"},
        }

    def test_seed_full_refresh_replaces(self, project):
        results, logs = run_dbt_and_capture(["--debug", "seed"], expect_pass=True)
        assert len(results) == 1
        assert "create or replace table" not in logs
        results, logs = run_dbt_and_capture(
            ["--debug", "seed", "--full-refresh"], expect_pass=True
        )
        assert len(results) == 1
        assert (
            f'create or replace table "{project.database}"."{project.test_schema}"."seed"' in logs
        )
        assert "drop table" not in logs
        assert self.count_rows(project) == 4


@pytest.mark.iceberg
class TestSeedOnTableExistsReplaceIceberg(BaseSeedOnTableExistsReplace):
    pass


@pytest.mark.delta
class TestSeedOnTableExistsReplaceDelta(BaseSeedOnTableExistsReplace):
    pass
