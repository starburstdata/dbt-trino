import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.functional.adapter.materialization.fixtures import model_sql, seed_csv


class BaseViewsEnabled:
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
        }


class TestViewsEnabledTrue(BaseViewsEnabled):
    """
    Testing without views_enabled config specified, which defaults to views_enabled = True configuration
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "views_enabled_true",
            "models": {"+materialized": "incremental"},
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1

        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert (
            f'''create or replace view
    "{project.database}"."{project.test_schema}"."materialization__dbt_tmp"'''
            in logs
        )


class TestViewsEnabledFalse(BaseViewsEnabled):
    """
    Testing views_enabled = False configuration for incremental materialization
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "views_enabled_false",
            "models": {"+materialized": "incremental", "+views_enabled": False},
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    # The actual sequence of dbt commands and assertions
    # pytest will take care of all "setup" + "teardown"
    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1

        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert (
            f'create table "{project.database}"."{project.test_schema}"."materialization__dbt_tmp"'
            in logs
        )
