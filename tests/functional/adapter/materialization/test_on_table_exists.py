import pytest
from dbt.tests.util import check_relations_equal, run_dbt, run_dbt_and_capture

from tests.functional.adapter.materialization.fixtures import (
    model_sql,
    profile_yml,
    seed_csv,
)


class BaseOnTableExists:
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


class TestOnTableExistsRename(BaseOnTableExists):
    """
    Testing on_table_exists = `rename` configuration for table materialization,
    using dbt seed, run and tests commands and validate data load correctness.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "table_rename",
            "models": {"+materialized": "table", "+on_table_exists": "rename"},
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
        # run models two times to check on_table_exists = 'rename'
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert (
            f'create table "{project.database}"."{project.test_schema}"."materialization"' in logs
        )
        assert "alter table" not in logs
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert (
            f'create table "{project.database}"."{project.test_schema}"."materialization__dbt_tmp"'
            in logs
        )
        assert (
            f'alter table "{project.database}"."{project.test_schema}"."materialization" rename to "{project.database}"."{project.test_schema}"."materialization__dbt_backup"'
            in logs
        )
        assert (
            f'alter table "{project.database}"."{project.test_schema}"."materialization__dbt_tmp" rename to "{project.database}"."{project.test_schema}"."materialization"'
            in logs
        )
        assert (
            f'drop table if exists "{project.database}"."{project.test_schema}"."materialization__dbt_backup"'
            in logs
        )
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "materialization"])


class TestOnTableExistsSkip(BaseOnTableExists):
    """
    Testing on_table_exists = `skip` configuration for table materialization,
    using dbt seed, run and tests commands and validate data load correctness.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "table_rename",
            "models": {"+materialized": "table", "+on_table_exists": "skip"},
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
        # run models two times to check on_table_exists = 'skip'
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert (
            f'create table "{project.database}"."{project.test_schema}"."materialization"' in logs
        )
        assert "alter table" not in logs
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "drop table" not in logs
        assert "create table" not in logs
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "materialization"])


class TestOnTableExistsRenameIncrementalFullRefresh(BaseOnTableExists):
    """
    Testing on_table_exists = `rename` configuration for incremental materialization and full refresh flag,
    using dbt seed, run and tests commands and validate data load correctness.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "table_rename",
            "models": {"+materialized": "incremental", "+on_table_exists": "rename"},
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
        # run models two times to check on_table_exists = 'rename'
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert (
            f'create table "{project.database}"."{project.test_schema}"."materialization"' in logs
        )
        assert "alter table" not in logs
        results, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"], expect_pass=True)
        assert len(results) == 1
        assert (
            f'create table "{project.database}"."{project.test_schema}"."materialization__dbt_tmp"'
            in logs
        )
        assert (
            f'alter table "{project.database}"."{project.test_schema}"."materialization" rename to "{project.database}"."{project.test_schema}"."materialization__dbt_backup"'
            in logs
        )
        assert (
            f'alter table "{project.database}"."{project.test_schema}"."materialization__dbt_tmp" rename to "{project.database}"."{project.test_schema}"."materialization"'
            in logs
        )
        assert (
            f'drop table if exists "{project.database}"."{project.test_schema}"."materialization__dbt_backup"'
            in logs
        )
        assert "create or replace view" not in logs
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "materialization"])


class TestOnTableExistsDrop(BaseOnTableExists):
    """
    Testing on_table_exists = `drop` configuration for table materialization,
    using dbt seed, run and tests commands and validate data load correctness.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "table_drop",
            "models": {"+materialized": "table", "+on_table_exists": "drop"},
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
        # run models two times to check on_table_exists = 'drop'
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "materialization"])


class TestOnTableExistsDropIncrementalFullRefresh(BaseOnTableExists):
    """
    Testing on_table_exists = `drop` configuration for incremental materialization and full refresh flag,
    using dbt seed, run and tests commands and validate data load correctness.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "table_drop",
            "models": {"+materialized": "incremental", "+on_table_exists": "drop"},
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
        # run models two times to check on_table_exists = 'drop'
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert (
            f'drop table if exists "{project.database}"."{project.test_schema}"."materialization"'
            not in logs
        )
        results, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"], expect_pass=True)
        assert len(results) == 1
        assert (
            f'drop table if exists "{project.database}"."{project.test_schema}"."materialization"'
            in logs
        )
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "materialization"])


class BaseOnTableExistsReplace(BaseOnTableExists):
    """
    Testing on_table_exists = `replace` configuration for table materialization,
    using dbt seed, run and tests commands and validate data load correctness.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "table_drop",
            "models": {"+materialized": "table", "+on_table_exists": "replace"},
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
        # run models two times to check on_table_exists = 'replace'
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "create or replace table" in logs
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "create or replace table" in logs
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "materialization"])


@pytest.mark.iceberg
class TestOnTableExistsReplaceIceberg(BaseOnTableExistsReplace):
    pass


@pytest.mark.delta
class TestOnTableExistsReplaceDelta(BaseOnTableExistsReplace):
    pass


class BaseOnTableExistsReplaceIncrementalFullRefresh(BaseOnTableExists):
    """
    Testing on_table_exists = `replace` configuration for incremental materialization and full refresh flag,
    using dbt seed, run and tests commands and validate data load correctness.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "table_drop",
            "models": {"+materialized": "incremental", "+on_table_exists": "replace"},
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
        # run models two times to check on_table_exists = 'replace'
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "create or replace table" not in logs
        results, logs = run_dbt_and_capture(["--debug", "run", "--full-refresh"], expect_pass=True)
        assert len(results) == 1
        assert "create or replace table" in logs
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 3

        # check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "materialization"])


@pytest.mark.iceberg
class TestOnTableExistsReplaceIcebergIncrementalFullRefresh(
    BaseOnTableExistsReplaceIncrementalFullRefresh
):
    pass


@pytest.mark.delta
class TestOnTableExistsReplaceDeltaIncrementalFullRefresh(
    BaseOnTableExistsReplaceIncrementalFullRefresh
):
    pass
