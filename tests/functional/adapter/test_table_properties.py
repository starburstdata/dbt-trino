import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

from tests.functional.adapter.materialization.fixtures import model_sql, seed_csv


class BaseTableProperties:
    # Everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    # Everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
        }


@pytest.mark.iceberg
class TestTableProperties(BaseTableProperties):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "properties_test",
            "models": {
                "+materialized": "table",
                "+properties": {
                    "format": "'PARQUET'",
                    "format_version": "2",
                },
            },
        }

    def test_table_properties(self, project):
        # Seed seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Create model with properties
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "WITH (" in logs
        assert "format = 'PARQUET'" in logs
        assert "format_version = 2" in logs


@pytest.mark.iceberg
class TestFileFormatConfig(BaseTableProperties):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "properties_test",
            "models": {
                "+materialized": "table",
                "file_format": "'PARQUET'",
            },
        }

    def test_table_properties(self, project):
        # Seed seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Create model with properties
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "WITH (" in logs
        assert "format = 'PARQUET'" in logs


@pytest.mark.iceberg
class TestFileFormatConfigAndFormatTablePropertyFail(BaseTableProperties):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "properties_test",
            "models": {
                "+materialized": "table",
                "+properties": {
                    "format": "'PARQUET'",
                },
                "file_format": "'ORC'",
            },
        }

    def test_table_properties(self, project):
        # Seed seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Create model with properties
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert len(results) == 1
        assert (
            "You can specify either 'file_format' or 'properties.format' configurations, but not both."
            in logs
        )


@pytest.mark.hive
# Setting `type` property is available only in Starburst Galaxy
# https://docs.starburst.io/starburst-galaxy/data-engineering/working-with-data-lakes/table-formats/gl-iceberg.html
@pytest.mark.skip_profile("trino_starburst")
class TestTableFormatConfig(BaseTableProperties):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "properties_test",
            "models": {
                "+materialized": "table",
                "table_format": "'iceberg'",
            },
        }

    def test_table_properties(self, project):
        # Seed seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Create model with properties
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "WITH (" in logs
        assert "type = 'iceberg'" in logs


@pytest.mark.hive
# Setting `type` property is available only in Starburst Galaxy
# https://docs.starburst.io/starburst-galaxy/data-engineering/working-with-data-lakes/table-formats/gl-iceberg.html
@pytest.mark.skip_profile("trino_starburst")
class TestTableFormatConfigAndTypeTablePropertyFail(BaseTableProperties):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "properties_test",
            "models": {
                "+materialized": "table",
                "+properties": {
                    "type": "'iceberg'",
                },
                "table_format": "'iceberg'",
            },
        }

    def test_table_properties(self, project):
        # Seed seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Create model with properties
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert len(results) == 1
        assert (
            "You can specify either 'table_format' or 'properties.type' configurations, but not both."
            in logs
        )
