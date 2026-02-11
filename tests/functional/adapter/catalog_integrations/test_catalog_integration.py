import pytest
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt_and_capture, write_file

from tests.functional.adapter.catalog_integrations.fixtures import (
    MODEL_WITH_CATALOG,
    MODEL_WITH_CATALOG_CONFIGS_BASE_LOCATION,
    MODEL_WITH_CATALOG_CONFIGS_BASE_LOCATION_NONE,
    MODEL_WITH_CATALOG_CONFIGS_BASE_LOCATION_NONE_OMIT_BASE_LOCATION_ROOT,
    MODEL_WITH_CATALOG_CONFIGS_FILE_FORMAT,
    MODEL_WITH_CATALOG_CONFIGS_LOCATION,
    MODEL_WITH_CATALOG_CONFIGS_STORAGE_URI,
    MODEL_WITH_CATALOG_CONFIGS_TABLE_FORMAT,
    MODEL_WITHOUT_CATALOG,
)


@pytest.mark.iceberg
class TestTrinoCatalogIntegrationFileFormat(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_trino_catalog",
                    "active_write_integration": "trino_integration",
                    "write_integrations": [
                        {
                            "name": "trino_integration",
                            "catalog_type": "trino",
                            "file_format": "orc",
                        }
                    ],
                }
            ]
        }

    def test_model_without_catalog(self, project):
        # Create model with catalog configuration
        write_file(MODEL_WITHOUT_CATALOG, project.project_root, "models", "test_model.sql")
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" not in logs

    def test_model_with_catalog(self, project):
        # Create model with catalog configuration
        write_file(MODEL_WITH_CATALOG, project.project_root, "models", "test_model.sql")
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert "format = 'orc'" in logs

    def test_model_with_catalog_configs_file_format(self, project):
        # Create model with catalog configuration
        write_file(
            MODEL_WITH_CATALOG_CONFIGS_FILE_FORMAT,
            project.project_root,
            "models",
            "test_model.sql",
        )
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert "format = 'parquet'" in logs


@pytest.mark.iceberg
# Setting `type` property is available only in Starburst Galaxy
# https://docs.starburst.io/starburst-galaxy/data-engineering/working-with-data-lakes/table-formats/gl-iceberg.html
@pytest.mark.skip_profile("trino_starburst")
class TestMyAdapterCatalogIntegration(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_trino_catalog",
                    "active_write_integration": "trino_integration",
                    "write_integrations": [
                        {
                            "name": "trino_integration",
                            "catalog_type": "trino",
                            "table_format": "iceberg",
                        }
                    ],
                }
            ]
        }

    def test_model_with_catalog(self, project):
        # Create model with catalog configuration
        write_file(MODEL_WITH_CATALOG, project.project_root, "models", "test_model.sql")
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert "type = 'iceberg'" in logs

    def test_model_with_catalog_configs_table_format(self, project):
        # Create model with catalog configuration
        write_file(
            MODEL_WITH_CATALOG_CONFIGS_TABLE_FORMAT,
            project.project_root,
            "models",
            "test_model.sql",
        )
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert "type = 'delta'" in logs


@pytest.mark.iceberg
@pytest.mark.skip_profile("starburst_galaxy")
class TestTrinoCatalogIntegrationLocation(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_trino_catalog",
                    "active_write_integration": "trino_integration",
                    "write_integrations": [
                        {
                            "name": "trino_integration",
                            "catalog_type": "trino",
                            "external_volume": "s3://datalake",
                        }
                    ],
                }
            ]
        }

    def test_model_with_catalog(self, project):
        # Create model with catalog configuration
        write_file(MODEL_WITH_CATALOG, project.project_root, "models", "test_model.sql")
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert f"location = 's3://datalake/_dbt/{project.test_schema}/test_model'" in logs

    def test_model_with_catalog_configs_location(self, project):
        # Create model with catalog configuration
        write_file(
            MODEL_WITH_CATALOG_CONFIGS_LOCATION, project.project_root, "models", "test_model.sql"
        )
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert "location = 's3://datalake/location'" in logs

    def test_model_with_catalog_configs_storage_uri(self, project):
        # Create model with catalog configuration
        write_file(
            MODEL_WITH_CATALOG_CONFIGS_STORAGE_URI,
            project.project_root,
            "models",
            "test_model.sql",
        )
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert "location = 's3://datalake/storage_uri'" in logs

    def test_model_with_catalog_configs_base_location(self, project):
        # Create model with catalog configuration
        write_file(
            MODEL_WITH_CATALOG_CONFIGS_BASE_LOCATION,
            project.project_root,
            "models",
            "test_model.sql",
        )
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert f"location = 's3://datalake/foo/{project.test_schema}/test_model/bar'" in logs

    def test_model_with_catalog_configs_base_location_none(self, project):
        # Create model with catalog configuration
        write_file(
            MODEL_WITH_CATALOG_CONFIGS_BASE_LOCATION_NONE,
            project.project_root,
            "models",
            "test_model.sql",
        )
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert f"location = 's3://datalake/_dbt/{project.test_schema}/test_model'" in logs

    def test_model_with_catalog_configs_base_location_none_omit_base_location_root(self, project):
        # Create model with catalog configuration
        write_file(
            MODEL_WITH_CATALOG_CONFIGS_BASE_LOCATION_NONE_OMIT_BASE_LOCATION_ROOT,
            project.project_root,
            "models",
            "test_model.sql",
        )
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" in logs
        assert f"location = 's3://datalake/{project.test_schema}/test_model'" in logs
