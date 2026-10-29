import pytest
from dbt.adapters.catalogs import DbtCatalogIntegrationNotSupportedError
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file

from tests.functional.adapter.catalog_integrations.fixtures import (
    MODEL_WITH_CATALOG,
    MODEL_WITH_CATALOG_CONFIGS_FILE_FORMAT,
    MODEL_WITH_CATALOG_CONFIGS_STORAGE_URI,
    MODEL_WITHOUT_CATALOG,
)


class BaseCatalogsV2(BaseCatalogIntegrationValidation):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}


@pytest.mark.iceberg
class TestTrinoCatalogsV2FileFormat(BaseCatalogsV2):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_trino_catalog",
                    "type": "trino",
                    "table_format": "default",
                    "config": {"trino": {"file_format": "orc"}},
                }
            ]
        }

    def test_model_without_catalog(self, project):
        write_file(MODEL_WITHOUT_CATALOG, project.project_root, "models", "test_model.sql")
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "CREATE TABLE" in logs
        assert "WITH (" not in logs

    def test_model_with_catalog(self, project):
        write_file(MODEL_WITH_CATALOG, project.project_root, "models", "test_model.sql")
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "WITH (" in logs
        assert "format = 'orc'" in logs
        assert "type = 'default'" not in logs
        assert "type = 'iceberg'" not in logs

    def test_model_with_catalog_configs_file_format(self, project):
        write_file(
            MODEL_WITH_CATALOG_CONFIGS_FILE_FORMAT,
            project.project_root,
            "models",
            "test_model.sql",
        )
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "format = 'parquet'" in logs


@pytest.mark.iceberg
@pytest.mark.skip_profile("starburst_galaxy")
class TestTrinoCatalogsV2MappedTypeLocation(BaseCatalogsV2):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_trino_catalog",
                    "type": "hive_metastore",
                    "table_format": "default",
                    "config": {"trino": {"external_volume": "s3://datalake"}},
                }
            ]
        }

    def test_model_with_catalog(self, project):
        write_file(MODEL_WITH_CATALOG, project.project_root, "models", "test_model.sql")
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert f"location = 's3://datalake/_dbt/{project.test_schema}/test_model'" in logs

    def test_model_with_catalog_configs_storage_uri(self, project):
        write_file(
            MODEL_WITH_CATALOG_CONFIGS_STORAGE_URI,
            project.project_root,
            "models",
            "test_model.sql",
        )
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "location = 's3://datalake/storage_uri'" in logs


@pytest.mark.iceberg
# Setting `type` property is available only in Starburst Galaxy
@pytest.mark.skip_profile("trino_starburst")
class TestTrinoCatalogsV2IcebergTableFormat(BaseCatalogsV2):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_trino_catalog",
                    "type": "trino",
                    "table_format": "iceberg",
                    "config": {"trino": {}},
                }
            ]
        }

    def test_model_with_catalog(self, project):
        write_file(MODEL_WITH_CATALOG, project.project_root, "models", "test_model.sql")
        results, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert len(results) == 1
        assert "type = 'iceberg'" in logs


MODEL_CATALOG_DB_BEATS_CONFIG = """
{{ config(materialized='table', catalog_name='test_trino_catalog', database=target.database) }}
select 1 as id
"""


@pytest.mark.iceberg
# The alternate catalog `memory` exists only in the local Trino/Starburst setup.
@pytest.mark.skip_profile("starburst_galaxy")
class TestTrinoCatalogsV2CatalogDatabase(BaseCatalogsV2):
    """catalog_database takes priority over the model's database config and target.database."""

    ALT_DATABASE = "memory"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "catalog_db_only.sql": MODEL_WITH_CATALOG,
            "catalog_db_beats_config.sql": MODEL_CATALOG_DB_BEATS_CONFIG,
            "no_catalog.sql": MODEL_WITHOUT_CATALOG,
        }

    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_trino_catalog",
                    "type": "trino",
                    "table_format": "default",
                    "config": {"trino": {"catalog_database": self.ALT_DATABASE}},
                }
            ]
        }

    @pytest.fixture
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=self.ALT_DATABASE, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_catalog_database_precedence(self, project, clean_up):
        results = run_dbt(["run"])
        assert len(results) == 3

        adapter = project.adapter
        schema = project.test_schema
        with adapter.connection_named("__test"):
            assert adapter.get_relation(self.ALT_DATABASE, schema, "catalog_db_only") is not None
            assert (
                adapter.get_relation(self.ALT_DATABASE, schema, "catalog_db_beats_config")
                is not None
            )
            assert adapter.get_relation(project.database, schema, "no_catalog") is not None
            assert adapter.get_relation(self.ALT_DATABASE, schema, "no_catalog") is None


@pytest.mark.iceberg
class TestTrinoCatalogsV2UnsupportedType(BaseCatalogsV2):
    @pytest.fixture(scope="class")
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "test_trino_catalog",
                    "type": "unity",
                    "table_format": "iceberg",
                    "config": {"trino": {}},
                }
            ]
        }

    def test_unsupported_type_errors(self, project):
        write_file(MODEL_WITH_CATALOG, project.project_root, "models", "test_model.sql")
        with pytest.raises(DbtCatalogIntegrationNotSupportedError, match="Expected one of: trino"):
            run_dbt(["parse"])
