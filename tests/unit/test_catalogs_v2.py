"""Unit tests for TrinoAdapter bridge_v2_catalog hook methods."""

from types import SimpleNamespace

import pytest
from dbt.adapters.capability import Capability
from dbt.adapters.catalogs._integration import CatalogWriteIntegrationConfig

from dbt.adapters.trino import TrinoAdapter
from dbt.adapters.trino.catalogs import TrinoCatalogIntegration


def _v2_catalog(name, catalog_type, table_format_value, config=None):
    return SimpleNamespace(
        name=name,
        catalog_type=catalog_type,
        table_format=SimpleNamespace(value=table_format_value),
        config=config or {},
    )


def test_catalogs_v2_capability_supported():
    assert TrinoAdapter.supports(Capability.CatalogsV2)


class TestTrinoV2ToV1Type:
    def setup_method(self):
        self.adapter = object.__new__(TrinoAdapter)

    @pytest.mark.parametrize("catalog_type", ["trino", "iceberg_rest", "glue", "hive_metastore"])
    def test_maps_to_trino(self, catalog_type):
        assert self.adapter._v2_to_v1_type(catalog_type) == "trino"

    def test_unknown_passthrough(self):
        assert self.adapter._v2_to_v1_type("custom_type") == "custom_type"


class TestTrinoV2TableFormat:
    def setup_method(self):
        self.adapter = object.__new__(TrinoAdapter)

    def test_iceberg(self):
        catalog = _v2_catalog("cat", "trino", "iceberg")
        assert self.adapter._v2_table_format(catalog) == "iceberg"

    def test_default_is_none(self):
        catalog = _v2_catalog("cat", "trino", "default")
        assert self.adapter._v2_table_format(catalog) is None


class TestTrinoTranslateV2Properties:
    def setup_method(self):
        self.adapter = object.__new__(TrinoAdapter)

    def test_no_translation(self):
        props = {"catalog_database": "iceberg", "storage_uri": "s3://bucket"}
        assert self.adapter._translate_v2_properties("trino", props) == props


class TestTrinoCatalogIntegrationCatalogDatabase:
    def test_build_relation_sets_catalog_database(self):
        integration = TrinoCatalogIntegration(
            CatalogWriteIntegrationConfig(
                name="cat", catalog_type="trino", catalog_database="iceberg"
            )
        )
        model = SimpleNamespace(config={}, schema="my_schema", name="my_model")
        assert integration.build_relation(model).catalog_database == "iceberg"
