from dataclasses import dataclass
from typing import Optional

from dbt.adapters.trino import constants, parse_model
from dbt.adapters.trino.catalogs._trino_catalog_metastore import (
    TrinoCatalogIntegration,
)


@dataclass
class _ExportLikeConfig:
    """Mimics a typed, non-mapping node config (no ``.get``)."""

    export_as: str = "table"
    database: Optional[str] = None


class _ExportLikeModel:
    def __init__(self) -> None:
        self.config = _ExportLikeConfig()
        self.schema = "my_schema"
        self.name = "my_export"


def test_catalog_name_non_mapping_config_returns_default():
    model = _ExportLikeModel()
    # Must not raise AttributeError; falls back to the default catalog.
    assert parse_model.catalog_name(model) == constants.DEFAULT_TRINO_CATALOG.name


def test_calculate_storage_uri_non_mapping_config_returns_none():
    integration = object.__new__(TrinoCatalogIntegration)
    integration.external_volume = "s3://bucket"
    model = _ExportLikeModel()
    # Must not raise AttributeError; a non-mapping config has no storage_uri.
    assert integration._calculate_storage_uri(model) is None
