from typing import Optional

from dbt.adapters.catalogs import CatalogIntegration, CatalogIntegrationConfig
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.trino import constants
from dbt.adapters.trino.catalogs._relation import TrinoCatalogRelation


class TrinoCatalogIntegration(CatalogIntegration):
    """
    Catalog type:
        In Trino, the metastore for a catalog is set when configuring the connector.
        This cannot be configured using dbt's generated SQL.

        Documentation:
            https://trino.io/docs/current/overview/concepts.html#catalog
            https://trino.io/docs/current/object-storage/metastores.html

    Table format:
        For Trino and Starburst SEP, the table format is specified by the connector configuration.
        Setting table_format here will result in error, as 'type' property is unavailable in Trino and Starburst SEP.
        If you are using Starburst Galaxy, you can set the default table format to use for this catalog.
        It will set `type` property to specified table format.

        Documentation:
            https://docs.starburst.io/starburst-galaxy/data-engineering/working-with-data-lakes/table-formats/index.html
    """

    catalog_type = constants.TRINO_CATALOG_TYPE
    allows_writes = True

    def __init__(self, config: CatalogIntegrationConfig) -> None:
        super().__init__(config)
        self.storage_uri = config.adapter_properties.get("storage_uri")

    def build_relation(self, model: RelationConfig) -> TrinoCatalogRelation:
        return TrinoCatalogRelation(
            catalog_type=self.catalog_type,
            catalog_name=self.catalog_name,
            table_format=self.table_format,
            file_format=self.file_format,
            external_volume=self.external_volume,
            storage_uri=self._calculate_storage_uri(model),
        )

    def _calculate_storage_uri(self, model: RelationConfig) -> Optional[str]:
        if not model.config:
            return None

        if model_storage_uri := model.config.get("storage_uri"):
            return model_storage_uri

        if not self.external_volume:
            return None

        # Default dbt behavior is that if base_location_root is not specified, `_dbt` prefix is added.
        # Even if base_location_root is explicitly set to None, `_dbt` prefix is still added.
        # Allow omitting the prefix by setting omit_base_location_root to True.
        omit_base_location_root = model.config.get("omit_base_location_root")
        if omit_base_location_root:
            storage_uri = f"{self.external_volume}/{model.schema}/{model.name}"
        else:
            prefix = model.config.get("base_location_root") or "_dbt"
            storage_uri = f"{self.external_volume}/{prefix}/{model.schema}/{model.name}"
        if suffix := model.config.get("base_location_subpath"):
            storage_uri = f"{storage_uri}/{suffix}"
        return storage_uri
