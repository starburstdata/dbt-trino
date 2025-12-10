from dataclasses import dataclass
from typing import Optional

from dbt.adapters.catalogs import CatalogRelation

from dbt.adapters.trino import constants


@dataclass
class TrinoCatalogRelation(CatalogRelation):
    catalog_type: str = constants.DEFAULT_TRINO_CATALOG.catalog_type
    catalog_name: Optional[str] = constants.DEFAULT_TRINO_CATALOG.name
    table_format: Optional[str] = None
    file_format: Optional[str] = None
    external_volume: Optional[str] = None
    storage_uri: Optional[str] = None
