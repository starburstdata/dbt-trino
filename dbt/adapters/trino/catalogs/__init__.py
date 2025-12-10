from dbt.adapters.trino.catalogs._relation import TrinoCatalogRelation
from dbt.adapters.trino.catalogs._trino_catalog_metastore import TrinoCatalogIntegration

__all__ = [
    "TrinoCatalogIntegration",
    "TrinoCatalogRelation",
]
