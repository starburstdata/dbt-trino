from typing import Optional

from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME  # type: ignore
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.trino import constants


def catalog_name(model: RelationConfig) -> Optional[str]:
    """Extract catalog name from model configuration"""
    if not hasattr(model, "config") or not model.config:
        return None

    if catalog := model.config.get(CATALOG_INTEGRATION_MODEL_CONFIG_NAME):
        return catalog

    return constants.DEFAULT_TRINO_CATALOG.name
