from typing import Any, Optional

from dbt.adapters.catalogs import CATALOG_INTEGRATION_MODEL_CONFIG_NAME  # type: ignore
from dbt.adapters.contracts.relation import RelationConfig

from dbt.adapters.trino import constants


def _config_get(config: Any, key: str) -> Any:
    """Read ``key`` from a node config that may not be dict-like.

    Some node configs are typed, non-mapping objects (e.g. a saved-query export's
    ``ExportConfig``, which has no ``.get``). Return None for those rather than
    raising AttributeError.
    """
    get = getattr(config, "get", None)
    return get(key) if callable(get) else None


def catalog_name(model: RelationConfig) -> Optional[str]:
    """Extract catalog name from model configuration"""
    if not hasattr(model, "config") or not model.config:
        return None

    if catalog := _config_get(model.config, CATALOG_INTEGRATION_MODEL_CONFIG_NAME):
        return catalog

    return constants.DEFAULT_TRINO_CATALOG.name
