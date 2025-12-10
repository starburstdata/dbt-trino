from types import SimpleNamespace

ADAPTER_TYPE = "trino"

TRINO_CATALOG_TYPE = "trino"

DEFAULT_TRINO_CATALOG = SimpleNamespace(
    name="trino_default",
    catalog_name="trino_default",
    catalog_type="trino",
    table_format=None,
    file_format=None,
    external_volume=None,
    adapter_properties={},
)
