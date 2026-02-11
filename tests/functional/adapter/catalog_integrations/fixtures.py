MODEL_WITHOUT_CATALOG = """
{{ config(
    materialized='table',
) }}

select 1 as id, 'test' as name
"""

MODEL_WITH_CATALOG = """
{{ config(
    materialized='table',
    catalog_name='test_trino_catalog'
) }}

select 1 as id, 'test' as name
"""

MODEL_WITH_CATALOG_CONFIGS_TABLE_FORMAT = """
{{ config(
    materialized='table',
    catalog_name='test_trino_catalog',
    table_format='delta',
) }}

select 1 as id, 'test' as name
"""

MODEL_WITH_CATALOG_CONFIGS_FILE_FORMAT = """
{{ config(
    materialized='table',
    catalog_name='test_trino_catalog',
    file_format='parquet',
) }}

select 1 as id, 'test' as name
"""

MODEL_WITH_CATALOG_CONFIGS_LOCATION = """
{{ config(
    materialized='table',
    catalog_name='test_trino_catalog',
    storage_uri='s3://datalake/storage_uri',
    properties= {
        'location': "'s3://datalake/location'",
    }
) }}

select 1 as id, 'test' as name
"""

MODEL_WITH_CATALOG_CONFIGS_STORAGE_URI = """
{{ config(
    materialized='table',
    catalog_name='test_trino_catalog',
    storage_uri='s3://datalake/storage_uri',
) }}

select 1 as id, 'test' as name
"""

MODEL_WITH_CATALOG_CONFIGS_BASE_LOCATION = """
{{ config(
    materialized='table',
    catalog_name='test_trino_catalog',
    base_location_root='foo',
    base_location_subpath='bar',
) }}

select 1 as id, 'test' as name
"""

MODEL_WITH_CATALOG_CONFIGS_BASE_LOCATION_NONE = """
{{ config(
    materialized='table',
    catalog_name='test_trino_catalog',
    base_location_root=None,
) }}

select 1 as id, 'test' as name
"""

MODEL_WITH_CATALOG_CONFIGS_BASE_LOCATION_NONE_OMIT_BASE_LOCATION_ROOT = """
{{ config(
    materialized='table',
    catalog_name='test_trino_catalog',
    base_location_root=None,
    omit_base_location_root=true,
) }}

select 1 as id, 'test' as name
"""
