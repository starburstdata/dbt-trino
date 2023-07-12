seed_csv = """
id,name,some_date
1,Easton,1981-05-20 06:46:51
2,Lillian,1978-09-03 18:10:33
3,Jeremiah,1982-03-11 03:59:51
4,Nolan,1976-05-06 20:21:35
""".lstrip()

model_sql = """
select * from {{ ref('seed') }}
"""

model_cte_sql = """
with source_data as (
    select 1 as id, 'aaa' as field1, 'bbb' as field2, 111 as field3, 'TTT' as field4
)
select id
       ,field1
       ,field2
       ,field3
       ,field4
from source_data
"""


profile_yml = """
version: 2
models:
  - name: materialization
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: name
        tests:
          - not_null
"""


schema_base_yml = """\
version: 2

models:
  - name: model_a
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_ignore
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_ignore_target
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_append_new_columns
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_append_new_columns_target
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_append_new_columns_remove_one
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_append_new_columns_remove_one_target
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_sync_all_columns
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_sync_all_columns_target
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_sync_all_columns_diff_data_types
    columns:
      - name: id
        tests:
          - unique

  - name: incremental_sync_all_columns_diff_data_types_target
    columns:
      - name: id
        tests:
          - unique
"""

model_a_sql = """\
{{
    config(materialized='table')
}}

with source_data as (

    select 1 as id, 'aaa' as field1, 'bbb' as field2, 111 as field3, 'TTT' as field4
    union all select 2 as id, 'ccc' as field1, 'ddd' as field2, 222 as field3, 'UUU' as field4
    union all select 3 as id, 'eee' as field1, 'fff' as field2, 333 as field3, 'VVV' as field4
    union all select 4 as id, 'ggg' as field1, 'hhh' as field2, 444 as field3, 'WWW' as field4
    union all select 5 as id, 'iii' as field1, 'jjj' as field2, 555 as field3, 'XXX' as field4
    union all select 6 as id, 'kkk' as field1, 'lll' as field2, 666 as field3, 'YYY' as field4

)

select id
       ,field1
       ,field2
       ,field3
       ,field4

from source_data
"""

incremental_ignore_sql = """\
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='ignore'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental() %}

SELECT id, field1, field2, field3, field4 FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id, field1, field2 FROM source_data LIMIT 3

{% endif %}
"""

incremental_ignore_target_sql = """\
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id
       ,field1
       ,field2

from source_data
"""

incremental_append_new_columns = """\
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id,
       cast(field1 as varchar) as field1,
       cast(field2 as varchar) as field2,
       cast(field3 as varchar) as field3,
       cast(field4 as varchar) as field4
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       cast(field1 as varchar) as field1,
       cast(field2 as varchar) as field2
FROM source_data where id <= 3

{% endif %}
"""

incremental_append_new_columns_target_sql = """\
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id
       ,cast(field1 as varchar) as field1
       ,cast(field2 as varchar) as field2
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field3 END as varchar) AS field3
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field4 END as varchar) AS field4

from source_data
"""

incremental_append_new_columns_remove_one_sql = """\
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id,
       cast(field1 as varchar) as field1,
       cast(field3 as varchar) as field3,
       cast(field4 as varchar) as field4
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       cast(field1 as varchar) as field1,
       cast(field2 as varchar) as field2
FROM source_data where id <= 3

{% endif %}
"""

incremental_append_new_columns_remove_one_target_sql = """\
{{
    config(materialized='table')
}}
with source_data as (

    select * from {{ ref('model_a') }}

)

select id,
       cast(field1 as varchar) as field1,
       cast(CASE WHEN id >  3 THEN NULL ELSE field2 END as varchar) AS field2,
       cast(CASE WHEN id <= 3 THEN NULL ELSE field3 END as varchar) AS field3,
       cast(CASE WHEN id <= 3 THEN NULL ELSE field4 END as varchar) AS field4

from source_data
"""


incremental_fail_sql = """\
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id, field1, field2 FROM source_data

{% else %}

SELECT id, field1, field3 FROm source_data

{% endif %}
"""

incremental_sync_all_columns_sql = """\
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'

    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental() %}

SELECT id,
       cast(field1 as varchar) as field1,
       cast(field3 as varchar) as field3, -- to validate new fields
       cast(field4 as varchar) AS field4 -- to validate new fields

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

select id,
       cast(field1 as varchar) as field1,
       cast(field2 as varchar) as field2

from source_data where id <= 3

{% endif %}
"""

incremental_sync_all_columns_target_sql = """\
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)
select id
       ,cast(field1 as varchar) as field1
       --,field2
       ,cast(case when id <= 3 then null else field3 end as varchar) as field3
       ,cast(case when id <= 3 then null else field4 end as varchar) as field4

from source_data
order by id
"""

incremental_sync_all_columns_diff_data_types_sql = """\
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental() %}

SELECT id,
       cast(id as varchar) "field1" -- to validate data type changes

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

select id,
       id "field1"

from source_data where id <= 3
order by id
{% endif %}
"""

incremental_sync_all_columns_diff_data_types_target_sql = """\
{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

select id,
       cast(id as varchar) "field1"

from source_data
order by id
"""

select_from_a_sql = "select * from {{ ref('model_a') }} where false"

select_from_incremental_append_new_columns_sql = (
    "select * from {{ ref('incremental_append_new_columns') }} where false"
)

select_from_incremental_append_new_columns_remove_one_sql = (
    "select * from {{ ref('incremental_append_new_columns_remove_one') }} where false"
)

select_from_incremental_append_new_columns_remove_one_target_sql = (
    "select * from {{ ref('incremental_append_new_columns_remove_one_target') }} where false"
)

select_from_incremental_append_new_columns_target_sql = (
    "select * from {{ ref('incremental_append_new_columns_target') }} where false"
)

select_from_incremental_ignore_sql = "select * from {{ ref('incremental_ignore') }} where false"

select_from_incremental_ignore_target_sql = (
    "select * from {{ ref('incremental_ignore_target') }} where false"
)

select_from_incremental_sync_all_columns_sql = (
    "select * from {{ ref('incremental_sync_all_columns') }} where false"
)

select_from_incremental_sync_all_columns_target_sql = (
    "select * from {{ ref('incremental_sync_all_columns_target') }} where false"
)

select_from_incremental_sync_all_columns_diff_data_types_sql = (
    "select * from {{ ref('incremental_sync_all_columns_diff_data_types') }} where false"
)

select_from_incremental_sync_all_columns_diff_data_types_target_sql = (
    "select * from {{ ref('incremental_sync_all_columns_diff_data_types_target') }} where false"
)
