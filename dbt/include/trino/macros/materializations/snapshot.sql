{% macro trino__snapshot_hash_arguments(args) -%}
  lower(to_hex(md5(to_utf8(concat({%- for arg in args -%}
    coalesce(cast({{ arg }} as varchar), ''){% if not loop.last %}, '|',{% endif -%}
  {%- endfor -%}
  )))))
{%- endmacro %}

{% macro trino__post_snapshot(staging_relation) %}
  -- Clean up the snapshot temp table
  {% do drop_relation(staging_relation) %}
{% endmacro %}

{% macro trino__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({% for insert_col in insert_cols -%}
            DBT_INTERNAL_SOURCE.{{ insert_col }}
            {%- if not loop.last %}, {% endif %}
            {%- endfor %})

{% endmacro %}

{% macro snapshot_check_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set updated_at = config.get('updated_at', snapshot_get_time()) %}

    {% set column_added = false %}

    {% set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists, check_cols_config) %}

    {%- set row_changed_expr -%}
    (
    {%- if column_added -%}
        {{ get_true_sql() }}
    {%- else -%}
    {%- for col in check_cols -%}
        {{ snapshotted_rel }}.{{ col }} IS DISTINCT FROM {{ current_rel }}.{{ col }}
        {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
    {%- endif -%}
    )
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes
    }) %}
{% endmacro %}
