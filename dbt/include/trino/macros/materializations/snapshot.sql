{% macro trino__snapshot_hash_arguments(args) -%}
  lower(to_hex(md5(to_utf8(concat({%- for arg in args -%}
    coalesce(cast({{ arg }} as varchar), ''){% if not loop.last %}, '|',{% endif -%}
  {%- endfor -%}
  )))))
{%- endmacro %}

{% macro trino__post_snapshot(staging_relation) %}
  -- Clean up the snapshot temp table
  {%- set snapshot_tmp_relation_type = config.get('snapshot_tmp_relation_type', 'view') -%}
  {% do drop_relation(staging_relation.incorporate(type=snapshot_tmp_relation_type)) %}
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


{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {# --Based on configuration, materialize temporary relation as a view or as a table-- #}
    {%- set snapshot_tmp_relation_type = config.get('snapshot_tmp_relation_type', 'view') -%}
    {% if snapshot_tmp_relation_type == 'view' %}
        {% do run_query(create_view_as(temp_relation, select)) %}
    {% elif snapshot_tmp_relation_type == 'table' %}
        {% do run_query(create_table_as(True, temp_relation, select)) %}
    {% else %}
        {% do exceptions.raise_compiler_error(
          "Invalid snapshot_tmp_relation_type, should be view or table, got " ~ snapshot_tmp_relation_type
        ) %}
    {% endif %}

    {% do return(temp_relation) %}
{% endmacro %}
