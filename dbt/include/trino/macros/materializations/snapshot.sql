{% materialization snapshot, adapter='trino' %}
    {% if config.get('properties') %}
        {% if config.get('properties').get('location') %}
            {%- do exceptions.raise_compiler_error("Specifying 'location' property in snapshots is not supported.") -%}
        {% endif %}
    {% endif %}
    {{ return(materialization_snapshot_default()) }}
{% endmaterialization %}

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

    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}

    merge into {{ target.render() }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.{{ columns.dbt_scd_id }} = DBT_INTERNAL_DEST.{{ columns.dbt_scd_id }}

    when matched
     {% if config.get("dbt_valid_to_current") %}
       and (DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }} or
            DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} is null)
     {% else %}
       and DBT_INTERNAL_DEST.{{ columns.dbt_valid_to }} is null
     {% endif %}
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set {{ columns.dbt_valid_to }} = DBT_INTERNAL_SOURCE.{{ columns.dbt_valid_to }}

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({% for insert_col in insert_cols -%}
            DBT_INTERNAL_SOURCE.{{ insert_col }}
            {%- if not loop.last %}, {% endif %}
            {%- endfor %})

{% endmacro %}

/*
    Overridden macro which builds a staging table for snapshot.

    As there is no such thing as a temporary table in Trino, such staging table is removed
    on cleanup (see trino__post_snapshot macro above). But it may happen that something goes
    wrong (like in `merge` statement), dbt fails and this 'temporary' table still exists.
    This macro takes care of it.
 */
{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}

    {{ drop_relation_if_exists(temp_relation) }}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(True, temp_relation, select) }}
    {% endcall %}

    {% do return(temp_relation) %}
{% endmacro %}
