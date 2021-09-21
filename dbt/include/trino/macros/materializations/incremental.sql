{% macro dbt_trino_get_append_sql(tmp_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into {{ target_relation }}
    select {{dest_cols_csv}} from {{ tmp_relation.include(database=false, schema=false) }};

    drop table if exists {{ tmp_relation }};

{% endmacro %}

{% materialization incremental, adapter='trino' -%}

  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% set drop_tmp_relation_sql = "drop table if exists " ~  tmp_relation %}
    {% do run_query(drop_tmp_relation_sql) %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% set build_sql = dbt_trino_get_append_sql(tmp_relation, target_relation) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
