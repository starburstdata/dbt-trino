{% macro dbt_trino_validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="append") -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'append', 'delete+insert'
  {%- endset %}
  {% if strategy not in ['append', 'delete+insert'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro dbt_trino_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}
  {% if strategy == 'append' %}
    {% do return(get_append_sql(target_relation, tmp_relation, dest_columns)) %}
  {% elif strategy == 'delete+insert' %}
    {% do return(get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'merge' %}
    {#-- Not yet supported by trino (blocked in dbt_trino_validate_get_incremental_strategy macro) --#}
    {% do return(get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}

{% macro get_append_sql(target_relation, tmp_relation, dest_columns) %}

    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into {{ target_relation }}
    select {{dest_cols_csv}} from {{ tmp_relation.include(database=true, schema=true) }};

    drop table if exists {{ tmp_relation }};

{% endmacro %}

{% materialization incremental, adapter='trino' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_trino_validate_get_incremental_strategy(config) -%}

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
    {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% set build_sql = dbt_trino_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}
  
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
