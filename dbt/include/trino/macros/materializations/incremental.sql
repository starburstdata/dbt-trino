{% macro dbt_trino_validate_get_incremental_strategy(raw_strategy) %}
  {#-- Validate the incremental strategy #}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: 'append', 'insert_overwrite'
  {%- endset %}

  {% if raw_strategy not in ['append', 'insert_overwrite'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}


{% macro dbt_trino_get_insert_overwrite_sql(source_relation, target_relation, unique_key) %}

    {% if unique_key %}
    delete from {{ target_relation }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ source_relation }}
        );
    {% endif %}

    {{ dbt_trino_get_insert_into_sql(source_relation, target_relation) }}
{% endmacro %}


{% macro dbt_trino_get_insert_into_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into {{ target_relation }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}



{% macro dbt_trino_get_incremental_sql(strategy, source, target, unique_key) %}
  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ dbt_trino_get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {{ dbt_trino_get_insert_overwrite_sql(source, target, unique_key) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}


{% materialization incremental, adapter='trino' -%}

  {%- set raw_strategy = config.get('incremental_strategy', default='append') -%}
  {%- set strategy = dbt_trino_validate_get_incremental_strategy(raw_strategy) -%}

  {%- set unique_key = config.get('unique_key', none) -%}

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
    {% set build_sql = dbt_trino_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
