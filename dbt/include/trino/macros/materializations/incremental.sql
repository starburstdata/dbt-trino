{% macro validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="append") -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'append', 'delete+insert', 'merge'
  {%- endset %}
  {% if strategy not in ['append', 'delete+insert', 'merge'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}
  {% if strategy == 'append' %}
    {% do return(get_append_sql(target_relation, tmp_relation, dest_columns)) %}
  {% elif strategy == 'delete+insert' %}
    {% do return(get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'merge' %}
    {% do return(get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns, predicates=none)) %}
  {% endif %}
{% endmacro %}

{% macro get_append_sql(target_relation, tmp_relation, dest_columns) %}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into {{ target_relation }}
    select {{dest_cols_csv}} from {{ tmp_relation }};

    drop table if exists {{ tmp_relation }};
{% endmacro %}

{% macro trino__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{ target }}
            where
                {% for key in unique_key %}
                    {{ target }}.{{ key }} in (select {{ key }} from {{ source }})
                    {{ "and " if not loop.last }}
                {% endfor %}
            ;
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select {{ unique_key }}
                from {{ source }}
            );

        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )
{%- endmacro %}

{% macro trino__get_merge_sql(target, source, unique_key, dest_columns, predicates=none) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set dest_cols_csv_source = dest_cols_csv.split(', ') -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match -%}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {%- endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match -%}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {%- endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}

        {{ sql_header if sql_header is not none }}

        merge into {{ target }} as DBT_INTERNAL_DEST {{ "\n" }}using {{ source }} as DBT_INTERNAL_SOURCE on
        {{ predicates | join(' and
        ') }}

        {%- if unique_key %}

        when matched then update set
            {% for column_name in update_columns -%}
                {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
                {%- if not loop.last %},
            {% endif %}
            {%- endfor %}
        {%- endif %}

        when not matched then insert
            ({{ dest_cols_csv }})
        values
            ({% for dest_cols in dest_cols_csv_source -%}
                DBT_INTERNAL_SOURCE.{{ dest_cols }}
                {%- if not loop.last %}, {% endif %}
            {%- endfor %})

    {% else %}
        insert into {{ target }} ({{ dest_cols_csv }})
        (
            select {{ dest_cols_csv }}
            from {{ source }}
        )
    {% endif %}
{% endmacro %}

{% materialization incremental, adapter='trino' -%}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(this) -%}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {%- set strategy = validate_get_incremental_strategy(config) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {{ run_hooks(pre_hooks) }}

  -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% set drop_tmp_relation_sql = "drop table if exists " ~  tmp_relation %}
    {% do run_query(drop_tmp_relation_sql) %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}
    {% set build_sql = get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {% set should_revoke =
   should_revoke(existing_relation.is_table, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
