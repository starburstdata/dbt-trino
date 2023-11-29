
-- - get_catalog
-- - list_relations_without_caching
-- - get_columns_in_relation

{% macro trino__get_columns_in_relation(relation) -%}
  {%- set sql -%}
    describe {{ relation }}
  {%- endset -%}
  {%- set result = run_query(sql) -%}

  {% set maximum = 10000 %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many columns in relation {{ relation }}! dbt can only get
      information about relations with fewer than {{ maximum }} columns.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}

  {% set columns = [] %}
  {% for row in result %}
    {% do columns.append(api.Column.from_description(row['Column'].lower(), row['Type'])) %}
  {% endfor %}
  {% do return(columns) %}
{% endmacro %}


{% macro trino__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      t.table_catalog as database,
      t.table_name as name,
      t.table_schema as schema,
      case when mv.name is not null then 'materialized_view'
           when t.table_type = 'BASE TABLE' then 'table'
           when t.table_type = 'VIEW' then 'view'
           else t.table_type
      end as table_type
    from {{ relation.information_schema() }}.tables t
    left join (
            select * from system.metadata.materialized_views
            where catalog_name = '{{ relation.database | lower }}'
              and schema_name = '{{ relation.schema | lower }}') mv
          on mv.catalog_name = t.table_catalog and mv.schema_name = t.table_schema and mv.name = t.table_name
    where t.table_schema = '{{ relation.schema | lower }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro trino__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
    {{ return(create_csv_table(model, agate_table)) }}
{% endmacro %}


{% macro trino__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {%- set _properties = config.get('properties') -%}

  {% set sql %}
    create table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    ) {{ properties(_properties) }}
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}

{% macro properties(properties) %}
  {%- if properties is not none -%}
      WITH (
          {%- for key, value in properties.items() -%}
            {{ key }} = {{ value }}
            {%- if not loop.last -%}{{ ',\n  ' }}{%- endif -%}
          {%- endfor -%}
      )
  {%- endif -%}
{%- endmacro -%}

{% macro comment(comment) %}
  {%- set persist_docs = model['config'].get('persist_docs') -%}
  {%- if persist_docs -%}
    {%- set persist_relation = persist_docs.get('relation') -%}
    {%- if persist_relation and comment is not none and comment|length > 0 -%}
        comment '{{ comment | replace("'", "''") }}'
    {%- endif -%}
  {%- endif -%}
{%- endmacro -%}

{% macro trino__create_table_as(temporary, relation, sql, replace=False) -%}
  {%- set _properties = config.get('properties') -%}

  {%- if replace -%}
    {%- set or_replace = ' or replace' -%}
  {%- else -%}
    {%- set or_replace = '' -%}
  {%- endif -%}

  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}

  create{{ or_replace }} table
    {{ relation }}
    {{ get_table_columns_and_constraints() }}
    {{ get_assert_columns_equivalent(sql) }}
    {%- set sql = get_select_subquery(sql) %}
    {{ comment(model.get('description')) }}
    {{ properties(_properties) }}
  ;

  insert into {{ relation }}
    (
      {{ sql }}
    )
  ;

  {%- else %}

    create{{ or_replace }} table {{ relation }}
      {{ comment(model.get('description')) }}
      {{ properties(_properties) }}
    as (
      {{ sql }}
    );

   {%- endif %}
{% endmacro %}


{% macro trino__create_view_as(relation, sql) -%}
  {%- set view_security = config.get('view_security', 'definer') -%}
  {%- if view_security not in ['definer', 'invoker'] -%}
      {%- set log_message = 'Invalid value for view_security (%s) specified. Setting default value (%s).' % (view_security, 'definer') -%}
      {% do log(log_message) %}
      {%- set on_table_exists = 'definer' -%}
  {% endif %}
  create or replace view
    {{ relation }}
  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  security {{ view_security }}
  as
    {{ sql }}
  ;
{% endmacro %}


{%- macro trino__get_drop_sql(relation) -%}
  {% set relation_type = relation.type|replace("_", " ") %}
    drop {{ relation_type }} if exists {{ relation }}
{% endmacro %}


{# see this issue: https://github.com/dbt-labs/dbt/issues/2267 #}
{% macro trino__information_schema_name(database) -%}
  {%- if database -%}
    {{ database }}.INFORMATION_SCHEMA
  {%- else -%}
    INFORMATION_SCHEMA
  {%- endif -%}
{%- endmacro %}


{# On Trino, 'cascade' is not supported so we have to manually cascade. #}
{% macro trino__drop_schema(relation) -%}
  {% for row in list_relations_without_caching(relation) %}
    {% set rel_db = row[0] %}
    {% set rel_identifier = row[1] %}
    {% set rel_schema = row[2] %}
    {% set rel_type = api.Relation.get_relation_type(row[3]) %}
    {% set existing = api.Relation.create(database=rel_db, schema=rel_schema, identifier=rel_identifier, type=rel_type) %}
    {% do drop_relation(existing) %}
  {% endfor %}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation }}
  {% endcall %}
{% endmacro %}


{% macro trino__rename_relation(from_relation, to_relation) -%}
  {% set from_relation_type = from_relation.type|replace("_", " ") %}
  {% call statement('rename_relation') -%}
    alter {{ from_relation_type }} {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}


{% macro trino__alter_relation_comment(relation, relation_comment) -%}
  comment on {{ relation.type }} {{ relation }} is '{{ relation_comment | replace("'", "''") }}';
{% endmacro %}


{% macro trino__alter_column_comment(relation, column_dict) %}
  {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
  {% for column_name in column_dict if (column_name in existing_columns) %}
    {% set comment = column_dict[column_name]['description'] %}
    {%- if comment|length -%}
      comment on column {{ relation }}.{{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }} is '{{ comment | replace("'", "''") }}';
    {%- else -%}
      comment on column {{ relation }}.{{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }} is null;
    {%- endif -%}
  {% endfor %}
{% endmacro %}


{% macro trino__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select schema_name
    from {{ information_schema_name(database) }}.schemata
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro trino__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
        select count(*)
        from {{ information_schema }}.schemata
        where catalog_name = '{{ information_schema.database }}'
          and schema_name = '{{ schema | lower }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro trino__get_binding_char() %}
  {%- if target.prepared_statements_enabled|as_bool -%}
    {{ return('?') }}
  {%- else -%}
    {{ return('%s') }}
  {%- endif -%}
{% endmacro %}


{% macro trino__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}
  {% if remove_columns is none %}
    {% set remove_columns = [] %}
  {% endif %}

  {% for column in add_columns %}
    {% set sql -%}
      alter {{ relation.type }} {{ relation }} add column {{ column.name }} {{ column.data_type }}
    {%- endset -%}
    {% do run_query(sql) %}
  {% endfor %}

  {% for column in remove_columns %}
    {% set sql -%}
      alter {{ relation.type }} {{ relation }} drop column {{ column.name }}
    {%- endset -%}
    {% do run_query(sql) %}
  {% endfor %}
{% endmacro %}


{% macro create_or_replace_view() %}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set target_relation = api.Relation.create(
      identifier=identifier, schema=schema, database=database,
      type='view') -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  -- If there is another object delete it
  {%- if old_relation is not none and not old_relation.is_view -%}
    {{ handle_existing_table(should_full_refresh(), old_relation) }}
  {%- endif -%}

  -- build model
  {% call statement('main') -%}
    {{ get_create_view_as_sql(target_relation, sql) }}
  {%- endcall %}

  {% set should_revoke = should_revoke(exists_as_view, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=True) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmacro %}

{% macro trino__alter_column_type(relation, column_name, new_column_type) %}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} add column {{ adapter.quote(tmp_column) }} {{ new_column_type }};
    update {{ relation }} set {{ adapter.quote(tmp_column) }} = CAST({{ adapter.quote(column_name) }} AS {{ new_column_type }});
    alter table {{ relation }} drop column {{ adapter.quote(column_name) }};
    alter table {{ relation }} rename column {{ adapter.quote(tmp_column) }} to {{ adapter.quote(column_name) }}
  {% endcall %}
{% endmacro %}
