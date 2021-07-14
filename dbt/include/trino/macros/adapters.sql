
-- - get_catalog
-- - list_relations_without_caching
-- - get_columns_in_relation

{% macro trino__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          case when regexp_like(data_type, 'varchar\(\d+\)') then 'varchar'
               else data_type
          end as data_type,
          case when regexp_like(data_type, 'varchar\(\d+\)') then
                  from_base(regexp_extract(data_type, 'varchar\((\d+)\)', 1), 10)
               else NULL
          end as character_maximum_length,
          NULL as numeric_precision,
          NULL as numeric_scale

      from
      {{ relation.information_schema('columns') }}

      where
        table_name = '{{ relation.name }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema | lower }}'
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro trino__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as database,
      table_name as name,
      table_schema as schema,
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type
    from {{ relation.information_schema() }}.tables
    where table_schema = '{{ relation.schema | lower }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro trino__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
    {{ return(create_csv_table(model, agate_table)) }}
{% endmacro %}


{% macro trino__create_table_as(temporary, relation, sql) -%}
  create table
    {{ relation }}
  as (
    {{ sql }}
  );
{% endmacro %}


{% macro trino__create_view_as(relation, sql) -%}
  create or replace view
    {{ relation }}
  as
    {{ sql }}
  ;
{% endmacro %}


{% macro trino__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}


{# see this issue: https://github.com/dbt-labs/dbt/issues/2267 #}
{% macro trino__information_schema_name(database) -%}
  {%- if database -%}
    {{ database }}.INFORMATION_SCHEMA
  {%- else -%}
    INFORMATION_SCHEMA
  {%- endif -%}
{%- endmacro %}


{# On Trino, 'cascade' isn't supported so we have to manually cascade. #}
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
  {% call statement('rename_relation') -%}
    alter {{ from_relation.type }} {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}


{% macro trino__load_csv_rows(model, agate_table) %}
  {{ return(basic_load_csv_rows(model, 1000, agate_table)) }}
{% endmacro %}


{% macro trino__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct schema_name
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
