
{% macro trino__get_catalog(information_schema, schemas) -%}
    {%- call statement('catalog', fetch_result=True) -%}
    select * from (

        (
            with tables as (

                select
                    table_catalog as "table_database",
                    table_schema as "table_schema",
                    table_name as "table_name",
                    table_type as "table_type",
                    null as "table_owner"

                from {{ information_schema }}.tables
                where
                    table_schema != 'information_schema'
                    and
                    table_schema in ('{{ schemas | join("','") | lower }}')

            ),

            columns as (

                select
                    table_catalog as "table_database",
                    table_schema as "table_schema",
                    table_name as "table_name",
                    null as "table_comment",

                    column_name as "column_name",
                    ordinal_position as "column_index",
                    data_type as "column_type",
                    null as "column_comment"

                from {{ information_schema }}.columns
                where
                    table_schema != 'information_schema'
                    and
                    table_schema in ('{{ schemas | join("','") | lower }}')

            )

            select *
            from tables
            join columns using ("table_database", "table_schema", "table_name")
            order by "column_index"
        )

    )
  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
