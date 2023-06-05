{% macro trino__get_catalog(information_schema, schemas) -%}
    {%- call statement('catalog', fetch_result=True) -%}
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

                column_name as "column_name",
                ordinal_position as "column_index",
                data_type as "column_type",
                comment as "column_comment"

            from {{ information_schema }}.columns
            where
                table_schema != 'information_schema'
                and
                table_schema in ('{{ schemas | join("','") | lower }}')

        ),

        table_comment as (
            {%- for schema in schemas %}
            select
                catalog_name as "table_database",
                schema_name as "table_schema",
                table_name as "table_name",
                comment as "table_comment"

            from system.metadata.table_comments
            where
                catalog_name = '{{ information_schema.database }}'
                and
                schema_name != 'information_schema'
                and
                schema_name = '{{ schema | lower }}'
            {% if not loop.last %}
            union all
            {% endif %}
            {%- endfor %}
        )

        select
            table_database,
            table_schema,
            table_name,
            table_type,
            table_owner,
            column_name,
            column_index,
            column_type,
            column_comment,
            table_comment
        from tables
        join columns using ("table_database", "table_schema", "table_name")
        join table_comment using ("table_database", "table_schema", "table_name")
        order by "column_index"

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
