{% macro trino__current_timestamp() -%}
    cast(current_timestamp as timestamp(6) with time zone)
{%- endmacro %}

{% macro trino__snapshot_string_as_time(timestamp) %}
    {%- set result = "timestamp '" ~ timestamp ~ "'" -%}
    {{ return(result) }}
{% endmacro %}
