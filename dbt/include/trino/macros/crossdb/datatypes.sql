{% macro trino__type_float() %}
    double
{% endmacro %}

{% macro trino__type_string() %}
    varchar
{% endmacro %}

{% macro trino__type_numeric() %}
    DECIMAL(28, 6)
{% endmacro %}

{%- macro trino__type_int() -%}
    integer
{%- endmacro -%}
