{% macro trino__array_construct(inputs, data_type) -%}
    {%- if not inputs -%}
    NULL
    {%- else -%}
    ARRAY[ {{ inputs|join(' , ') }} ]
    {%- endif -%}
{%- endmacro %}
