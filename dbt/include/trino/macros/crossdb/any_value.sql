{% macro default__any_value(expression) -%}
    min({{ expression }})
{%- endmacro %}
