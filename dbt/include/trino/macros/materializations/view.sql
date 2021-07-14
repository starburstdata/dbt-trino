{% materialization view, adapter='trino' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
