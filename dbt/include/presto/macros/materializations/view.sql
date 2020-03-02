{% materialization view, adapter='presto' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
