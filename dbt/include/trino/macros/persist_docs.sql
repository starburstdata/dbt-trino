{% macro trino__persist_docs(relation, model, for_relation, for_columns) -%}
  {% set do_relation = for_relation and config.persist_relation_docs() %}
  {% set do_columns = for_columns and config.persist_column_docs() %}

  {% if do_relation and model.description %}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {% endif %}

  {% if do_columns and model.columns %}
    {% do run_query(alter_column_comment(relation, model.columns)) %}
  {% endif %}

  {% if target.starburst_url %}
    {% do adapter.persist_starburst_docs(relation, model, do_relation, do_columns) %}
  {% endif %}
{%- endmacro %}
