{% macro trino__get_batch_size() %}
  {{ return(1000) }}
{% endmacro %}
