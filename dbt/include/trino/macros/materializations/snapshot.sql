
{% materialization snapshot, adapter='trino' -%}
  {{ exceptions.raise_not_implemented(
    'snapshot materialization not implemented for '+adapter.type())
  }}
{% endmaterialization %}
