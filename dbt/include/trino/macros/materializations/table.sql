{% materialization table, adapter = 'trino' %}
  {%- set on_table_exists = config.get('on_table_exists', 'rename') -%}
  {% if on_table_exists not in ['rename', 'drop'] %}
      {%- set log_message = 'Invalid value for on_table_exists (%s) specified. Setting default value (%s).' % (on_table_exists, 'rename') -%}
      {% do log(log_message) %}
      {%- set on_table_exists = 'rename' -%}
  {% endif %}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {% if on_table_exists == 'rename' %}
      {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                          schema=schema,
                                                          database=database,
                                                          type='table') -%}

      {%- set backup_relation_type = 'table' if old_relation is none else old_relation.type -%}
      {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                    schema=schema,
                                                    database=database,
                                                    type=backup_relation_type) -%}

      {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
      {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

        -- drop the temp relations if they exists for some reason
      {{ adapter.drop_relation(intermediate_relation) }}
      {{ adapter.drop_relation(backup_relation) }}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% if on_table_exists == 'rename' %}
      -- build modeldock
      {% call statement('main') -%}
        {{ create_table_as(False, intermediate_relation, sql) }}
      {%- endcall %}

      -- cleanup
      {% if old_relation is not none %}
          {{ adapter.rename_relation(old_relation, backup_relation) }}
      {% endif %}

      {{ adapter.rename_relation(intermediate_relation, target_relation) }}

      -- finally, drop the existing/backup relation after the commit
      {{ drop_relation_if_exists(backup_relation) }}

  {% elif on_table_exists == 'drop' %}
      -- cleanup
      {%- if old_relation is not none -%}
          {{ adapter.drop_relation(old_relation) }}
      {%- endif -%}

      -- build model
      {% call statement('main') -%}
        {{ create_table_as(False, target_relation, sql) }}
      {%- endcall %}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
