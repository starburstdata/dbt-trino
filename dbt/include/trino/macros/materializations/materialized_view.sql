{% materialization trino_materialized_view, adapter="trino" %} 
{%- set target_relation = this %}
{%- set existing_relation = load_relation(this) -%}
{%- set config_grace_period = config.require('grace_period') %}
{%- set config_max_import_duration = config.require('max_import_duration') %}
{%- set config_drop_any_existing = config.require('drop_any_existing') %}
{%- set config_cron = config.get('cron') %}
{%- set config_refresh_interval = config.get('refresh_interval') %}


{{ run_hooks(pre_hooks) }}

{% if existing_relation is none %}

    {{ log("No existing view found, creating materialized view...", info=true) }}
    {%- set build_sql = build_materialized_view(target_relation,config_grace_period,config_max_import_duration,config_cron,config_refresh_interval) %}

{% else %}
    {%- if existing_relation.is_view %}
        {%-set existing_type = 'view'%}
    {%- elif existing_relation.is_table %}
        {%-set existing_type = 'table'%}
    {%- elif existing_relation.is_cte %}
        {%-set existing_type = 'CTE'%}
    {% endif %}

    {%- if existing_type is not none and config_drop_any_existing == 'False' %}
        {{ log("Found a " ~ existing_type ~ " with same name.", info=true) }}
        {%- set build_sql = build_materialized_view(target_relation,config_grace_period,config_max_import_duration,config_cron,config_refresh_interval) %}

    {% elif existing_type is not none and config_drop_any_existing == 'True' and existing_type != 'table'%} -- theres an issue with starburst thinking that the materialised view is a table in the information schema, hence the logic on this line
        {%- set drop_existing_sql = "DROP " ~ existing_type ~ " IF EXISTS " ~ existing_relation %}
        {%- set build_sql = build_materialized_view(target_relation,config_grace_period,config_max_import_duration,config_cron,config_refresh_interval) %}
        {%- call statement('drop_existing') -%}
            {{ drop_existing_sql }} 
        {% endcall %}

    {% else %}
        {{ log("Found something with same name, trying to create materialized view anyway...", info=true) }}
        {%- set build_sql = build_materialized_view(target_relation,config_grace_period,config_max_import_duration,config_cron,config_refresh_interval) %}
    {% endif %}
    
{% endif %}

-- Sends the sql command to starburst
{%- call statement('main') -%}
    {{ build_sql }} 
{% endcall %}

{{ run_hooks(pre_hooks) }}

{{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

---------------- Macros -------------------

{%- macro build_materialized_view(target_relation,config_grace_period,config_max_import_duration,config_cron,config_refresh_interval) -%}
{% if config_cron is not none and config_refresh_interval is not none %}
    {{ log("Found config for CRON and Refresh Interval, error will be thrown as only 1 is allowed", info=true) }}
    {{ exceptions.raise_compiler_error("Invalid config: only CRON or refresh interval allowed - " ~ config_cron ~ " - " ~ config_refresh_interval) }}

{% else %}  
    {% if config_cron is not none and config_refresh_interval is none %}
        {%- set schedule_to_use ="cron = \'" ~  config_cron  ~ "\'," %}
    {% else %}
        {%- set schedule_to_use ="refresh_interval = \'" ~  config_refresh_interval  ~ "\'," %}
    {% endif %}

    {%- set sqlcode= "CREATE OR REPLACE MATERIALIZED VIEW " ~  target_relation ~ " WITH (
    " ~ schedule_to_use ~ 
    "grace_period = \'" ~  config_grace_period  ~ "\',
    max_import_duration = \'" ~  config_max_import_duration  ~ "\'
    ) AS" ~ sql
    %}

    {{sqlcode}} 

{% endif %}
{%- endmacro -%}