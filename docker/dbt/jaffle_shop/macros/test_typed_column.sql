{% test column_data_type(model, expression, column_name, expected_type) %}
with meet_condition as (
    select {{ column_name }} from {{ model }} limit 1
)

select
    typeof({{ column_name }}) as type_of_column
from meet_condition where typeof({{ column_name }}) NOT LIKE '{{ expected_type }}'

{% endtest %}