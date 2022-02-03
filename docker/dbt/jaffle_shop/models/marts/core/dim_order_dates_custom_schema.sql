-- same as dim_orders but stores the model in a custom schema
-- meant to ensure that Trino incremental materialization works as expected
-- for custom schemas

{{
    config(materialized = 'incremental', schema='custom_schema')
}}

with orders as (

    select * from {{ ref('stg_orders') }}

)

select distinct order_date
from orders

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where order_date > (select max(order_date) from {{ this }})
{% endif %}
