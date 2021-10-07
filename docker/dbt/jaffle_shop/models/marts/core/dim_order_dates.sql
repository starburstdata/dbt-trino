{{
    config(materialized = 'incremental')
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
