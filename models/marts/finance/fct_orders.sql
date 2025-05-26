{{
    config(
        materialized='ephemeral'
    )
}}

select 
    a.customer_id,
    a.order_id,
    a.order_status,
    b.order_value_dollars
from {{ ref('stg_jaffle_shop__orders') }} a
left join {{ ref('stg_stripe__payments') }} b on b.order_id = a.order_id