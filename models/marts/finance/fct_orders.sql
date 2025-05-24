select 
    a.customer_id,
    a.order_id,
    a.status,
    b.amount
from {{ ref('stg_jaffle_shop__orders') }} a
left join {{ ref('stg_stripe__payments') }} b on b.order_id = a.order_id