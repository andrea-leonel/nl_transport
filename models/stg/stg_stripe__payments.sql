select 
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    round(amount/100,0) as amount,
    status,
    created as created_at,
    _batched_at as batched_at

from {{ source('jaffle_shop_stripe', 'payment') }}