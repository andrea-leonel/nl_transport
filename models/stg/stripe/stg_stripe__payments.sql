with source as (
    
    select * from {{ source('jaffle_shop_stripe', 'payment') }}

),

payments as (
    select
        id,
        orderid as order_id,
        paymentmethod as payment_method,
        status as payment_status,
        round(amount/100.0,2) as order_value_dollars,
        created as created_at,
        _batched_at as batched_at
    from source
)

select * from payments

