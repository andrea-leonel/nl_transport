{{
    config(
        severity='warn'
    )
}}

with payments as(

    select * from {{ ref("stg_stripe__payments")}}
)

select order_id, sum(order_value_dollars) as total_amount from payments
group by order_id
having sum(order_value_dollars) < 0


