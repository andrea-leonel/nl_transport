with source as (
    
    select * from {{ source('jaffle_shop', 'orders') }}

),

orders as (
      select 
        row_number() over (partition by user_id order by order_date, id) as user_order_seq,
        id as order_id,
        user_id as customer_id,
        status as order_status,
        order_date,
        case when 
            order_status IN ('returned','return_pending') 
            then 'returned' else 'not returned'
        end as order_return_status
      from source
)

select * from orders