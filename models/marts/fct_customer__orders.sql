with customers as (

    select * from {{ ref('stg_jaffle_shop__customers')}}
),

orders as (

    select * from {{ ref('stg_jaffle_shop__orders')}}
),

payments as (

    select * from {{ ref("stg_stripe__payments")}}
    where payment_status != 'fail'
),

non_returned as (

    select 
        customer_id,
        min(order_date) as first_non_returned_order_date,
        max(order_date) as latest_non_returned_order_date,
        count(distinct orders.order_id) as non_returned_order_count,
        sum(payments.order_value_dollars) as total_lifetime_value,
        sum(payments.order_value_dollars)/count(distinct orders.order_id) as avg_non_returned_order_value,
    from orders
    left join payments on payments.order_id = orders.order_id
    where order_return_status = 'not returned' 
    group by customer_id
),

all_orders as (

    select 
        customer_id,
        min(order_date) as first_order_date,
        count(distinct order_id) as order_count,
        array_agg(distinct order_id) as order_ids,
    from orders
    group by customer_id

),

customer_order_history as (
    select 
        customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,
        non_returned.first_non_returned_order_date,
        non_returned.latest_non_returned_order_date,
        all_orders.order_count,
        non_returned.non_returned_order_count,
        non_returned.total_lifetime_value,
        non_returned.avg_non_returned_order_value,
        all_orders.order_ids
    from customers
    left join non_returned on non_returned.customer_id = customers.customer_id
    left join all_orders on all_orders.customer_id = customers.customer_id
),

final as (
    select 
        orders.order_id,
        {% set customer_columns = ['customer_id','surname','givenname'] %}
        {%- for column in customer_columns -%}
        customers.{{column}},
        {% endfor -%}
        {% set history_columns = ['first_non_returned_order_date','order_count','total_lifetime_value'] %}
        {%- for column in history_columns -%}
        customer_order_history.{{column}},
        {% endfor -%}
        payments.order_value_dollars,
        order_status,
        payments.payment_status
    from orders
    join customers on orders.customer_id = customers.customer_id
    join customer_order_history on orders.customer_id = customer_order_history.customer_id
    left outer join payments on orders.order_id = payments.order_id
)

select * from final
order by order_id asc