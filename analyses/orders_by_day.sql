{% set order_statuses = ['placed','shipped','completed','return_pending','returned'] %}

with orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}
),

daily as (

    select
    order_date,
    count(*) as orders_num,
    {% for status in ['placed','shipped','completed','return_pending','returned'] %}
    sum(case when status = '{{status}}' then 1 end) as orders_num_{{status}},
    {% endfor %}
    from orders
    group by order_date

)

select * from daily