with payments as (

    select * from {{ ref('stg_stripe__payments') }}
),

total_success as (

    select
    sum(case when status = 'success' then amount else 0 end) as total_success_amount
    from payments
)

select * from total_success
