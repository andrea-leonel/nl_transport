with items as(

    select * from {{ source('jaffle_shop', 'items') }}
)

select * from items