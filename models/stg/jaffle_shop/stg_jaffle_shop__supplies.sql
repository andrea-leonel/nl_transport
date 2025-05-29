with supplies as(

    select * from {{ source('jaffle_shop', 'supplies') }}
)

select * from supplies