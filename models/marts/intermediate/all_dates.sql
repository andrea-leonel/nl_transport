{{
    config(
        materialized='table'
    )
}}

with date_spine as (

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2020-01-01' as date)",
    end_date="cast('2020-12-31' as date)"
   )
}}

)

select * from date_spine