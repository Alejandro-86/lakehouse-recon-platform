{{ config(materialized = 'table') }}

select
    business_date,
    year(business_date)         as calendar_year,
    month(business_date)        as calendar_month,
    day_of_week(business_date)  as day_of_week,
    day_of_week(business_date) in (6, 7) as is_weekend

from (select distinct business_date from {{ ref('silver_transactions') }})
