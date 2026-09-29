{{ config(materialized = 'table') }}

select
    currency                        as currency_code,
    2                               as minor_unit_exponent,
    count(*)                        as transaction_count,
    sum(abs(signed_amount_minor))   as gross_amount_minor

from {{ ref('silver_transactions') }}
group by currency
