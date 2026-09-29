{{ config(materialized = 'table') }}

-- Conformed account dimension. Built from the full silver key space on purpose:
-- narrowing it (to "active" accounts, say) would orphan fact rows and quietly
-- shrink every total an analyst produces by joining to it.

select
    account_id,
    min(occurred_at)                          as first_seen_at,
    max(occurred_at)                          as last_seen_at,
    count(*)                                  as transaction_count,
    count(distinct currency)                  as currency_count,
    sum(signed_amount_minor)                  as net_amount_minor

from {{ ref('silver_transactions') }}
group by account_id
