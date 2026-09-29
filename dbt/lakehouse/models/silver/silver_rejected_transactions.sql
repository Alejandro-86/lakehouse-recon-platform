{{ config(materialized = 'table') }}

-- The quarantine table. Every row silver refused, with the reason attached, so
-- a bronze-to-silver row-count break can be explained in seconds instead of
-- being investigated for an afternoon. Rejects are evidence, not litter.

with ranked as (

    select
        *,
        row_number() over (partition by event_id order by ingested_at asc) as arrival_rank

    from {{ source('bronze', 'transactions_raw') }}

)

select
    event_id,
    ledger_entry_id,
    account_id,
    amount_minor,
    signed_amount_minor,
    currency,
    occurred_at,
    ingested_at,
    batch_id,
    case
        when event_id is null              then 'missing_event_id'
        when currency is null              then 'missing_currency'
        when signed_amount_minor = 0       then 'zero_amount'
        else 'unknown'
    end as rejection_reason

from ranked
where arrival_rank = 1
  and (event_id is null or currency is null or signed_amount_minor = 0)
