{{
  config(
    materialized = 'table',
    properties = {
      "format": "'PARQUET'",
      "partitioning": "ARRAY['month(occurred_at)']",
      "sorted_by": "ARRAY['occurred_at']"
    }
  )
}}

-- Silver does exactly three things, and each one is a reconciliation control's
-- counterpart:
--   1. deduplicate on the event's idempotency key (first arrival wins),
--   2. reject structurally invalid rows into an auditable quarantine model,
--   3. normalise types, leaving amounts as integer minor units so downstream
--      sums stay exact.
-- It deliberately does NOT filter on business rules. A silver layer that
-- quietly drops rows it dislikes is how a bronze-to-silver amount break gets
-- explained away as "expected."

with ranked as (

    select
        *,
        row_number() over (
            partition by event_id
            order by ingested_at asc
        ) as arrival_rank

    from {{ source('bronze', 'transactions_raw') }}

),

deduplicated as (

    select * from ranked where arrival_rank = 1

)

select
    event_id,
    ledger_entry_id,
    account_id,
    counterparty_account_id,
    amount_minor,
    signed_amount_minor,
    upper(currency)                as currency,
    direction,
    transaction_type,
    occurred_at,
    date(occurred_at)              as business_date,
    source_system,
    ingested_at,
    batch_id

from deduplicated
where event_id is not null
  and currency is not null
  and signed_amount_minor <> 0
