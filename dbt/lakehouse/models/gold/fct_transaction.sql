{{
  config(
    materialized = 'table',
    properties = {
      "format": "'PARQUET'",
      "partitioning": "ARRAY['month(occurred_at)']"
    }
  )
}}

-- The fact table of the star schema: one row per ledger event, foreign keys to
-- the account, currency and date dimensions, additive measure in integer minor
-- units.
--
-- The `inject_break` var exists to demonstrate the controls doing their job.
-- Setting it drops FX conversions on the floor — exactly the shape of a real
-- incident, where someone adds a WHERE clause for a good local reason and
-- silently changes a reported total. Run:
--     dbt build --vars 'inject_break: true'
-- and the silver-to-gold amount reconciliation fails with the exact delta.

select
    t.event_id,
    t.ledger_entry_id,
    t.account_id,
    t.currency          as currency_code,
    t.business_date,
    t.occurred_at,
    t.direction,
    t.transaction_type,
    t.signed_amount_minor,
    t.source_system

from {{ ref('silver_transactions') }} t

{% if var('inject_break', false) %}
where t.transaction_type <> 'fx_conversion'
{% endif %}
