-- Metric 3. One row per (call_date, market) with the total attributed
-- payment value converted to USD -- call_date here is the PAYMENT date
-- (when the money was actually recovered), not the call date, since that's
-- the natural date to report "value recovered on this day" against.
--
-- FX rate used: the MOST RECENTLY FETCHED rate for that currency, not the
-- true historical rate on the payment's date. exchangerate-api.com's free
-- tier only exposes current rates, and our own rate history only starts
-- accumulating from whenever this pipeline first ran -- so for this sample
-- (payments span 2026-08-04 to 2026-08-07, a single fetched rate dated
-- 2026-08-17) every conversion uses that one rate as an approximation. This
-- is a real limitation, not a rounding detail: going forward, once daily
-- fetches have accumulated a rate history, this should switch to matching
-- each payment to the rate fetched on (or nearest to) its own date rather
-- than always using the latest.
--
-- Incremental with the same 4-day lookback as int_calls_payment_attribution,
-- filtered on pay_timestamp_utc (the date this mart's grain is built on).

{{ config(
    materialized='incremental',
    unique_key='value_recovered_id',
    incremental_strategy='delete+insert'
) }}

with payments_to_convert as (
    select
        market,
        pay_timestamp_utc,
        currency,
        amount_local
    from {{ ref('int_calls_payment_attribution') }}
    where pay_timestamp_utc is not null

    {% if is_incremental() %}
    and pay_timestamp_utc >= (
        select coalesce(max(call_date), date '1900-01-01') - interval '4 days'
        from {{ this }}
    )
    {% endif %}
),

latest_rate_per_currency as (
    select currency, local_units_per_usd, rate_date
    from (
        select
            currency,
            local_units_per_usd,
            rate_date,
            row_number() over (partition by currency order by rate_date desc) as _rn
        from {{ ref('stg_fx__rates') }}
    ) ranked
    where _rn = 1
),

aggregated as (
    select
        cast(p.pay_timestamp_utc as date) as call_date,
        p.market,
        max(r.rate_date) as fx_rate_date_used,
        sum(p.amount_local / r.local_units_per_usd) as value_recovered_usd
    from payments_to_convert p
    left join latest_rate_per_currency r
        on p.currency = r.currency
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['call_date', 'market']) }} as value_recovered_id,
    *
from aggregated
