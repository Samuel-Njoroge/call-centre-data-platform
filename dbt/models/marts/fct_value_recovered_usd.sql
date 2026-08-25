-- See this model's description (dbt docs) for the full rationale.
{{ config(
    materialized='incremental',
    unique_key='value_recovered_id',
    incremental_strategy='delete+insert',
    sort=['call_date']
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
