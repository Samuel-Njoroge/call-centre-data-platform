-- See this model's description (dbt docs) for the full rationale.
{{ config(
    materialized='incremental',
    unique_key='attribution_id',
    incremental_strategy='delete+insert',
    dist='contract_id',
    sort=['disposed_at_utc']
) }}

with dispositions as (
    select
        call_log_id,
        market,
        contract_id,
        disposed_at_utc
    from {{ ref('stg_atlas__dispositions') }}
    where contract_id is not null

    {% if is_incremental() %}
    and disposed_at_utc >= (
        select coalesce(max(disposed_at_utc), timestamp '1900-01-01') - interval '4 days'
        from {{ this }}
    )
    {% endif %}
),

payments as (
    select
        contract_id,
        pay_timestamp_utc,
        currency,
        amount_local
    from {{ ref('stg_atlas__payments') }}
    where contract_id is not null
      and amount_local > 0  -- non-positive amounts can't be "money recovered"

    {% if is_incremental() %}
    and pay_timestamp_utc >= (
        select coalesce(max(disposed_at_utc), timestamp '1900-01-01') - interval '4 days'
        from {{ this }}
    )
    {% endif %}
),

candidate_matches as (
    select
        p.contract_id,
        p.pay_timestamp_utc,
        p.currency,
        p.amount_local,
        d.call_log_id,
        row_number() over (
            partition by p.contract_id, p.pay_timestamp_utc, p.amount_local
            order by d.disposed_at_utc desc, d.call_log_id desc  -- deterministic tiebreak, see docs
        ) as _nearest_preceding_call_rank
    from payments p
    inner join dispositions d
        on p.contract_id = d.contract_id
        and p.pay_timestamp_utc >= d.disposed_at_utc
        and p.pay_timestamp_utc <= d.disposed_at_utc + interval '3 days'
),

attributed_payments as (
    select call_log_id, pay_timestamp_utc, currency, amount_local
    from candidate_matches
    where _nearest_preceding_call_rank = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['d.call_log_id', 'ap.pay_timestamp_utc', 'ap.amount_local']) }} as attribution_id,
    d.call_log_id,
    d.market,
    d.contract_id,
    d.disposed_at_utc,
    d.disposed_at_utc + interval '3 days' as window_closes_at_utc,
    (current_timestamp >= d.disposed_at_utc + interval '3 days') as is_window_closed,
    ap.pay_timestamp_utc,
    ap.currency,
    ap.amount_local,
    (ap.call_log_id is not null) as is_paid_post_call
from dispositions d
left join attributed_payments ap
    on d.call_log_id = ap.call_log_id
