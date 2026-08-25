-- See this model's description (dbt docs) for the full rationale.
{{ config(
    materialized='incremental',
    unique_key='call_log_id',
    incremental_strategy='delete+insert',
    sort=['disposed_at_utc']
) }}

select
    call_log_id,
    market,
    contract_id,
    disposed_at_utc,
    window_closes_at_utc,
    is_window_closed,
    bool_or(is_paid_post_call) as is_paid_post_call,
    sum(case when pay_timestamp_utc is not null then 1 else 0 end) as attributed_payment_count
from {{ ref('int_calls_payment_attribution') }}

{% if is_incremental() %}
where disposed_at_utc >= (
    select coalesce(max(disposed_at_utc), timestamp '1900-01-01') - interval '4 days'
    from {{ this }}
)
{% endif %}

group by 1, 2, 3, 4, 5, 6
