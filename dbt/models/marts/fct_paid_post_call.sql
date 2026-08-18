-- Metric 2. One row per assessable disposition (call_log_id) -- dispositions
-- with a null contract_id are excluded upstream (see docs/WRITEUP.md), not
-- counted as unpaid.
--
-- is_window_closed is the moving-window handling the brief asks for (4.4):
-- false means the 3-day attribution window hasn't fully elapsed yet as of
-- this run, so is_paid_post_call is provisional, not a settled "no". A daily
-- refresh should treat is_window_closed = false rows as "too early to call"
-- in reporting, not blend them into a rate that looks final.
--
-- int_calls_payment_attribution is at (call x attributed payment) grain --
-- grouped back to one row per call here so Metric 2's rate isn't inflated by
-- calls that happen to have more than one attributed payment.
--
-- Incremental with the same 4-day lookback as its source
-- (int_calls_payment_attribution) so this mart stays in sync with whatever
-- that model just reprocessed, rather than reading a stale slice of it.

{{ config(
    materialized='incremental',
    unique_key='call_log_id',
    incremental_strategy='delete+insert'
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
