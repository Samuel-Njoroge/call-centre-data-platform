-- Metrics 2 (paid post call) & 3 (value recovered) logic. Grain: one row per
-- (disposition x attributed payment) -- a disposition with no attributed
-- payment appears once with null payment fields; a disposition attributed
-- more than one payment appears once per payment.
--
-- Attribution rule (see docs/WRITEUP.md for the full reasoning): each payment
-- attributes to exactly one call -- the nearest PRECEDING disposition on the
-- same contract_id, among dispositions whose [disposed_at_utc, +3 days]
-- window contains the payment. A payment is never claimed by more than one
-- call, so Metric 3's sum never double-counts a payment's value even though
-- 41% of paying contracts pay more than once. A call CAN legitimately end up
-- attributed to more than one payment (both genuinely followed it, no other
-- call intervened).
--
-- is_window_closed: false for calls made within the last 3 days as of the
-- most recent run -- their paid-post-call status is still provisional, not a
-- final "no" (see the moving-window problem in the case study brief, 4.4).
--
-- Incremental: this is a range join (contract_id + a timestamp BETWEEN), not
-- a cheap keyed lookup -- at "many orders of magnitude larger" volume,
-- recomputing it from full history on every run is the first thing that
-- would blow up Redshift query time/cost. Only dispositions from the last 4
-- days are reprocessed each run -- 1 day more than the 3-day attribution
-- window itself, so a disposition can't fall out of scope while its window
-- is still open. Older, already-closed-window rows are left untouched.

{{ config(
    materialized='incremental',
    unique_key='attribution_id',
    incremental_strategy='delete+insert'
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
            -- call_log_id as a deterministic tiebreaker: 420 (contract_id,
            -- disposed_at_utc) combos in this data have >1 disposition at the
            -- exact same timestamp, which left the winner nondeterministic
            -- across separate query executions without this -- caught by
            -- rerunning against the same day's data and seeing row count
            -- drift (delete+insert doesn't clean up a row whose key changed).
            order by d.disposed_at_utc desc, d.call_log_id desc
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
