-- Metric 1. One row per (call_date, market, campaign, agent) -- the exact
-- breakdown the brief asks for, since the team wants this for coaching
-- individual agents. call_date is the LOCAL calendar date the call was
-- placed (not UTC -- coaching an agent on "yesterday's" calls should mean
-- their local yesterday).
--
-- Incremental, reprocessing the last 4 days of call_date on each run: coding
-- status isn't a "moving window" the way Metric 2 is, but Atlas dispositions
-- can still arrive with a short lag after the call, so a small buffer avoids
-- a call permanently freezing as "not coded" if its match lands a day late.

{{ config(
    materialized='incremental',
    unique_key='coding_rate_id',
    incremental_strategy='delete+insert'
) }}

with aggregated as (
    select
        cast(c.called_at_local as date) as call_date,
        c.market,
        c.campaign_name,
        c.ameyo_user_id,
        a.atlas_user_name,
        a.team,
        count(*) as total_calls,
        sum(case when c.is_coded then 1 else 0 end) as coded_calls,
        sum(case when c.is_coded then 1 else 0 end) * 1.0 / count(*) as coding_rate
    from {{ ref('int_calls_coded') }} c
    left join {{ ref('dim_agent') }} a
        on c.ameyo_user_id = a.ameyo_user_id

    {% if is_incremental() %}
    where cast(c.called_at_local as date) >= (
        select coalesce(max(call_date), date '1900-01-01') - interval '4 days'
        from {{ this }}
    )
    {% endif %}

    group by 1, 2, 3, 4, 5, 6
)

select
    {{ dbt_utils.generate_surrogate_key(['call_date', 'market', 'campaign_name', 'ameyo_user_id']) }} as coding_rate_id,
    *
from aggregated
