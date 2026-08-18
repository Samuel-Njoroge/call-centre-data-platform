-- Metric 4. One row per (call_date, market, level_one, level_two, level_three)
-- combination with a call count -- built for a BI tool to start at level_one
-- and drill down, per the brief. call_date is the UTC disposition date
-- (Atlas has no local-time field to use instead, unlike Ameyo).
--
-- "Inbound" population is call_type = 'Inbound Team' only -- see
-- docs/WRITEUP.md for why the broader, ambiguous call_type values were left
-- out rather than guessed at.
--
-- Incremental, reprocessing the last 4 days of call_date -- same short-lag
-- rationale as fct_coding_rate, not a moving-window metric like Metric 2.

{{ config(
    materialized='incremental',
    unique_key='driver_id',
    incremental_strategy='delete+insert'
) }}

with aggregated as (
    select
        cast(disposed_at_utc as date) as call_date,
        market,
        level_one,
        level_two,
        level_three,
        count(*) as call_count
    from {{ ref('int_dispositions_hierarchy') }}

    {% if is_incremental() %}
    where cast(disposed_at_utc as date) >= (
        select coalesce(max(call_date), date '1900-01-01') - interval '4 days'
        from {{ this }}
    )
    {% endif %}

    group by 1, 2, 3, 4, 5
)

select
    {{ dbt_utils.generate_surrogate_key(['call_date', 'market', 'level_one', 'level_two', 'level_three']) }} as driver_id,
    *
from aggregated
