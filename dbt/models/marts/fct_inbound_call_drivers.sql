-- See this model's description (dbt docs) for the full rationale.
{{ config(
    materialized='incremental',
    unique_key='driver_id',
    incremental_strategy='delete+insert',
    sort=['call_date']
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
