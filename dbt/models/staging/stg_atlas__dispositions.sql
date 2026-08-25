-- See this model's description (dbt docs) for the full rationale.
{{ config(
    materialized='table',
    dist='contract_id',
    sort=['disposed_at_utc']
) }}

select
    call_log_id,
    case tenant_id
        when 1001 then 'Kenya'
        when 1002 then 'Uganda'
        when 1003 then 'Tanzania'
        when 1004 then 'Nigeria'
    end as market,
    customer_id,
    contract_id,
    cast(created_timestamp_utc as timestamp) as disposed_at_utc,
    createdby as disposed_by,
    call_type,
    level_one,
    nullif(trim(level_two), '') as level_two,
    nullif(trim(level_three), '') as level_three,
    {{ lineage_columns() }}
from {{ source('raw', 'atlas_calls_dispositions') }}
