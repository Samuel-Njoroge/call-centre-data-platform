-- See this model's description (dbt docs) for the full rationale.
{{ config(
    materialized='table',
    dist='contract_id',
    sort=['pay_timestamp_utc']
) }}

select
    pay_timestamp_utc,
    case tenant_id
        when 1001 then 'Kenya'
        when 1002 then 'Uganda'
        when 1003 then 'Tanzania'
        when 1004 then 'Nigeria'
    end as market,
    case tenant_id
        when 1001 then 'KES'
        when 1002 then 'UGX'
        when 1003 then 'TZS'
        when 1004 then 'NGN'
    end as currency,
    tenant_id,
    contract_id,
    trim(payment_request_provider) as payment_request_provider,
    create_program,
    cast(amount as double precision) as amount_local,
    {{ lineage_columns() }}
from {{ source('raw', 'atlas_payments') }}
