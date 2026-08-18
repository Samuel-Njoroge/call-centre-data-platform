-- One row per call disposed of in Atlas against a customer contract. call_log_id
-- is the primary key -- this is the id agents are supposed to paste into Ameyo.
--
-- created_timestamp_utc is cast explicitly: Airbyte's raw columns are all
-- varchar (no schema inference configured), unlike local_loader's DuckDB
-- output. Confirmed necessary by a real error -- int_calls_payment_attribution
-- COALESCEs max(disposed_at_utc) against a typed timestamp literal, which
-- Redshift refuses to do across varchar/timestamp even though plain
-- comparisons tolerate the implicit cast fine.

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
