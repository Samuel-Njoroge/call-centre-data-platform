-- One row per payment transaction per contract. amount is in the LOCAL currency
-- of the market (KES/UGX/TZS/NGN) -- converted to USD later via stg_fx__rates,
-- not here. Whitespace on payment_request_provider is trimmed (pure formatting
-- fix); non-positive amounts and non-provider values (MANUAL/UNKNOWN/
-- LOYALTY_DISCOUNT) are left in -- excluding them is a business decision for
-- the marts layer, not something to silently drop at landing.
--
-- amount is explicitly cast to double precision here: Airbyte's file-based S3
-- source lands every raw column as varchar regardless of actual content (no
-- schema inference configured), unlike local_loader's DuckDB output, which
-- infers real types from the CSV. Comparisons/joins on other varchar raw
-- columns work fine via implicit cast on both engines, but this column feeds
-- an arithmetic division (amount_local / local_units_per_usd in
-- fct_value_recovered_usd), which Redshift refuses to do on varchar without
-- an explicit cast -- confirmed by a real "operator does not exist: character
-- varying / double precision" error.

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
