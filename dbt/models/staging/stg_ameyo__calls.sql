-- One row per Ameyo call leg dialled. ch_call_id is the natural key.
-- ch_date_added is LOCAL time of the contact centre country -- not converted
-- to UTC here, since doing so needs a per-market timezone lookup that
-- belongs in intermediate/marts, not staging.
--
-- ch_date_added and total_talk_time are cast explicitly: Airbyte's raw
-- columns are all varchar (no schema inference configured), unlike
-- local_loader's DuckDB output, which infers real types. Comparisons work


select
    ch_call_id as call_id,
    cast(ch_date_added as timestamp) as called_at_local,
    case ch_contact_center_id
        when 1 then 'Kenya'
        when 2 then 'Uganda'
        when 3 then 'Tanzania'
        when 4 then 'Nigeria'
    end as market,
    ch_system_disposition as system_disposition,
    ch_call_type as call_type,
    cast(total_talk_time as bigint) as talk_time_ms,
    udh_user_id as ameyo_user_id,
    campaign_name,
    nullif(trim(udh_notes), '') as call_notes_raw,
    {{ lineage_columns() }}
from {{ source('raw', 'ameyo_outbound_calls') }}
