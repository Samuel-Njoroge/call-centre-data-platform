with candidate as (
    select
        *,
        {{ regexp_replace_global("call_notes_raw", "'^\\s+|\\s+$'", "''") }} as _stripped_notes,
        case
            when {{ regexp_replace_global("call_notes_raw", "'^\\s+|\\s+$'", "''") }} ~ '^[0-9]+$'
                then cast({{ regexp_replace_global("call_notes_raw", "'^\\s+|\\s+$'", "''") }} as bigint)
            else null
        end as candidate_call_log_id
    from {{ ref('stg_ameyo__calls') }}
),

matched as (
    select
        c.call_id,
        c.called_at_local,
        c.market,
        c.call_type,
        c.talk_time_ms,
        c.ameyo_user_id,
        c.campaign_name,
        c.call_notes_raw,
        c.candidate_call_log_id,
        d.call_log_id as matched_call_log_id,
        d.contract_id as matched_contract_id,
        d.disposed_at_utc as matched_disposed_at_utc
    from candidate c
    left join {{ ref('stg_atlas__dispositions') }} d
        on c.candidate_call_log_id = d.call_log_id
)

select
    *,
    (matched_call_log_id is not null) as is_coded
from matched
