-- Metric 1 (coding rate) logic. One row per Ameyo call leg.
--
-- A call is "coded" when udh_notes holds a value that (a) is entirely digits
-- -- so it could plausibly be a pasted call_log_id, not free-text garbage --
-- and (b) actually matches a call_log_id in Atlas dispositions. Guarding with
-- a regex before casting (rather than a bare/try cast) keeps this portable
-- across DuckDB and Redshift without relying on TRY_CAST semantics matching
-- between the two engines.

with candidate as (
    select
        *,
        -- staging's trim() doesn't strip embedded newlines/tabs (only spaces),
        -- and some genuine call_log_id pastes carry a trailing \n (copy-paste
        -- artifact) -- strip LEADING/TRAILING whitespace only (not internal:
        -- a note with an embedded newline separating two distinct tokens,
        -- e.g. two ids pasted on separate lines, is genuinely ambiguous and
        -- should stay excluded, not get concatenated into one bad number).
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
