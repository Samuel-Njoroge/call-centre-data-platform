-- Metric 4 (inbound call drivers) population. One row per inbound
-- disposition, carrying the 3-level drill-down hierarchy as-is.
--
-- "Inbound" = call_type = 'Inbound Team' only. See docs/WRITEUP.md: the
-- brief's documented call_type values aren't exhaustive, the field is
-- documented as "team handling the call" rather than a direction flag, and
-- the largest single value (Phone Call, 59% of all dispositions) has no
-- reliable directionality signal -- excluded rather than guessed at.

select
    call_log_id,
    market,
    disposed_at_utc,
    level_one,
    level_two,
    level_three
from {{ ref('stg_atlas__dispositions') }}
where call_type = 'Inbound Team'
