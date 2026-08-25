select
    call_log_id,
    market,
    disposed_at_utc,
    level_one,
    level_two,
    level_three
from {{ ref('stg_atlas__dispositions') }}
where call_type = 'Inbound Team'
