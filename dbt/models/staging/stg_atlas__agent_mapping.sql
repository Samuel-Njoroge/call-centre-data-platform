with deduplicated as (
    select distinct
        contact_centre,
        ameyo_user_id,
        team,
        atlas_user_name
    from {{ source('raw', 'atlas_ameyo_mapping') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by ameyo_user_id
            order by atlas_user_name
        ) as _tie_break_rank
    from deduplicated
)

select
    contact_centre as market,
    ameyo_user_id,
    team,
    atlas_user_name
from ranked
where _tie_break_rank = 1
