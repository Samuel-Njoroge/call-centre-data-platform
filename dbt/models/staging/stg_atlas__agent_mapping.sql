-- One row per agent (ameyo_user_id). Bridges Ameyo logins to Atlas identities/teams.
--
-- The raw file has 28 duplicate ameyo_user_id values: 14 are byte-for-byte exact
-- duplicates (DISTINCT removes them with no information loss), 14 have genuine
-- conflicts -- mostly cosmetic, but 4 disagree on `team` itself. There's no
-- timestamp in this file to determine which row is current, so ties are broken
-- deterministically (alphabetically-first atlas_user_name) rather than guessed at.

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
