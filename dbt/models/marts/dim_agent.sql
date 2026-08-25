select
    ameyo_user_id,
    atlas_user_name,
    team,
    market
from {{ ref('stg_atlas__agent_mapping') }}
