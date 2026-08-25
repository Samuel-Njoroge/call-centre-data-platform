select
    rate_date,
    currency,
    local_units_per_usd,
    fetched_at
from {{ source('raw', 'fx_rates') }}
