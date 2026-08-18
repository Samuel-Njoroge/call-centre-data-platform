-- One row per (rate_date, currency). local_units_per_usd = how many units of
-- that currency equal 1 USD (i.e. amount_local / local_units_per_usd = USD).
select
    rate_date,
    currency,
    local_units_per_usd,
    fetched_at
from {{ source('raw', 'fx_rates') }}
