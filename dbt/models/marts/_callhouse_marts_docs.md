{% docs dim_agent %}
One row per agent (`ameyo_user_id`), deduplicated in staging — see
`stg_atlas__agent_mapping`'s documentation for how the 28 duplicate/
conflicting source rows are resolved.
{% enddocs %}

{% docs fct_coding_rate %}
Metric 1. One row per (`call_date`, `market`, `campaign_name`,
`ameyo_user_id`) — the exact breakdown since the team intends to use it to coach individual agents. `call_date` is the LOCAL calendar date the call was placed (not UTC) — coaching an agent on "yesterday's" calls should mean their local yesterday, not a UTC day that may straddle two of their local days.

**Incremental strategy.** Reprocesses the last 4 days of `call_date` on
each run. Coding status isn't a "moving window" the way Metric 2 is, but
Atlas dispositions can still arrive with a short lag after the call, so a
small buffer avoids a call permanently freezing as "not coded" if its match
lands a day late. `sort=['call_date']` on Redshift lets the `delete+insert`
prune via zone maps instead of a full scan; no distkey override — a low-
cardinality key like `market` (4 values) would skew data unevenly across
Redshift slices, so this is left on Redshift's default (AUTO). Ignored
(harmlessly) on DuckDB.
{% enddocs %}

{% docs fct_paid_post_call %}
Metric 2. One row per assessable disposition (`call_log_id`) — dispositions
with a null `contract_id` are excluded upstream (see `docs/WRITEUP.md`), not
counted as unpaid.

**`is_window_closed`.** The moving-window handling the case study brief asks
for (4.4): false means the 3-day attribution window hasn't fully elapsed
yet as of this run, so `is_paid_post_call` is provisional, not a settled
"no". A daily refresh should treat `is_window_closed = false` rows as "too
early to call" in reporting, not blend them into a rate that looks final.

**Grain.** `int_calls_payment_attribution` is at (call × attributed
payment) grain — grouped back to one row per call here so Metric 2's rate
isn't inflated by calls that happen to have more than one attributed
payment.

**Incremental strategy.** Same 4-day lookback as its source
(`int_calls_payment_attribution`), filtered on `disposed_at_utc`, so this
mart stays in sync with whatever that model just reprocessed rather than
reading a stale slice of it. `sort=['disposed_at_utc']` on Redshift; no
distkey override (see `fct_coding_rate`'s docs for why). Ignored on DuckDB.
{% enddocs %}

{% docs fct_value_recovered_usd %}
Metric 3. One row per (`call_date`, `market`) with the total attributed
payment value converted to USD. `call_date` here is the PAYMENT date (when
the money was actually recovered), not the call date — the natural date to
report "value recovered on this day" against.

**FX rate used.** The most recently fetched rate for that currency, not the
true historical rate on the payment's date. exchangerate-api.com's free
tier only exposes current rates, and this project's own rate history only
starts accumulating from whenever the pipeline first ran — so for this
sample (payments span 2026-08-04 to 2026-08-07, a single fetched rate dated
2026-08-17), every conversion uses that one rate as an approximation. This
is a real limitation, not a rounding detail: once daily fetches have
accumulated real rate history, this should switch to matching each payment
to the rate fetched on (or nearest to) its own date rather than always
using the latest.

**Incremental strategy.** Same 4-day lookback as `int_calls_payment_
attribution`, filtered on `pay_timestamp_utc` (the column this mart's grain
is built on). `sort=['call_date']` on Redshift; no distkey override (see
`fct_coding_rate`'s docs). Ignored on DuckDB.
{% enddocs %}

{% docs fct_inbound_call_drivers %}
Metric 4. One row per (`call_date`, `market`, `level_one`, `level_two`,
`level_three`) combination with a call count — built for a BI tool to start
at `level_one` and drill down, per the case study brief. `call_date` is the
UTC disposition date (Atlas has no local-time field to use instead, unlike
Ameyo).

**Population.** `call_type = 'Inbound Team'` only — see
`int_dispositions_hierarchy`'s documentation for why the broader, ambiguous
`call_type` values (`Phone Call` in particular, 59% of all dispositions)
were left out rather than guessed at.

**Incremental strategy.** Reprocesses the last 4 days of `call_date` — same
short-lag rationale as `fct_coding_rate`, not a moving-window metric like
Metric 2. `sort=['call_date']` on Redshift; no distkey override. Ignored on
DuckDB.
{% enddocs %}
