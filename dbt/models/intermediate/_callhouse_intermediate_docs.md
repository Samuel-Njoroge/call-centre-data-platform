{% docs int_calls_coded %}
Metric 1 (coding rate) logic. One row per Ameyo call leg — a superset of
`stg_ameyo__calls`, carrying whether each call was successfully "coded"
(linked to an Atlas disposition).

**The coding rule.** A call is "coded" when `udh_notes` holds a value that
(a) is entirely digits — so it could plausibly be a pasted `call_log_id`,
not free-text garbage — and (b) actually matches a `call_log_id` in Atlas
dispositions. The digits-only check runs on the notes text before casting,
rather than a bare/`TRY_CAST`, to stay portable across DuckDB and Redshift
without relying on `TRY_CAST` semantics matching between the two engines.

**Whitespace handling.** Staging's `trim()` only strips spaces, not embedded
newlines/tabs, and some genuine `call_log_id` pastes carry a trailing `\n`
(a copy-paste artifact). Only leading/trailing whitespace is stripped here —
a note with an embedded newline separating two distinct tokens (e.g. two ids
pasted on separate lines) is genuinely ambiguous and stays excluded, not
concatenated into one bad number.
{% enddocs %}

{% docs int_calls_payment_attribution %}
Metrics 2 (paid post call) and 3 (value recovered) logic. Grain: one row per
(disposition × attributed payment) — a disposition with no attributed
payment appears once with null payment fields; a disposition attributed
more than one payment appears once per payment.

**Attribution rule.** Each payment attributes to exactly one call: the
nearest *preceding* disposition on the same `contract_id`, among
dispositions whose `[disposed_at_utc, +3 days]` window contains the payment.
A payment is never claimed by more than one call, so Metric 3's sum never
double-counts a payment's value even though 41% of paying contracts pay
more than once. A call *can* legitimately end up attributed to more than one
payment (both genuinely followed it, no other call intervened). Full
reasoning in `docs/WRITEUP.md`.

**Deterministic tiebreak.** `call_log_id DESC` breaks ties when more than
one disposition on the same contract shares the exact same timestamp — 420
`(contract_id, disposed_at_utc)` combinations in this data have more than
one disposition at the same instant, which left the "nearest preceding"
winner nondeterministic across separate query executions without this tie-
break. Caught by rerunning against the same day's data and seeing row-count
drift — `delete+insert` doesn't clean up a row whose key changed between
runs, so a nondeterministic winner would silently corrupt the incremental
table over time, not just produce a wrong number once.

**`is_window_closed`.** False for dispositions made within the last 3 days
as of the most recent run — their paid-post-call status is still
provisional, not a final "no". This is the moving-window problem the case
study brief calls out directly (4.4): a call made Monday can't be assessed
until Thursday, because the 3-day window is still open.

**Incremental strategy.** This is a range join (`contract_id` + a timestamp
`BETWEEN`), not a cheap keyed lookup — at "many orders of magnitude larger"
production volume, recomputing it from full history on every run is the
first thing that would blow up Redshift query time/cost. Only dispositions
from the last 4 days are reprocessed each run — one day more than the 3-day
attribution window itself, so a disposition can't fall out of scope while
its window is still open. Older, already-closed-window rows are left
untouched. `dist='contract_id'` matches the two staging tables feeding the
range join, so this model's own `delete+insert` doesn't need a
redistribution either; `sort=['disposed_at_utc']` matches the incremental
filter and the two downstream marts built on this model. Both Redshift-only,
ignored on DuckDB.
{% enddocs %}

{% docs int_dispositions_hierarchy %}
Metric 4 (inbound call drivers) population. One row per inbound disposition,
carrying the 3-level drill-down hierarchy as-is.

**"Inbound" definition.** `call_type = 'Inbound Team'` only. The case study
brief's documented `call_type` values aren't exhaustive, the field itself is
documented as "the team or channel handling the call" rather than a
direction flag, and the largest single value (`Phone Call`, 59% of all
dispositions) has no reliable directionality signal at all — excluded
rather than guessed at. Full reasoning, including how the 59% figure and
the "no signal" claim were each independently verified against the raw
data, in `docs/WRITEUP.md`.
{% enddocs %}
