# Write Up Summary

Covers the following sections:

- Answers
- Data gaps
- Assumptions
- How each gap was handled
- Recommendations 

For how to run the pipeline and what you should see, check [README](../README.md). 

For the detailed production (Redshift/S3/Airbyte) setup, check [PROD DOCS](PROD.md).

## 1. Answers

Numbers below are from the local target against the sample extract (single day of Ameyo/Atlas
data, 2026-08-05; payments span 2026-08-04 to 2026-08-07). All four are reproducible by running
`dbt build --target local` -see README.

**Metric 1 -Coding rate: 44.68%** (4,695 / 10,508 outbound calls). 

Over half the shortfall is calls that were never coded at all (53% blank `udh_notes`), not agents pasting a wrong `id`.
`fct_coding_rate` breaks this down by day, market, campaign, and agent for coaching.

**Metric 2 -Paid post call: 12.88%** (5,901 / 45,814 assessable dispositions; 6,318 dispositions with no `contract_id` are excluded as not-assessable, not counted as failures). `is_window_closed` on `fct_paid_post_call` flags whether a given row's 3-day attribution window has actually elapsed -this matters once the pipeline is running daily rather than against a single closed day (see [How handled](#4-how-handled)).

**Metric 3 -Value recovered:**  $14,007.04 USD across the sample, using the payment-attribution rule below. 

By market: 
- Kenya $6,575.58
- Nigeria $2,920.35
- Tanzania $2,969.64
- Uganda $1,541.47

(see `fct_value_recovered_usd` for the day-level breakdown). Converted using the most recently fetched FX rate, not true historical per-date rates -see [Recommendations](#5-recommendations).

**Metric 4 -Top inbound drivers** (7,679 inbound dispositions, `call_type = 'Inbound Team'` only): 

Enquiry (3,851)

Service Request (1,616)

Complaints (1,296)

Customer Feedback (916), plus smaller categories. 

`fct_inbound_call_drivers` carries the full level_two/level_three breakdown for
drill-down starting at level_one.

## 2. Data gaps

Quantified findings from the raw extracts, tied to where each was found.

- **`udh_notes` (Ameyo) is blank on 5,575 of 10,508 outbound calls (53%)** -over half of outbound
  calls never had a `call_log_id` pasted back into Ameyo at all, which alone caps Metric 1 well
  below 100% before any validity checking against Atlas.
- **Of the 4,933 calls with a non-blank note, only 4,695 match a real `call_log_id`** in the Atlas
  dispositions extract -15 are non-numeric, and ~238 numeric-looking notes don't match anything
  in this single-day extract (typos, mis-pastes, or dispositions logged after this extract's
  cutoff).
- **28 duplicate `ameyo_user_id` values in the Atlas↔Ameyo agent mapping file.** 14 are exact
  duplicate rows; the other 14 genuinely conflict (mostly cosmetic -email formatting, spacing —
  but 4 disagree on `team` itself). The file carries no timestamp, so there's no principled way to
  tell which row is current.
- **2 payments with `amount <= 0`.**
- **`payment_request_provider` contains non-provider values**: `MANUAL` (412 rows), `UNKNOWN` (1
  row), `LOYALTY_DISCOUNT` (101 rows) -the last in particular reads as a non-cash credit, not
  money collected, which risks inflating Metric 3 if included without a decision.
- **6,318 of 52,132 Atlas dispositions (12.1%) have a null `contract_id`** -spread across call
  types, not concentrated in one team. These can't be assessed for payment attribution at all;
  there's no contract to check.
- **420 `(contract_id, disposed_at_utc)` combinations have more than one disposition at the exact
  same timestamp** -the payment-attribution logic needs a deterministic tiebreak for these, or
  reruns against unchanged data can attribute a payment to a different call each time.
- **`call_type`'s documented values aren't exhaustive** -the data also contains `On Site Visit`
  (400 rows) and `Welcome Team` (7 rows), plus 2,944 null. More importantly, the single largest
  value, `Phone Call` (30,515 rows, 59% of all dispositions), carries no directionality signal at
  all, which directly affects how Metric 4 can be defined (see Assumptions).

## 3. Assumptions

**Payment attribution (Metrics 2 & 3): each payment attributes to exactly one call.** 41% of
contracts with any payment have more than one (220,464 of 532,729), and a contract can have up to
11 dispositions in this sample -a naive "does any payment exist within 3 days" join would let one
payment satisfy multiple calls, double-counting its value in Metric 3. Instead, a payment attributes
to the nearest *preceding* disposition on the same `contract_id`, among dispositions whose window
`[disposed_at_utc, disposed_at_utc + 3 days]` contains the payment's timestamp. A single call can
still end up attributed to more than one payment (legitimate -both genuinely followed only that
call); a single payment is never attributed to more than one call. Ties (the 420 same-timestamp
combinations above) are broken deterministically by `call_log_id DESC`, so reruns against unchanged
data always pick the same winner. Payments with `amount <= 0` are excluded from attribution
entirely.

- Metric 2 (rate) = share of dispositions that are the attributed call for at least one payment.
- Metric 3 (value) = sum of attributed payment amounts (converted to USD).

**Metric 4 "inbound" definition: `call_type = 'Inbound Team'` only.** `call_type` is documented as
"the team or channel handling the call," not a direction flag, and most named teams map to outbound
functions per the brief's own background (Onboarding/Upsell are outbound by definition; Voice of
Customer is an outbound satisfaction survey). `Phone Call` (59% of all dispositions) has no
directionality signal at all. Classifying only `Inbound Team` as inbound is conservative and likely
undercounts true inbound volume -see Recommendations -but avoids guessing at a field that wasn't
designed to answer this question.

**`payment_request_provider`'s non-provider values (`MANUAL`, `UNKNOWN`, `LOYALTY_DISCOUNT`) are
left in the attribution population by default.** They carry a real `amount` and there's no
grounding in the data alone to assume they aren't cash recovered -flagged here as a business
question, not silently guessed at either way (see Recommendations).

## 4. How handled

| Data gap | How handled | Effect on the numbers |
|---|---|---|
| 53% blank `udh_notes` | Excluded -no `call_log_id` to match | Caps Metric 1 well below 100% |
| 28 duplicate mapping rows | 14 exact dupes deduped via `DISTINCT`; 14 conflicts resolved by alphabetically-first `atlas_user_name` -deterministic, not a claim of correctness | <1% of agents affected |
| 2 non-positive payments | Excluded from attribution entirely | Negligible (2 rows) |
| `payment_request_provider` junk values (514 rows) | Defaulted in (no grounding to exclude), flagged as an open business question | Metric 3 possibly overstated |
| 6,318 dispositions with null `contract_id` | Excluded from the Metric 2/3 denominator -"not assessable," not "unpaid" | Avoids understating Metric 2 |
| 420 tied disposition timestamps | Deterministic tiebreak (`call_log_id DESC`) added to the attribution model | Keeps attribution stable across reruns |
| `call_type` ambiguity (`Phone Call`, 59% of rows) | Excluded from Metric 4's inbound count | Metric 4 likely undercounts -see Recommendations |

**The daily-refresh moving-window problem.** A call made Monday can't be assessed for
paid-post-call until Thursday, because the 3-day payment window is still open -Monday's number
would otherwise keep moving and then silently "settle" with no signal that it had. `fct_paid_post_call`
carries an `is_window_closed` flag (`disposed_at_utc + 3 days` has actually elapsed as of the run),
false for any disposition still inside its window. Metric 2's rate is computed over closed-window
rows only, so a daily-refresh run never reports a still-open window as a settled "no" -it's simply
excluded from that day's rate until it closes, then included permanently.

**Materialization.** Staging stays views (cheap pass-throughs). The payment-attribution model and
the three marts built on it are `incremental` with a 4-day lookback -one day more than the 3-day
attribution window, so still-moving dispositions get reprocessed while settled history doesn't. This
matters because the join is a range join (contract_id + timestamp `BETWEEN`), not a cheap keyed
lookup -the first thing that would blow up cost/time at production volume if recomputed from full
history every run. `dim_agent` stays a plain table (small full snapshot, no incremental benefit).

## 5. Recommendations

### To the call centre team

**The manual Ameyo → Atlas link is the single biggest lever on Metric 1, by a wide margin.** 53% of
outbound calls never had a `call_log_id` pasted back at all -that alone dwarfs every other issue
affecting coding rate combined. Before treating this as an individual coaching target, check whether
the miss rate is spread evenly across agents or concentrated in a few, and whether it correlates
with call volume, campaign type, or shift -`fct_coding_rate` already supports slicing by
agent/campaign/day for exactly this. But the ceiling on what coaching alone can fix is real: even a
perfectly disciplined agent is doing a manual copy-paste step today that a system integration could
do automatically (see below) -worth escalating as a process/tooling gap, not only a coaching
target.

**Metric 4's inbound number is very likely an undercount, and the gap is a data-labeling issue, not
a query choice.** `call_type = 'Phone Call'` is 59% of all dispositions with zero directionality
signal, and it's excluded from "inbound" entirely because there's no principled way to split it. If
even a modest fraction of that bucket is genuinely inbound, the true shape of "what customers are
calling about" is meaningfully different from what this dashboard shows. Worth asking whether
`call_type` can be split into two fields (channel/team *and* direction) going forward.

**`payment_request_provider` needs a business ruling, not a warehouse guess.** `LOYALTY_DISCOUNT`
(101 rows), `MANUAL` (412 rows), and `UNKNOWN` (1 row) are currently left inside Metric 3's total by
default -but `LOYALTY_DISCOUNT` in particular reads like a non-cash credit, not money collected. If
that's right, Metric 3 as reported is somewhat overstating actual cash recovered. This is a
five-minute question for whoever owns the payments/finance side of Atlas, and the answer changes a
real number on this dashboard.

### To d.light (engineering / product / Data)

**Automate the Ameyo ↔ Atlas link instead of relying on agents to paste an id.** The single
highest-leverage fix available, and upstream of everything downstream in this pipeline -when an
agent disposes a call in Atlas, an API call or webhook could write the resulting `call_log_id`
directly into the corresponding Ameyo call record, removing the manual step (and its 53% miss rate)
entirely rather than measuring around it forever.

**For real production ingestion of Ameyo and Atlas specifically, build custom API extractors rather
than routing them through Airbyte.** The honest production path is a custom
Python extractor per system, following the same auth/retry/idempotency discipline already built and
tested in `ingestion/fx_rates/fetch_fx.py`. Airbyte's real value in this stack is for sources that
*do* have off-the-shelf connectors (mobile-money/PayGo platforms, etc.).

**FX conversion should move from "latest rate" to "rate on the payment's own date" once real rate
history has accumulated.** The current design applies the most recently fetched rate to every
historical payment, because only a single day of fetched history exists right now. Once
`raw.fx_rates` has been accumulating daily, this should switch to matching each payment to the rate
nearest its own `pay_timestamp_utc` -Metric 3 is currently a reasonable approximation, not the
number a finance team should reconcile against.

**Turn the data-gap findings above into upstream tickets, not permanent downstream patches.** Every
issue in Section 2 is currently absorbed silently in the warehouse (deduped, defaulted, or excluded)
so the pipeline keeps producing a number every morning -the right call for a daily pipeline, but
several of these are genuinely fixable at the source. The Atlas↔Ameyo mapping file having no
timestamp (making its 28 duplicate/conflicting rows unresolvable in principle, not just in practice)
is the clearest example -that should become a properly versioned mapping owned in Atlas itself, not
a static CSV re-deduplicated by a `DISTINCT` every day.

**Add data-quality tests that catch drift, not just structural validity.** The current `dbt test`
suite (not-null/unique on natural keys) wouldn't catch, for example, `udh_notes`'s blank rate
jumping from 53% to 90% overnight, or a new junk value appearing in `payment_request_provider`.
Worth adding range/accepted-values tests on exactly the columns this write-up found real gaps in —
the two lists overlap almost entirely, which is itself a decent heuristic for where to add tests
first.
