# How This Pipeline Works, Stage by Stage

This document explains what actually happens, end to end, when this pipeline runs — from
the raw CSV files all the way to the numbers on the Superset dashboard. It assumes no prior
familiarity with this repo. If you just want to *run* it, see the [README](../README.md).
If you want the reasoning behind specific modelling decisions, data gaps, and the four
business answers, see [WRITEUP.md](WRITEUP.md). If you want the step-by-step production
(Redshift/Airbyte) setup, see [PROD.md](PROD.md). This document is the map that ties all of
that together.

## The one-sentence version

Two call-centre systems that don't talk to each other (Ameyo for phone calls, Atlas for
outcomes and payments) get loaded into a warehouse, cleaned up and joined by dbt, and turned
into four dashboards — all orchestrated by one Dagster job that can run entirely on your
laptop (DuckDB) or against production infrastructure (S3 + Airbyte + Redshift) with the same
code.

## The big picture

```
  data/raw/*.csv                 (the only thing that changes day to day)
        |
        v
  ┌─────────────┐   local target       ┌──────────────┐   redshift target
  │  Ingestion  │──────────────────────│               │──────────────────────┐
  └─────────────┘                      └──────────────┘                      │
        |                                                                     v
        |  (local: straight into DuckDB)              (redshift: S3 upload, then Airbyte sync)
        v                                                                     |
  ┌─────────────────────────────────────────────────────────────────────────┘
  │  raw schema           <- untouched landing tables, exactly what arrived
  └─────────────┬─────────────────────────────────────────────────────────────
                v
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  staging (dbt)         <- typed, renamed, one model per source, no logic │
  └─────────────┬─────────────────────────────────────────────────────────────┘
                v
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  intermediate (dbt)    <- the real business logic: coding match,        │
  │                            payment attribution, inbound classification  │
  └─────────────┬─────────────────────────────────────────────────────────────┘
                v
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  marts (dbt)            <- the 4 answers, one fact table each, plus     │
  │                             dim_agent                                   │
  └─────────────┬─────────────────────────────────────────────────────────────┘
                v
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  Superset               <- dashboard, charts, pre-loaded on first boot  │
  └─────────────────────────────────────────────────────────────────────────┘
```

**Dagster** is the thing that runs all of this in order, every morning, and makes sure the
transform step never starts before the load step has actually finished.

## Why two paths exist (local vs. production)

Everything above has two ways to run: **local** (the default — nothing to install, no cloud
account, runs entirely on your machine using DuckDB as the warehouse) and **redshift**
(matches how this would actually run in production — S3, Airbyte, and a real Redshift
cluster). The dbt models, the Dagster job, and the four dashboard answers are identical
either way; only *where the data lands* changes. You pick which one at the moment you launch
a Dagster run — it's a dropdown in the Dagster UI's Launchpad, not a different codebase.

---

## Stage 0: The raw data

Two systems generate the data this pipeline consumes, and — this is the whole reason this
project exists — **they don't share a database or a common ID.**

- **Ameyo** is the telephony platform. It records every call an agent dials: who dialled it,
  when, how long they talked, which campaign it belonged to. One file: `ameyo_outbound_calls.csv`.
- **Atlas** is the CRM. After a call, the agent is supposed to open the customer's record in
  Atlas and log a *disposition* — what the call was about, and how it went. Atlas is also
  where payments get recorded. Two files: `atlas_calls_dispositions.csv` and
  `atlas_payments.csv`.
- A fourth file, `atlas_ameyo_mapping.csv`, is the only bridge between the two systems' agent
  identities (Ameyo's `ameyo_user_id` vs. Atlas's `atlas_user_name`) — there's no shared user
  key otherwise.

**The manual link between calls and outcomes.** When an agent logs a disposition in Atlas,
Atlas generates a `call_log_id`. The agent is then supposed to copy that id and paste it into
a free-text notes field back in Ameyo. That's the *only* thread connecting a dialled call to
its business outcome — and it depends entirely on the agent remembering to do it. (Spoiler:
53% of the time, they don't. That gap is Metric 1 — see below.)

These four CSVs live under `data/raw/{date}/` — one dated folder per day of data, e.g.
`data/raw/2026-08-15/`. A production run would see a new dated folder show up every morning;
this pipeline is built to pick that up automatically without any code changes.

---

## Stage 1: Ingestion — getting the CSVs into a warehouse

This is the one stage that genuinely differs between local and production.

### Local target (`ingestion/local_loader/load_raw.py`)

The simplest possible path: reads every CSV under `data/raw/**/`, and for each of the four
entities, drops and recreates a table in a local DuckDB file (`warehouse_local.duckdb`) under
a `raw` schema. It globs across *all* dated folders at once (`data/raw/*/ameyo_outbound_calls.csv`),
so tables always reflect every day of data that exists on disk — rerunning it is safe (it
rebuilds from scratch every time, so nothing is ever duplicated), and a new day's folder needs
zero code changes to be picked up.

### Redshift/production target

This path has two separate steps, because in production nothing writes directly into the
warehouse — everything goes through a proper ingestion tool:

1. **`ingestion/s3_upload/upload_to_s3.py`** uploads `data/raw/{date}/{entity}.csv` to an S3
   bucket, preserving that same folder layout (S3 key: `raw/{date}/{entity}.csv`). It's not a
   blind upload — it hashes each file's content (MD5) and compares against a hash it
   previously stashed in the S3 object's own metadata, so re-running it doesn't re-upload
   files that haven't actually changed.
2. **Airbyte** (a separate, containerized service — see [PROD.md](PROD.md) for how it's set
   up) has an S3 source connector watching that bucket via a glob pattern
   (`raw/**/{entity}.csv`), and a Redshift destination connector writing into Redshift's `raw`
   schema. Airbyte handles the actual file-parsing and loading; this pipeline's own code never
   touches that part directly — it only *triggers* a sync and waits for it to finish
   (`ingestion/airbyte_sync/trigger_sync.py`), so the transform stage never starts against a
   sync that's still in flight. Sync mode is **incremental/append** per stream, not full-refresh
   — full-refresh would re-duplicate every historical row on every run.

### The exchange rate feed (`ingestion/fx_rates/fetch_fx.py`)

A fifth, independent ingestion source: daily USD exchange rates for the four markets' local
currencies (KES, UGX, TZS, NGN), fetched from exchangerate-api.com. This isn't one of the
original four extracts — Metric 3 (value recovered) needs to convert local-currency payments
to USD, and no exchange rate exists anywhere in the source data, so this pipeline integrates
an external API for it. The fetch has real retry/backoff logic (the API will eventually be
slow or rate-limited) and writes idempotently — deleting and reinserting the day's rates
rather than blindly appending, so re-running it never duplicates a day's numbers.

---

## Stage 2: The raw layer

Whichever path loaded it, the result is the same: a `raw` schema with five tables
(`ameyo_outbound_calls`, `atlas_calls_dispositions`, `atlas_payments`, `atlas_ameyo_mapping`,
`fx_rates`), completely untouched from source — same column names, same messy values, same
gaps. This is deliberate: `raw` is "what arrived," not "what we've cleaned." Every
transformation from here on happens in dbt, where it's version-controlled, testable, and
visible — nothing gets quietly cleaned up on the way in.

---

## Stage 3: Staging (dbt) — typing, not logic

`dbt/models/staging/` has one model per raw source (`stg_ameyo__calls`,
`stg_atlas__dispositions`, `stg_atlas__payments`, `stg_atlas__agent_mapping`, `stg_fx__rates`).
Each one does light, mechanical work only: casting text columns to real types (Airbyte lands
*everything* as `varchar` regardless of actual content, so a payment amount arrives as literal
text and has to be cast to a number before it's usable), renaming cryptic source column names
to something readable (`ch_call_id` → `call_id`), and decoding lookup codes (Ameyo's
`ch_contact_center_id` and Atlas's `tenant_id` both encode the same four markets, but with two
completely different numbering schemes — staging is where both get decoded to the same plain
market name). No joins, no business rules, no filtering out "bad" data — that all happens
one layer down.

One exception: `stg_atlas__agent_mapping` does deduplicate its 28 duplicate rows here
(14 exact duplicates removed outright, 14 genuine conflicts resolved by a documented,
deterministic tiebreak) — a data-hygiene fix, not a business decision, so it's fine to do in
staging rather than waiting for the marts layer.

Three of these five models are physical **tables**, not views (`stg_atlas__dispositions`,
`stg_atlas__payments`, `stg_ameyo__calls`) — the two that feed into the most expensive join
downstream (see Stage 4) are materialized with a matching Redshift distribution key so that
join doesn't need to shuffle data across nodes at production scale. The other two
(`stg_atlas__agent_mapping`, `stg_fx__rates`) stay views — they're dimension-scale data (one
row per agent, one row per currency-date) that will never grow large enough for this to
matter, regardless of how much call volume grows.

---

## Stage 4: Intermediate (dbt) — where the real logic lives

`dbt/models/intermediate/` is where the two systems actually get reconciled, and it's the
most important layer to understand.

### `int_calls_coded` — Metric 1's logic

For every Ameyo call, this checks whether the free-text notes field holds something that
looks like a genuinely pasted Atlas `call_log_id` (all-digits, no stray text) *and* whether
that id actually matches a real Atlas disposition. Only if both are true is the call marked
`is_coded = true`. This is the model that measures the manual-paste gap directly — currently,
53% of calls fail this check, which is the single biggest lever on Metric 1.

### `int_calls_payment_attribution` — Metrics 2 and 3's logic

This is the most complex model in the project, because the brief leaves a real question open:
a customer can hold more than one contract, be called more than once a day, and make several
payments in a day — so which call does a given payment actually belong to? The rule this
pipeline uses: **each payment attributes to exactly one call — the nearest disposition that
came *before* it, on the same contract, within a 3-day window.** A single call can legitimately
end up credited with more than one payment; a single payment is never credited to more than
one call (so Metric 3's total never double-counts a dollar). Where two dispositions on the
same contract land at the exact same timestamp (420 such cases in the sample data), a
deterministic tiebreak picks the same winner every time a rerun happens against unchanged
data — without it, a rerun could silently attribute the same payment to a different call.

This model is also where the **moving-window problem** the brief specifically asks about gets
solved: a call made Monday can't be assessed for "did it get paid" until Thursday, because the
3-day window is still open. Every row carries an `is_window_closed` flag, so a daily refresh
never reports a still-open call as a settled "no" — it's just excluded from that day's rate
until its window actually closes.

Because this does a genuinely expensive join (matching every payment against every disposition
on the same contract within a date range — not a simple lookup), it only reprocesses the last
4 days on each run, not the full history every time. That's what keeps this affordable once
the data is "many orders of magnitude larger" than this sample, per the brief's own framing.

### `int_dispositions_hierarchy` — Metric 4's population

Filters Atlas dispositions down to `call_type = 'Inbound Team'` only. This sounds like it
should be simple, but it's the most-discussed data gap in this whole project: `call_type` is
documented as "the team or channel handling the call," not a direction flag, and the single
largest value (`Phone Call`, 59% of all dispositions) has no way to tell whether it was
inbound or outbound at all — not in this field, not anywhere else in the data. Rather than
guess, this pipeline excludes it, which means Metric 4 is very likely an undercount of true
inbound volume. Full investigation of exactly how that 59% figure and the "no signal" claim
were verified is in [WRITEUP.md](WRITEUP.md).

---

## Stage 5: Marts (dbt) — the four answers

`dbt/models/marts/` has one fact table per business question, plus a small `dim_agent`
dimension table. Every mart here is **incremental** (except `dim_agent`, which is a small
full snapshot) — each run only reprocesses the last few days, not the entire history, which
is what makes a daily refresh affordable at real scale.

| Model | Answers | Grain |
|---|---|---|
| `fct_coding_rate` | Metric 1 — coding rate | one row per (day, market, campaign, agent) |
| `fct_paid_post_call` | Metric 2 — paid post call | one row per assessable call |
| `fct_value_recovered_usd` | Metric 3 — value recovered | one row per (day, market) |
| `fct_inbound_call_drivers` | Metric 4 — inbound drivers | one row per (day, market, level_one, level_two, level_three) |
| `dim_agent` | agent lookup | one row per agent |

Every column in every one of these (and every staging/intermediate model above) is documented
in dbt's own schema files — run `dbt docs generate && dbt docs serve` from `dbt/` for a full
browsable catalog with lineage graph, not just this narrative.

---

## Stage 6: Orchestration — what actually runs this, and when

`dagster_project/callhouse_dagster/definitions.py` defines a single Dagster job,
`callhouse_pipeline`, with three steps run in this exact order:

1. **`load_raw_data`** — runs Stage 1 (either the local loader, or the S3-upload-then-Airbyte-sync
   path).
2. **`load_fx_rates`** — fetches and stores the day's exchange rates (Stage 1's fifth source).
   Explicitly sequenced *after* `load_raw_data`, not in parallel with it — on the local target,
   both would otherwise try to open a write connection to the same DuckDB file at once, and
   DuckDB only allows one writer at a time.
3. **`run_dbt_build`** — runs `dbt build`, which executes every staging/intermediate/marts
   model in dependency order (Stages 3-5) in one command.

Both later steps are configured to start only after the earlier ones finish (`start_after=`),
so "the transform waits for the load" holds by construction, not by convention — if
`load_raw_data` fails, `run_dbt_build` never starts.

**Choosing local vs. redshift.** This is a per-run choice, not a hardcoded setting — when you
click "Launch Run" in the Dagster UI, the Launchpad's run config lets you set
`resources.target_resource.config.target` to `local` (default) or `redshift`. Every op above
reads that same choice and branches accordingly. The daily 06:00 schedule
(`callhouse_daily_schedule`) always runs against `redshift`, regardless of what a manual run
picks, since a real production schedule wouldn't be pointed at a laptop's local DuckDB file.

**Running it.** `python run.py up` starts everything (Dagster UI at `localhost:3000`); a run
either fires from that daily schedule or gets launched manually from the UI. See the README
for the full setup.

---

## Stage 7: Visualization — Superset

A containerized Superset instance is connected to *both* warehouses at once — a "Local
(DuckDB)" connection and a "Production (Redshift)" connection — so the same dashboard works
regardless of which target you've been running against. On first boot, a bootstrap script
(`superset/bootstrap.py`) automatically creates both database connections, registers all five
marts as datasets on each, and imports a pre-built dashboard
(`superset/exports/dashboard_export.zip`) covering all four metrics plus a couple of
supporting charts — so a fresh install shows real charts immediately, not an empty instance
someone has to build by hand. That bootstrap step is also self-healing: if Superset's own
container ever starts before dbt has built anything (so a dataset's columns can't be
introspected yet), it automatically re-syncs those columns on the *next* restart rather than
staying broken forever.

---

## Putting it all together: what one full run looks like

**Local, start to finish:**
1. `python run.py up` — starts Dagster + Superset containers.
2. You (or the schedule) launch `callhouse_pipeline` with `target: local`.
3. `load_raw_data` reads `data/raw/**/*.csv` straight into `warehouse_local.duckdb`'s `raw`
   schema.
4. `load_fx_rates` fetches today's rates into the same DuckDB file.
5. `run_dbt_build` runs every staging → intermediate → marts model against that same file.
6. Open Superset at `localhost:8088` — the dashboard queries the "Local (DuckDB)" connection
   and shows real numbers.

**Production (Redshift), start to finish:**
1. Airbyte, Dagster, and Superset are all running (see [PROD.md](PROD.md) for one-time setup
   of the S3 source / Redshift destination / connection in Airbyte).
2. The 06:00 schedule (or a manual run with `target: redshift`) fires `callhouse_pipeline`.
3. `load_raw_data` uploads `data/raw/**/*.csv` to S3, then triggers the Airbyte connection and
   blocks until Airbyte finishes loading Redshift's `raw` schema.
4. `load_fx_rates` writes today's rates directly into Redshift.
5. `run_dbt_build` runs the same dbt project, `--target redshift`, against the real warehouse.
6. Superset's "Production (Redshift)" connection shows the same dashboard, now backed by
   production data.

Same code, same dbt models, same dashboard — only where the data physically lives changes.
