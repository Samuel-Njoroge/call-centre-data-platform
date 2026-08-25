{% docs stg_ameyo__calls %}
One row per Ameyo call leg dialled. `call_id` (Ameyo's `ch_call_id`) is the
natural key.

**Local time, not UTC.** `called_at_local` is the contact centre's local
time, deliberately not converted to UTC here — a per-market timezone lookup
belongs in the intermediate/marts layers (where it's actually needed for a
business decision), not in staging, whose job is a faithful typed mirror of
the source.

**Explicit casts.** `called_at_local` and `talk_time_ms` are cast explicitly
because Airbyte's file-based S3 source lands every raw column as `varchar`
regardless of actual content (no schema inference configured) — unlike
`ingestion/local_loader`, which infers real types directly from the CSV on
the local/DuckDB path. Both paths must produce the same typed shape here so
nothing downstream needs to know which path loaded a given row.
{% enddocs %}

{% docs stg_atlas__dispositions %}
One row per call disposed of in Atlas against a customer contract.
`call_log_id` is the primary key — the id agents are supposed to paste back
into Ameyo's notes field.

**Explicit cast on `disposed_at_utc`.** Airbyte's raw columns are all
`varchar` (no schema inference configured), unlike `local_loader`'s DuckDB
output. This cast is required, not just tidy: `int_calls_payment_attribution`
`COALESCE`s `max(disposed_at_utc)` against a typed timestamp literal, which
Redshift refuses to do across `varchar`/`timestamp` even though plain
comparisons elsewhere tolerate the implicit cast fine — confirmed by a real
error, not by reading the docs.

**Materialization.** A table (not the staging-layer default of view), with
an explicit Redshift distkey/sortkey. This table feeds the payment-
attribution range join (`contract_id` + a `disposed_at_utc BETWEEN`) that
`int_calls_payment_attribution` itself flags as the first thing that would
blow up cost/time at production volume. `dist='contract_id'` matches
`stg_atlas__payments`' distkey so Redshift can merge-join the two without a
network redistribution; `sort=['disposed_at_utc']` lets every downstream
incremental filter on that column prune via zone maps instead of a full
scan through a view on every run. Both are Redshift-only config keys,
silently ignored on DuckDB.
{% enddocs %}

{% docs stg_atlas__payments %}
One row per payment transaction per contract. `amount_local` is in the
local currency of the market (KES/UGX/TZS/NGN) — converted to USD later via
`stg_fx__rates`, not here.

**Left in deliberately.** Whitespace on `payment_request_provider` is
trimmed (a pure formatting fix), but non-positive amounts and non-provider
values (`MANUAL`, `UNKNOWN`, `LOYALTY_DISCOUNT`) are left in as-is —
excluding them is a business decision that belongs in the marts layer, not
something to silently drop at landing. See `docs/WRITEUP.md` for the
`LOYALTY_DISCOUNT` reasoning specifically (it reads like a non-cash credit,
not money collected).

**Explicit cast on `amount_local`.** Airbyte's file-based S3 source lands
every raw column as `varchar` regardless of actual content, unlike
`local_loader`'s DuckDB output, which infers real types from the CSV.
Comparisons/joins on other varchar raw columns tolerate an implicit cast
fine on both engines, but `amount_local` feeds an arithmetic division
(`amount_local / local_units_per_usd` in `fct_value_recovered_usd`), which
Redshift refuses to do on varchar without an explicit cast — confirmed by a
real `operator does not exist: character varying / double precision` error.

**Materialization.** A table with `dist='contract_id'`, matching
`stg_atlas__dispositions`' distkey — same reasoning: this is one side of
the flagged expensive range join in `int_calls_payment_attribution`, and
collocating both sides on the same distkey avoids a network redistribution
for it. `sort=['pay_timestamp_utc']` supports that model's incremental
filter. Both ignored (harmlessly) on DuckDB.
{% enddocs %}

{% docs stg_atlas__agent_mapping %}
One row per agent (`ameyo_user_id`). Bridges Ameyo logins to Atlas
identities/teams — there is no shared user key between the two systems
otherwise.

**Deduplication.** The raw file has 28 duplicate `ameyo_user_id` values: 14
are byte-for-byte exact duplicates (`DISTINCT` removes them with no
information loss); 14 have genuine conflicts — mostly cosmetic, but 4
disagree on `team` itself. There is no timestamp in this file to determine
which row is current, so ties are broken deterministically (alphabetically-
first `atlas_user_name`) rather than guessed at — a documented, repeatable
choice, not a claim that the winning row is correct.
{% enddocs %}

{% docs stg_fx__rates %}
One row per (`rate_date`, `currency`). `local_units_per_usd` is how many
units of that currency equal 1 USD, i.e.
`amount_local / local_units_per_usd = USD`. Fetched from an external API
(see `ingestion/fx_rates/`) — not part of the original four case-study
extracts.
{% enddocs %}
