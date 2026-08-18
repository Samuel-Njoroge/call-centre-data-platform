# Ingestion

Two independent loaders converge on the same `raw` schema contract — see
[`data/README.md`](../data/README.md) for the shared file-naming/partitioning convention they
both rely on.

## Status

| Path | Status |
|---|---|
| `local_loader/` (DuckDB, no AWS) | Done, verified idempotent (see below) |
| S3 upload of `data/raw/` -> production bucket | Done, verified idempotent — `s3_upload/upload_to_s3.py`, `raw-data-ingestor` identity |
| Airbyte (S3 source -> Redshift destination) | Done, verified end-to-end — see below |

## local_loader/

`python ingestion/local_loader/load_raw.py`

Reads every `data/raw/{date}/{entity}.csv` directly into `warehouse_local.duckdb`, one table per
entity under a `raw` schema. Each run drops and rebuilds every raw table from whatever files
currently exist on disk, so re-running never duplicates rows, and a new date folder is picked up
without touching the script.

Verified: ran twice back to back, identical row counts both times —

```
raw.ameyo_outbound_calls:      10508 rows
raw.atlas_calls_dispositions:  52132 rows
raw.atlas_payments:           849306 rows
raw.atlas_ameyo_mapping:        1618 rows
```

Two lineage columns are added on load: `_source_file` (which CSV the row came from) and
`_loaded_at` (UTC load timestamp) — keeps the raw layer traceable without touching the original
column set.

## Production path (S3 -> Airbyte -> Redshift)

Built and verified end-to-end:
1. `s3_upload/upload_to_s3.py` uploads `data/raw/**` to `s3://$S3_RAW_BUCKET/raw/...`, using
   `raw-data-ingestor` credentials (represents an upstream system dropping extracts into the
   bucket — outside the pipeline itself, same as the four CSVs being given to us rather than
   produced by us).
2. Airbyte (`abctl local install --low-resource-mode`) — S3 source (`airbyte-s3-reader`, one
   stream per entity via glob `raw/**/{entity}.csv`) -> Redshift destination (`airbyte_writer` DB
   user + `airbyte-redshift-writer` IAM for S3-staged COPY via `airbyte-staging/`), connection sync
   mode `incremental` (source, file-modified-time cursor) / `append` (destination) per stream --
   raw reflects exactly what arrived, deduplicated only later in dbt staging, and each daily sync
   picks up only new/changed files rather than re-reading and re-appending history.
3. Configured via direct scripting against Airbyte's REST API (`octavia-cli`, originally planned
   for this, turned out to be dead -- last released mid-2023, no longer in Airbyte's repo, and
   wildly incompatible with the `2.2.0` platform abctl installs today).

First full sync: 913,564 rows landed in Redshift `raw.*`, exact match against all four expected
table counts (10,508 / 52,132 / 849,306 / 1,618).