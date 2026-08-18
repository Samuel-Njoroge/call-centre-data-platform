# Raw data landing convention

## File naming

The four extracts described in section 3 of the case study brief have been renamed from their
original (space-separated, mixed-case) filenames to a `{source_system}_{entity}.csv` convention,
so they sort/group by source system and map cleanly onto raw table names. Content is unchanged —
only the filename differs.

| Brief section | Original filename              | Renamed to                    |
|---------------|---------------------------------|--------------------------------|
| 3.1           | Outbound Calls Ameyo.csv        | `ameyo_outbound_calls.csv`     |
| 3.2           | Calls Dispositions Atlas.csv    | `atlas_calls_dispositions.csv` |
| 3.3           | Payments Data Atlas.csv         | `atlas_payments.csv`           |
| 3.4           | Atlas Ameyo Mapping.csv         | `atlas_ameyo_mapping.csv`      |

## Partitioning

Files are laid out under `data/raw/{YYYY-MM-DD}/`, one folder per day of extracts:

```
data/raw/
  2026-08-15/
    ameyo_outbound_calls.csv
    atlas_calls_dispositions.csv
    atlas_payments.csv
    atlas_ameyo_mapping.csv
  2026-08-16/
    ...
```

New date folders are picked up automatically by both loaders below — nothing needs to be edited
when tomorrow's files arrive.

## Two ingestion paths, one raw contract

This repo can populate the warehouse's `raw` schema two ways. Both produce the same table/column
shape; nothing downstream (dbt, Dagster, Superset) cares which one ran.

- **Local (no AWS needed):** `ingestion/local_loader/load_raw.py` reads these files directly into
  a local DuckDB file. This is the zero-credential path for anyone reviewing this repo.
- **Production:** files are uploaded to a real S3 bucket, then Airbyte's S3 source connector lands
  them in Redshift. See [`ingestion/README.md`](../ingestion/README.md).

## Known data quirk

`wc -l` on these files overcounts rows — several free-text fields (e.g. `udh_notes`,
`level_two`/`level_three`) contain literal embedded newlines inside quoted CSV values. A
CSV-aware parser (confirmed with both Python's `csv` module and DuckDB) gives the correct logical
row count; no rows are lost or rejected by either loader.
