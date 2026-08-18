"""
Local-only raw loader — no AWS required.

Mirrors what the production path (S3 -> Airbyte -> Redshift) lands in the `raw`
schema, but reads directly from data/raw/ on disk so this repo is runnable by
anyone without AWS credentials.

Idempotent by construction: each run drops and rebuilds every raw table from
whatever files currently exist under data/raw/**/, so re-running never
duplicates rows. New date-partitioned folders (e.g. data/raw/2026-08-16/) are
picked up automatically — nothing here needs to change when tomorrow's files
arrive.
"""

from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = REPO_ROOT / "data" / "raw"
DB_PATH = REPO_ROOT / "warehouse_local.duckdb"

# table name -> filename to match under each data/raw/{date}/ folder
STREAMS = {
    "ameyo_outbound_calls": "ameyo_outbound_calls.csv",
    "atlas_calls_dispositions": "atlas_calls_dispositions.csv",
    "atlas_payments": "atlas_payments.csv",
    "atlas_ameyo_mapping": "atlas_ameyo_mapping.csv",
}


def load() -> None:
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    for table_name, filename in STREAMS.items():
        glob_pattern = (RAW_DATA_DIR / "*" / filename).as_posix()
        matches = list(RAW_DATA_DIR.glob(f"*/{filename}"))

        if not matches:
            raise FileNotFoundError(
                f"No files found for '{table_name}' matching {glob_pattern}"
            )

        con.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
        con.execute(
            f"""
            CREATE TABLE raw.{table_name} AS
            SELECT
                * EXCLUDE (filename),
                replace(replace(filename, '\\', '/'), '{REPO_ROOT.as_posix()}/', '') AS _source_file,
                current_timestamp AS _loaded_at
            FROM read_csv_auto(
                '{glob_pattern}',
                filename = true,
                union_by_name = true,
                all_varchar = false
            )
            """
        )

        count = con.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
        print(f"raw.{table_name}: {count} rows from {len(matches)} file(s)")

    con.close()
    print(f"\nDone. Local warehouse at {DB_PATH}")


if __name__ == "__main__":
    load()
