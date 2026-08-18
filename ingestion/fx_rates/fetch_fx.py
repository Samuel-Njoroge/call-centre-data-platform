"""
Exchange rate ingestion for Metric 3 (value recovered in USD).

  1. Auth: the API key is read from an env var, never hardcoded, never logged.
  2. Failure: the provider will eventually be slow, rate-limited, or down --
     retried with exponential backoff, and Retry-After is honoured on 429s.
  3. Idempotency: writes are delete+insert keyed on (rate_date, currency), so
     re-running this on the same day never duplicates a day's rates.

Provider: exchangerate-api.com (v6, /latest/USD). Rates come back as "1 USD =
X units of local currency" -- stored as-is (local_units_per_usd), converted
at read time in dbt (amount_local / local_units_per_usd = amount_usd), not
inverted here, so the raw table stays a faithful copy of what the API said.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import requests

TARGET_CURRENCIES = ["KES", "UGX", "TZS", "NGN"]
API_URL_TEMPLATE = "https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"
MAX_ATTEMPTS = 5
BASE_BACKOFF_SECONDS = 1.0


class FxFetchError(RuntimeError):
    pass


def fetch_rates_from_api() -> dict:
    """Auth + retry + parse. Returns {currency: local_units_per_usd}."""
    api_key = os.environ.get("EXCHANGERATE_API_KEY")
    if not api_key:
        raise FxFetchError(
            "EXCHANGERATE_API_KEY is not set. Add it to .env (never commit "
            "the key itself) -- sign up for a free key at exchangerate-api.com."
        )

    url = API_URL_TEMPLATE.format(api_key=api_key)
    last_error = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        default_delay = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
        delay = default_delay

        try:
            response = requests.get(url, timeout=10)
        except requests.RequestException as exc:
            last_error = exc
        else:
            if response.status_code == 200:
                payload = response.json()
                if payload.get("result") != "success":
                    raise FxFetchError(f"API returned an error payload: {payload}")
                rates = payload["conversion_rates"]
                missing = [c for c in TARGET_CURRENCIES if c not in rates]
                if missing:
                    raise FxFetchError(f"API response is missing currencies: {missing}")
                return {c: rates[c] for c in TARGET_CURRENCIES}

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    delay = float(retry_after)
                last_error = FxFetchError(f"Rate limited (429), retrying after {delay}s")
            elif 500 <= response.status_code < 600:
                last_error = FxFetchError(f"Server error {response.status_code}, retrying after {delay}s")
            else:
                # Not retryable -- bad key, bad request, etc.
                raise FxFetchError(f"API request failed: {response.status_code} {response.text[:200]}")

        if attempt < MAX_ATTEMPTS:
            time.sleep(delay)

    raise FxFetchError(f"Giving up after {MAX_ATTEMPTS} attempts. Last error: {last_error}")


def _rows_for_today(rates: dict) -> list:
    # tzinfo stripped deliberately: DuckDB silently converts a tz-aware
    # datetime to LOCAL system wall-clock time when binding it into a naive
    # TIMESTAMP column, which would make the stored value depend on which
    # machine/timezone ran this script rather than being a stable UTC value
    # -- confirmed by testing on this machine (UTC+3), where fetched_at was
    # coming back 3 hours ahead of the actual UTC fetch time. Every other
    # UTC timestamp column in this repo is naive-but-UTC (from CSV parsing,
    # which never had tzinfo to begin with); this matches that convention
    # explicitly rather than relying on the same implicit conversion never
    # biting again.
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    rate_date = now_utc.date()
    fetched_at = now_utc
    return [
        (rate_date, currency, rate, fetched_at, "exchangerate-api.com")
        for currency, rate in rates.items()
    ]


def upsert_duckdb(rates: dict, db_path: str) -> None:
    import duckdb

    rows = _rows_for_today(rates)
    rate_date = rows[0][0]

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.fx_rates (
            rate_date DATE,
            currency VARCHAR,
            local_units_per_usd DOUBLE,
            fetched_at TIMESTAMP,
            source VARCHAR
        )
        """
    )
    con.execute("DELETE FROM raw.fx_rates WHERE rate_date = ?", [rate_date])
    con.executemany(
        "INSERT INTO raw.fx_rates VALUES (?, ?, ?, ?, ?)", rows
    )
    con.close()


def upsert_redshift(rates: dict) -> None:
    import redshift_connector

    rows = _rows_for_today(rates)
    rate_date = rows[0][0]

    conn = redshift_connector.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ["REDSHIFT_PORT"]),
        database=os.environ["REDSHIFT_DATABASE"],
        user=os.environ["REDSHIFT_AIRBYTE_USER"],
        password=os.environ["REDSHIFT_AIRBYTE_PASSWORD"],
    )
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS "raw".fx_rates (
            rate_date DATE,
            currency VARCHAR(3),
            local_units_per_usd DOUBLE PRECISION,
            fetched_at TIMESTAMP,
            source VARCHAR(64)
        )
        """
    )
    cur.execute('DELETE FROM "raw".fx_rates WHERE rate_date = %s', (rate_date,))
    cur.executemany(
        'INSERT INTO "raw".fx_rates VALUES (%s, %s, %s, %s, %s)', rows
    )
    conn.commit()
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["local", "redshift"], required=True)
    parser.add_argument("--duckdb-path", default="warehouse_local.duckdb")
    args = parser.parse_args()

    rates = fetch_rates_from_api()

    if args.target == "local":
        upsert_duckdb(rates, args.duckdb_path)
    else:
        upsert_redshift(rates)

    print(f"fx_rates updated for {datetime.now(timezone.utc).date()}: {rates}")


if __name__ == "__main__":
    try:
        main()
    except FxFetchError as exc:
        print(f"FX fetch failed: {exc}", file=sys.stderr)
        sys.exit(1)
