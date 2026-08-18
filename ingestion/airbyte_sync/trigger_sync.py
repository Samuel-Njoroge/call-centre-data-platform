"""
Triggers the Airbyte connection sync (S3 raw/ -> Redshift raw.*) via Airbyte's public
REST API and blocks until it reaches a terminal state.

Dagster owns triggering + waiting here, rather than relying on Airbyte's own
independent connection schedule, so "the transform waits for the load" (the brief's
orchestration requirement) holds for the redshift target the same way it does for the
local target's direct load_raw() call. 

Auth tokens expire in 900s, so a long-running sync (the 849k-row atlas_payments
stream took ~90 minutes in testing) needs to refresh the token mid-poll, not just once
at the start.
"""

import os
import sys
import time

import requests

DEFAULT_TIMEOUT_SECONDS = 3 * 60 * 60  
POLL_INTERVAL_SECONDS = 15
TOKEN_LIFETIME_SECONDS = 900
TOKEN_REFRESH_MARGIN_SECONDS = 60
HTTP_MAX_ATTEMPTS = 3
HTTP_RETRY_BACKOFF_SECONDS = 5
TERMINAL_STATUSES = {"succeeded", "failed", "cancelled"}


class AirbyteSyncError(RuntimeError):
    pass


def _request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    """Retries connection errors and 5xx only -- a real API restart was observed
    mid-session (memory-pressure-induced pod restart, surfaced as a 502) and
    shouldn't fail the whole pipeline run on its own. 4xx is NOT retried -- confirmed
    by hitting a real 409 ("sync already running") during testing, where retrying
    only wasted time before failing anyway, since the conflict can't resolve in
    seconds. A 4xx means the request itself is wrong (bad auth, bad connection id,
    already-running conflict) and should surface immediately, same as the job
    itself reaching status=failed."""
    last_error = None
    for attempt in range(1, HTTP_MAX_ATTEMPTS + 1):
        try:
            resp = requests.request(method, url, timeout=30, **kwargs)
        except requests.RequestException as exc:
            last_error = exc
            if attempt < HTTP_MAX_ATTEMPTS:
                time.sleep(HTTP_RETRY_BACKOFF_SECONDS)
            continue

        if resp.status_code < 500:
            resp.raise_for_status()  # 4xx raises immediately here, not retried
            return resp

        last_error = AirbyteSyncError(f"{method} {url} -> HTTP {resp.status_code}: {resp.text[:200]}")
        if attempt < HTTP_MAX_ATTEMPTS:
            time.sleep(HTTP_RETRY_BACKOFF_SECONDS)
    raise AirbyteSyncError(f"Giving up on {method} {url} after {HTTP_MAX_ATTEMPTS} attempts: {last_error}")


def _get_token(base_url: str, client_id: str, client_secret: str) -> str:
    resp = _request_with_retry(
        "POST",
        f"{base_url}/api/public/v1/applications/token",
        json={"client_id": client_id, "client_secret": client_secret},
    )
    return resp.json()["access_token"]


def _find_running_job(base_url: str, token: str, connection_id: str) -> dict | None:
    resp = _request_with_retry(
        "GET",
        f"{base_url}/api/public/v1/jobs",
        headers={"Authorization": f"Bearer {token}"},
        params={"connectionId": connection_id, "status": "running", "limit": 1},
    )
    jobs = resp.json().get("data", [])
    return jobs[0] if jobs else None


def trigger_and_wait(timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> dict:
    """Starts a sync on AIRBYTE_CONNECTION_ID and blocks until success/failure.

    Idempotent under concurrent triggers: if a sync for this connection is already
    running (Airbyte itself enforces one-at-a-time per connection via a 409 on the
    trigger call -- confirmed by hitting this for real during testing), this attaches
    to and waits on the already-running job instead of erroring or leaving a
    duplicate in flight.
    """
    base_url = os.environ.get("AIRBYTE_API_URL", "http://localhost:8000")
    client_id = os.environ["AIRBYTE_CLIENT_ID"]
    client_secret = os.environ["AIRBYTE_CLIENT_SECRET"]
    connection_id = os.environ["AIRBYTE_CONNECTION_ID"]

    token = _get_token(base_url, client_id, client_secret)
    token_fetched_at = time.monotonic()

    try:
        resp = _request_with_retry(
            "POST",
            f"{base_url}/api/public/v1/jobs",
            headers={"Authorization": f"Bearer {token}"},
            json={"connectionId": connection_id, "jobType": "sync"},
        )
        job_id = resp.json()["jobId"]
    except requests.exceptions.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 409:
            raise
        existing = _find_running_job(base_url, token, connection_id)
        if existing is None:

            raise
        job_id = existing["jobId"]
        print(
            f"A sync was already in progress for this connection (job {job_id}) -- "
            "attaching to it instead of starting a duplicate."
        )

    start = time.monotonic()
    while True:
        if time.monotonic() - start > timeout_seconds:
            raise AirbyteSyncError(f"Airbyte sync job {job_id} did not finish within {timeout_seconds}s")

        if time.monotonic() - token_fetched_at > (TOKEN_LIFETIME_SECONDS - TOKEN_REFRESH_MARGIN_SECONDS):
            token = _get_token(base_url, client_id, client_secret)
            token_fetched_at = time.monotonic()

        resp = _request_with_retry(
            "GET",
            f"{base_url}/api/public/v1/jobs/{job_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        job = resp.json()
        status = job.get("status")

        if status == "succeeded":
            return job
        if status in ("failed", "cancelled"):
            raise AirbyteSyncError(f"Airbyte sync job {job_id} ended with status={status}: {job}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        result = trigger_and_wait()
        print(f"Airbyte sync succeeded: {result.get('rowsSynced')} rows, {result.get('bytesSynced')} bytes")
    except AirbyteSyncError as exc:
        print(f"Airbyte sync failed: {exc}", file=sys.stderr)
        sys.exit(1)
