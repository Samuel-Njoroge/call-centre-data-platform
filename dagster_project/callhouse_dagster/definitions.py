"""
Orchestrates the local pipeline: load raw data -> fetch FX rates -> dbt build.
Op-based (not asset-based dbt lineage) deliberately -- explicit op
dependencies give an unambiguous "transform waits for load" guarantee without
needing dagster-dbt's automatic dbt-source-to-asset key mapping to be exactly
right, which is real complexity for real fragility risk this project doesn't
need. Failure in any op fails the run visibly in the Dagster UI; a re-run
re-executes from scratch, which is safe because every step downstream
(local_loader, fetch_fx, dbt incremental models) is independently idempotent
-- already verified individually.

Runs identically whether this process is started natively (venv) or inside
the Dagster container: REPO_ROOT is resolved from this file's own location at
runtime, not hardcoded, so the same relative repo layout resolves correctly
either way.
"""

import subprocess
import sys
from pathlib import Path
from typing import Literal

from dagster import (
    ConfigurableResource,
    Definitions,
    In,
    Nothing,
    ScheduleDefinition,
    job,
    op,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = REPO_ROOT / "dbt"


class TargetResource(ConfigurableResource):
    """Picks local (DuckDB) vs redshift per run, from the Launchpad's run config --
    not a container-level env var, so switching pipelines no longer needs a
    container restart, just a different choice at launch time."""

    target: Literal["local", "redshift"] = "local"


def _add_ingestion_path(subdir: str) -> None:
    """Adds an ingestion/<subdir> directory to sys.path.

    Called at the top of each op body, not just once at module import time --
    Dagster executes ops in worker processes/subprocesses that re-import this
    module independently, and a module-level sys.path mutation in one process
    does not reliably carry over into whichever process actually runs an op
    (confirmed by hitting a real ModuleNotFoundError for load_raw/fetch_fx when
    running via `dagster job launch`, despite the module loading fine when
    dagster dev first discovers it). Doing this per-op, right before the lazy
    import that needs it, is what actually makes it correct everywhere.
    """
    path = str(REPO_ROOT / "ingestion" / subdir)
    if path not in sys.path:
        sys.path.insert(0, path)


@op
def load_raw_data(context, target_resource: TargetResource) -> None:
    target = target_resource.target
    if target == "local":
        _add_ingestion_path("local_loader")
        from load_raw import load

        load()
        context.log.info("raw calls/dispositions/payments/mapping loaded (local)")
    else:
        _add_ingestion_path("s3_upload")
        from upload_to_s3 import upload

        upload()
        context.log.info("data/raw uploaded to S3")

        _add_ingestion_path("airbyte_sync")
        from trigger_sync import trigger_and_wait

        result = trigger_and_wait()
        context.log.info(
            f"Airbyte sync complete: {result.get('rowsSynced')} rows, "
            f"{result.get('bytesSynced')} bytes"
        )


@op(ins={"start_after": In(Nothing)})
def load_fx_rates(context, target_resource: TargetResource) -> None:
    _add_ingestion_path("fx_rates")
    from fetch_fx import fetch_rates_from_api, upsert_duckdb

    rates = fetch_rates_from_api()
    target = target_resource.target
    if target == "local":
        upsert_duckdb(rates, str(REPO_ROOT / "warehouse_local.duckdb"))
    else:
        from fetch_fx import upsert_redshift

        upsert_redshift(rates)
    context.log.info(f"fx_rates updated: {rates}")


@op(ins={"start_after": In(Nothing)})
def run_dbt_build(context, target_resource: TargetResource) -> None:
    target = target_resource.target
    result = subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
            "--target",
            target,
        ],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dbt build failed (exit {result.returncode})")


@job(
    tags={
        # Cosmetic only -- shown as a chip next to the run ID in the Runs list, without
        # splitting this into separate jobs (which would need its own schedule/sensor
        # chaining to keep "transform waits for load" true, undoing the sequencing
        # fixes above). No "target" tag here -- unlike the old env-var approach, target
        # is now a per-run choice (TargetResource, set in the Launchpad), not something
        # fixed at job-definition time.
        "stages": "raw_ingestion+fx_rates+dbt_build",
    },
)
def callhouse_pipeline() -> None:
    # load_fx_rates explicitly sequenced after load_raw_data, not run in parallel with
    # it -- for the local target, both open write connections to the same
    # warehouse_local.duckdb file, and DuckDB allows only one writer at a time. Running
    # them as independent (parallel) ops, as originally written, hit a real
    # "Conflicting lock is held" IOException when actually executed through Dagster's
    # multiprocess executor -- the case study's own ingestion scripts never exposed
    # this because they were always run one at a time by hand.
    raw_loaded = load_raw_data()
    fx_loaded = load_fx_rates(start_after=raw_loaded)
    run_dbt_build(start_after=[raw_loaded, fx_loaded])


callhouse_daily_schedule = ScheduleDefinition(
    job=callhouse_pipeline,
    cron_schedule="0 6 * * *",  # every morning, 06:00
    run_config={"resources": {"target_resource": {"config": {"target": "redshift"}}}},
)

defs = Definitions(
    jobs=[callhouse_pipeline],
    schedules=[callhouse_daily_schedule],
    resources={"target_resource": TargetResource()},
)
