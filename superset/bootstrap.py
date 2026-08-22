"""
Idempotently creates the two warehouse connections this deployment needs --
DuckDB (local target) and Redshift (production target) -- so a reviewer gets
both available without manually configuring anything through the UI. Reads
Redshift credentials from the environment (.env, gitignored) rather than a
committed file -- unlike dashboard/chart definitions, a database connection
embeds a password and has no business being versioned in git.

Run once at container startup, after `superset db upgrade` and `superset
init` (needs the metadata schema and default roles to already exist).
"""

import os
import uuid

from superset.app import create_app

app = create_app()

# Fixed UUIDs matching superset/exports/dashboard_export.zip's database/dataset
# YAMLs -- so on a brand-new install, the objects created below are the exact
# same objects the bundled dashboard import (further down) attaches its charts
# to, rather than creating a second, duplicate copy. Only ever applied when an
# object is created fresh; an already-existing object's uuid is never touched.
LOCAL_DB_UUID = "05025255-d7fe-4d36-a5c7-f31acbb220be"
DATASET_FIXED_UUIDS = {
    "local_fct_coding_rate": "59598bcf-7bb4-43ba-add6-4fc4fdf14056",
    "local_fct_paid_post_call": "f80612da-6049-483f-be3a-9b24efcc38d5",
    "local_fct_value_recovered_usd": "5716d1fc-4274-4901-8001-437b74e8811f",
    "local_fct_inbound_call_drivers": "21c45615-3a4b-4683-a2ca-1c70e6e41d5b",
}

with app.app_context():
    from superset import db
    from superset.models.core import Database

    def upsert_database(name: str, uri: str, fixed_uuid: str | None = None) -> None:
        existing = db.session.query(Database).filter_by(database_name=name).first()
        if existing:
            existing.sqlalchemy_uri = uri
            print(f"Updated existing database connection: {name}")
        else:
            kwargs = {"database_name": name, "sqlalchemy_uri": uri}
            if fixed_uuid:
                kwargs["uuid"] = uuid.UUID(fixed_uuid)
            database = Database(**kwargs)
            db.session.add(database)
            print(f"Created database connection: {name}")
        db.session.commit()

    # Local target: DuckDB file, bind-mounted read-only from the host repo (see
    # docker-compose.yml). access_mode=read_only lets Superset's connection coexist
    # with a concurrent Dagster write attempt failing gracefully rather than
    # Superset itself holding a write lock nothing else could get past -- DuckDB's
    # single-writer constraint is still real either way (see docs/WRITEUP.md), this
    # just keeps Superset itself from being the thing that blocks a pipeline run.
    upsert_database(
        "Local (DuckDB)",
        "duckdb:////workspace/warehouse_local.duckdb?access_mode=read_only",
        fixed_uuid=LOCAL_DB_UUID,
    )

    # Production target: Redshift, superset_reader (read-only, all schemas -- see
    # infra/redshift.sql).
    redshift_host = os.environ["REDSHIFT_HOST"]
    redshift_port = os.environ["REDSHIFT_PORT"]
    redshift_db = os.environ["REDSHIFT_DATABASE"]
    redshift_user = os.environ["REDSHIFT_SUPERSET_USER"]
    redshift_password = os.environ["REDSHIFT_SUPERSET_PASSWORD"]
    upsert_database(
        "Production (Redshift)",
        f"redshift+redshift_connector://{redshift_user}:{redshift_password}"
        f"@{redshift_host}:{redshift_port}/{redshift_db}",
    )

    print("Database connections bootstrapped.")

    # Register the 5 marts tables as Datasets on both connections -- mechanical
    # plumbing (same table names/columns either way), not a design decision, so
    # it's automated the same way the connections themselves are. Building the
    # actual charts/dashboards on top of these is still a manual, deliberate step
    # in the Superset UI, not scripted here.
    #
    # Virtual datasets (explicit SQL), not a direct physical-table reference:
    # Superset's dataset table has a UNIQUE constraint on table_name ALONE, not
    # scoped per-connection -- confirmed by a real IntegrityError trying to
    # register "fct_coding_rate" for both connections, since they're
    # identically-named tables. A prefixed table_name per connection
    # ("local_fct_coding_rate", "redshift_fct_coding_rate") avoids the
    # collision, and pointing it at explicit SQL rather than the physical table
    # sidesteps a second real quirk: duckdb-engine reports DuckDB's schema as
    # "warehouse_local.marts" (catalog-prefixed) via introspection, not plain
    # "marts" like Redshift -- but "SELECT * FROM marts.<table>" is valid SQL on
    # both engines regardless of that introspection difference.
    from superset.connectors.sqla.models import SqlaTable

    MART_TABLES = [
        "fct_coding_rate",
        "fct_paid_post_call",
        "fct_value_recovered_usd",
        "fct_inbound_call_drivers",
        "dim_agent",
    ]

    CONNECTIONS = [
        ("Local (DuckDB)", "local"),
        ("Production (Redshift)", "redshift"),
    ]

    for db_name, prefix in CONNECTIONS:
        database = db.session.query(Database).filter_by(database_name=db_name).one()
        for table_name in MART_TABLES:
            dataset_name = f"{prefix}_{table_name}"
            dataset = db.session.query(SqlaTable).filter_by(table_name=dataset_name).first()
            is_new = dataset is None
            if dataset:
                print(f"Dataset already registered: {dataset_name}")
            else:
                kwargs = {
                    "table_name": dataset_name,
                    "database": database,
                    "sql": f"SELECT * FROM marts.{table_name}",
                }
                if dataset_name in DATASET_FIXED_UUIDS:
                    kwargs["uuid"] = uuid.UUID(DATASET_FIXED_UUIDS[dataset_name])
                dataset = SqlaTable(**kwargs)
                db.session.add(dataset)
                db.session.commit()
                print(f"Created dataset: {dataset_name}")

            # Local (DuckDB, a file on disk) is always (re)synced on every startup, not
            # just on creation -- self-heals a dataset whose columns were never
            # populated because marts.* didn't exist yet the first time this ran (dbt
            # hadn't built), which the old create-only fetch left permanently stale
            # until someone fixed it by hand. Redshift keeps the original creation-only
            # fetch instead -- unlike DuckDB this is a real network call, and while
            # Redshift is unreachable (no prod creds yet) each attempt blocks on a
            # ~110s TCP timeout; retrying all 5 on every container restart would add
            # ~9 minutes to every restart for no benefit until Redshift is actually
            # configured. Not a silent gap either way: an unsynced dataset still
            # lazy-loads its columns in the UI same as it always could.
            if prefix == "local" or is_new:
                try:
                    dataset.fetch_metadata()
                    db.session.commit()
                    print(f"Synced columns: {dataset_name}")
                except Exception as exc:
                    print(f"Columns not synced (will lazy-load in UI): {dataset_name} -- {exc}")

    print("Datasets bootstrapped.")

    # Bundled dashboard/charts (superset/exports/dashboard_export.zip) -- imported
    # once, only if a dashboard with this title doesn't already exist. This is what
    # makes a first-time `docker compose up` come with the built dashboard already
    # in place rather than an empty Superset instance someone has to rebuild by
    # hand. Guarded by dashboard_title (not just "run once") so that an instance
    # that already has this dashboard -- including one a reviewer has since
    # customized -- is never touched by a later container restart re-running this
    # script; only a genuinely fresh instance gets the import.
    import subprocess

    from superset.models.dashboard import Dashboard

    DASHBOARD_TITLE = "d.light Call Centre Daily Effectiveness Dashboard"
    EXPORT_PATH = "/workspace/superset/exports/dashboard_export.zip"

    existing_dashboard = (
        db.session.query(Dashboard).filter_by(dashboard_title=DASHBOARD_TITLE).first()
    )
    if existing_dashboard:
        print(f"Dashboard already present: {DASHBOARD_TITLE!r} -- skipping bundled import.")
    elif not os.path.exists(EXPORT_PATH):
        print(f"No bundled dashboard export at {EXPORT_PATH} -- skipping.")
    else:
        print("Importing bundled dashboard (first run on this instance)...")
        subprocess.run(
            ["superset", "import-dashboards", "-p", EXPORT_PATH, "-u", "admin"],
            check=True,
        )
        print("Dashboard imported.")
