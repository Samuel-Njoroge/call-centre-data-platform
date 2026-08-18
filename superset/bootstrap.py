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

from superset.app import create_app

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.core import Database

    def upsert_database(name: str, uri: str) -> None:
        existing = db.session.query(Database).filter_by(database_name=name).first()
        if existing:
            existing.sqlalchemy_uri = uri
            print(f"Updated existing database connection: {name}")
        else:
            database = Database(database_name=name, sqlalchemy_uri=uri)
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
            existing = db.session.query(SqlaTable).filter_by(table_name=dataset_name).first()
            if existing:
                print(f"Dataset already registered: {dataset_name}")
                continue
            dataset = SqlaTable(
                table_name=dataset_name,
                database=database,
                sql=f"SELECT * FROM marts.{table_name}",
            )
            db.session.add(dataset)
            db.session.commit()
            try:
                dataset.fetch_metadata()
                db.session.commit()
                print(f"Registered dataset: {dataset_name}")
            except Exception as exc:
                print(f"Registered dataset (metadata fetch failed, will lazy-load in UI): {dataset_name} -- {exc}")

print("Datasets bootstrapped.")
