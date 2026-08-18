FROM apache/superset:latest

USER root

# Redshift driver (sqlalchemy-redshift + its redshift_connector DBAPI) and DuckDB
# driver (duckdb + duckdb-engine, the SQLAlchemy dialect) -- the two warehouses this
# deployment connects to, so a reviewer can build dashboards/run queries against
# either the local DuckDB target or the production Redshift target.

RUN uv pip install --python /app/.venv/bin/python \
    "sqlalchemy<2" \
    sqlalchemy-redshift \
    redshift_connector \
    duckdb \
    duckdb-engine

COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.py /app/bootstrap.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

USER superset

CMD ["/app/entrypoint.sh"]
