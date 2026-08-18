# The repo is bind-mounted at /workspace by docker-compose (see its `volumes:`
# entry) -- this image only bakes in the Python environment, not the code.
# That keeps code changes (ingestion scripts, dbt models) reflected without
# a rebuild, and guarantees the container sees the exact same relative file
# layout as running natively, so paths in definitions.py resolve identically
# either way.
FROM python:3.12-slim

WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV DAGSTER_HOME=/workspace/dagster_project/.dagster_home

EXPOSE 3000

CMD ["dagster", "dev", "-f", "dagster_project/callhouse_dagster/definitions.py", "-h", "0.0.0.0", "-p", "3000"]
