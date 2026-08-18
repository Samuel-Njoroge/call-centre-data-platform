#!/bin/sh
set -e

superset db upgrade

# create-admin errors on a restart (user already exists) -- not a real failure,
# the metadata volume already has it.
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password "$SUPERSET_ADMIN_PASSWORD" || true

superset init

python /app/bootstrap.py

exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 1 \
    --timeout 120 \
    "superset.app:create_app()"
