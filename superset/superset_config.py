"""
SQLite metadata backend (Superset's own state -- saved charts, dashboards,
users), not Postgres. Lighter (one fewer container) 
The SQLite file lives on the superset_home named volume (see
docker-compose.yml), not the container's own layer, so it survives restarts.
"""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
