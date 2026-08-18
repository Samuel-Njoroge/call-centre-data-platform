-- === Airbyte writer: raw schema only ===
CREATE SCHEMA IF NOT EXISTS "raw";
CREATE USER airbyte_writer PASSWORD 'PASSWORD';
GRANT USAGE, CREATE ON SCHEMA "raw" TO airbyte_writer;
GRANT ALL ON ALL TABLES IN SCHEMA "raw" TO airbyte_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA "raw" GRANT ALL ON TABLES TO airbyte_writer;
-- Redshift's destination connector creates temp tables during connection checks and
-- syncs (e.g. staging COPY intermediates) -- without database-level TEMP, every check
-- and sync fails with "permission denied for database dev", even though the schema
-- grants above are otherwise sufficient. Not implied by the schema-level grants.
GRANT TEMP ON DATABASE dev TO airbyte_writer;
-- The connector also unconditionally runs `CREATE SCHEMA IF NOT EXISTS "raw"` on every
-- check/sync, even though the schema already exists -- Postgres/Redshift checks
-- database-level CREATE privilege before checking whether the schema exists, so
-- IF NOT EXISTS does not bypass this. Schema-level CREATE (granted above) is not
-- the same privilege and does not cover this.
GRANT CREATE ON DATABASE dev TO airbyte_writer;

-- === dbt schemas ===
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- === dbt user: read raw, write staging/intermediate/marts ===
CREATE USER dbt_user PASSWORD 'PASSWORD';

GRANT USAGE ON SCHEMA "raw" TO dbt_user;
GRANT SELECT ON ALL TABLES IN SCHEMA "raw" TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA "raw" GRANT SELECT ON TABLES TO dbt_user;
-- ALTER DEFAULT PRIVILEGES is grantor-scoped: the line above only covers future
-- tables created by whoever runs this script, not tables airbyte_writer creates
-- tables it had just created, dbt_user saw none. Needs both a retroactive grant
-- (for tables that already exist) and a grantor-scoped default-privileges rule
-- (for tables Airbyte creates on every future sync).
GRANT SELECT ON ALL TABLES IN SCHEMA "raw" TO dbt_user;

ALTER DEFAULT PRIVILEGES FOR USER airbyte_writer IN SCHEMA "raw" GRANT SELECT ON TABLES TO dbt_user;

GRANT USAGE, CREATE ON SCHEMA staging TO dbt_user;
GRANT USAGE, CREATE ON SCHEMA intermediate TO dbt_user;
GRANT USAGE, CREATE ON SCHEMA marts TO dbt_user;
GRANT ALL ON ALL TABLES IN SCHEMA staging TO dbt_user;
GRANT ALL ON ALL TABLES IN SCHEMA intermediate TO dbt_user;
GRANT ALL ON ALL TABLES IN SCHEMA marts TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate GRANT ALL ON TABLES TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO dbt_user;

-- === Superset user: read-only, ALL schemas ===
CREATE USER superset_reader PASSWORD 'PASSWORD';

GRANT USAGE ON SCHEMA "raw" TO superset_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA "raw" TO superset_reader;
ALTER DEFAULT PRIVILEGES FOR USER airbyte_writer IN SCHEMA "raw" GRANT SELECT ON TABLES TO superset_reader;

GRANT USAGE ON SCHEMA staging TO superset_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO superset_reader;
ALTER DEFAULT PRIVILEGES FOR USER dbt_user IN SCHEMA staging GRANT SELECT ON TABLES TO superset_reader;

GRANT USAGE ON SCHEMA intermediate TO superset_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA intermediate TO superset_reader;
ALTER DEFAULT PRIVILEGES FOR USER dbt_user IN SCHEMA intermediate GRANT SELECT ON TABLES TO superset_reader;

GRANT USAGE ON SCHEMA marts TO superset_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO superset_reader;
ALTER DEFAULT PRIVILEGES FOR USER dbt_user IN SCHEMA marts GRANT SELECT ON TABLES TO superset_reader;