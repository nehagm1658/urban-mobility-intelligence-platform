-- scripts/airflow_db_init.sql
-- Runs automatically on first Postgres container startup (alongside
-- db_setup.sql, both mounted into /docker-entrypoint-initdb.d/). Creates a
-- separate database for Airflow's own scheduler/webserver metadata,
-- kept isolated from umip_platform (the pipeline's actual data) so the
-- two are never confused and Airflow's tables don't clutter the
-- platform's schemas.
CREATE DATABASE airflow_meta;
