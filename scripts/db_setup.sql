-- ============================================================================
-- db_setup.sql
-- Creates all PostgreSQL schemas and tables used by the Urban Mobility
-- Intelligence Platform: source (operational data), metadata (pipeline run
-- tracking), audit (record-level audit trail), error (data quality rejects).
-- Safe to re-run: drops and recreates everything.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- SCHEMAS
-- ---------------------------------------------------------------------------
DROP SCHEMA IF EXISTS source CASCADE;
DROP SCHEMA IF EXISTS metadata CASCADE;
DROP SCHEMA IF EXISTS audit CASCADE;
DROP SCHEMA IF EXISTS error CASCADE;

CREATE SCHEMA source;
CREATE SCHEMA metadata;
CREATE SCHEMA audit;
CREATE SCHEMA error;

-- ---------------------------------------------------------------------------
-- SOURCE SCHEMA: operational tables that live in Postgres (system of record)
-- ---------------------------------------------------------------------------

-- Customers is our Postgres-native source (per project spec: customer master
-- data lives in the rider-facing operational database).
CREATE TABLE source.customers (
    customer_id     INT NOT NULL,
    customer_name   TEXT,
    phone_number    TEXT,
    email           TEXT,
    city            TEXT,
    signup_date     DATE,
    is_premium      BOOLEAN,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Deliberately NOT a PRIMARY KEY: this table models an upstream OLTP
-- export that can contain duplicate customer_id rows (a required bad-data
-- scenario for Silver's duplicate-PK validation to catch). A plain index
-- still supports fast lookups/joins.
CREATE INDEX idx_customers_customer_id ON source.customers (customer_id);
CREATE INDEX idx_customers_updated_at ON source.customers (updated_at);
CREATE INDEX idx_customers_city ON source.customers (city);

-- ---------------------------------------------------------------------------
-- METADATA SCHEMA: pipeline run tracking (one row per pipeline execution)
-- ---------------------------------------------------------------------------
CREATE TABLE metadata.pipeline_runs (
    run_id            SERIAL PRIMARY KEY,
    batch_id          TEXT NOT NULL,
    pipeline_name     TEXT NOT NULL,
    layer             TEXT NOT NULL,           -- bronze / silver / gold
    entity            TEXT NOT NULL,           -- drivers, trips, gold_fact_trip, etc.
    execution_start   TIMESTAMP NOT NULL,
    execution_end     TIMESTAMP,
    duration_seconds  NUMERIC,
    record_count      INT DEFAULT 0,
    insert_count      INT DEFAULT 0,
    update_count      INT DEFAULT 0,
    reject_count      INT DEFAULT 0,
    status            TEXT NOT NULL,           -- SUCCESS / FAILED / PARTIAL
    error_message     TEXT,
    execution_host    TEXT,
    pipeline_version  TEXT,
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pipeline_runs_batch ON metadata.pipeline_runs (batch_id);
CREATE INDEX idx_pipeline_runs_entity ON metadata.pipeline_runs (entity);

-- Watermarks for incremental loads (per entity)
CREATE TABLE metadata.watermarks (
    entity            TEXT PRIMARY KEY,
    watermark_column  TEXT NOT NULL,
    watermark_value   TEXT,
    updated_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- AUDIT SCHEMA: high-level audit trail, one row per pipeline stage execution
-- ---------------------------------------------------------------------------
CREATE TABLE audit.audit_log (
    audit_id                SERIAL PRIMARY KEY,
    batch_id                TEXT NOT NULL,
    pipeline_name           TEXT NOT NULL,
    layer                   TEXT NOT NULL,
    processed_records       INT DEFAULT 0,
    rejected_records        INT DEFAULT 0,
    inserted_records        INT DEFAULT 0,
    updated_records         INT DEFAULT 0,
    execution_duration_sec  NUMERIC,
    pipeline_status         TEXT NOT NULL,
    logged_at               TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- ERROR SCHEMA: data quality rejects and pipeline exceptions
-- ---------------------------------------------------------------------------

-- Rejected records from Silver validation (schema/null/PK/type/duplicate failures)
CREATE TABLE error.rejected_records (
    reject_id         SERIAL PRIMARY KEY,
    batch_id          TEXT NOT NULL,
    entity            TEXT NOT NULL,
    record_pk         TEXT,                    -- primary key value of the bad record, as text
    failure_reason    TEXT NOT NULL,            -- e.g. NULL_PRIMARY_KEY, NEGATIVE_FARE
    raw_record_json   TEXT NOT NULL,            -- full offending record, serialized as JSON
    rejected_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rejected_entity ON error.rejected_records (entity);
CREATE INDEX idx_rejected_batch ON error.rejected_records (batch_id);

-- Pipeline-level exceptions (schema errors, transformation errors, parsing errors)
CREATE TABLE error.pipeline_errors (
    error_id        SERIAL PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    pipeline_name   TEXT NOT NULL,
    error_type      TEXT NOT NULL,             -- SCHEMA_ERROR / TRANSFORMATION_ERROR / PARSING_ERROR
    error_message   TEXT NOT NULL,
    occurred_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- METADATA SCHEMA (cont'd): SCD Type 2 change log for the Driver dimension.
-- The versioned dimension itself lives in the Gold Parquet layer
-- (data/gold/dim_driver_scd2, with effective_start_date / effective_end_date /
-- is_current / version_number columns, consistent with how every other Gold
-- table is stored). This table is the queryable, append-only audit trail of
-- *why* each new version was created -- one row per attribute-level change.
-- ---------------------------------------------------------------------------
CREATE TABLE metadata.scd2_change_log (
    change_id         SERIAL PRIMARY KEY,
    batch_id          TEXT NOT NULL,
    driver_id         INT NOT NULL,
    changed_column    TEXT NOT NULL,
    old_value         TEXT,
    new_value         TEXT,
    old_version       INT,
    new_version       INT,
    effective_date    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_scd2_change_driver ON metadata.scd2_change_log (driver_id);
CREATE INDEX idx_scd2_change_batch ON metadata.scd2_change_log (batch_id);

-- ---------------------------------------------------------------------------
-- Recommendation Engine output (rule-based, no ML). Also mirrored to
-- data/gold/recommendations/ Parquet for the dashboard -- same dual-write
-- pattern as everything else: Postgres for queryability, Parquet for the
-- analytics layer.
-- ---------------------------------------------------------------------------
CREATE TABLE metadata.recommendations (
    recommendation_id  SERIAL PRIMARY KEY,
    batch_id            TEXT NOT NULL,
    rule_name            TEXT NOT NULL,
    scope_type            TEXT NOT NULL,       -- CITY / ZONE / DRIVER / PLATFORM
    scope_value           TEXT NOT NULL,
    metric_value          NUMERIC,
    recommendation        TEXT NOT NULL,
    severity               TEXT NOT NULL,      -- INFO / WARNING / CRITICAL
    generated_at            TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_recommendations_batch ON metadata.recommendations (batch_id);

-- ---------------------------------------------------------------------------
-- SEED: a handful of customers so the platform has a Postgres source to read
-- from immediately. generate_mock_data.py will insert the full realistic set;
-- this seed just guarantees the table isn't empty if run standalone.
-- ---------------------------------------------------------------------------
INSERT INTO source.customers (customer_id, customer_name, phone_number, email, city, signup_date, is_premium, updated_at)
VALUES
    (1, 'Seed Customer One', '9000000001', 'seed1@umip.local', 'Bengaluru', '2024-01-15', TRUE, NOW()),
    (2, 'Seed Customer Two', '9000000002', 'seed2@umip.local', 'Bengaluru', '2024-03-20', FALSE, NOW());
