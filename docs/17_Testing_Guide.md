# 17. Testing Guide

This project was tested against a real local PostgreSQL + PySpark
environment during development -- every claim in this doc is from an
actual command that was run, not a description of what "should" happen.

## Environment used for testing

- PostgreSQL 16 (local install, not Docker -- no Docker daemon was
  available in the development sandbox; see "Known gaps" below)
- PySpark 3.5.1, Python 3.12, OpenJDK 21
- Package versions exactly as pinned in `requirements.txt`

## What was verified, and how

### 1. Baseline (before any enhancement) ran clean end to end
`docker compose`-equivalent manual setup, `generate_mock_data.py`, then
`orchestrator.py` -- all original bronze/silver/gold stages passed before
any changes were made, establishing a known-good starting point.

### 2. Silver's vectorized validation rewrite produces identical results
to the original row-by-row implementation, at small scale (drivers
242/12 valid/rejected, trips 3938/67, payments 3532/64 -- matched the
pre-rewrite baseline exactly) and was then flattened from nested
`when()/otherwise()` chains to `coalesce()` over independent columns
after a Spark codegen-size warning appeared on the wide `trips` entity;
re-tested and the warning was gone with identical results.

### 3. Full enterprise-scale run (1,000 drivers / 10,000 customers /
110,275 raw trips / 98,247 payments / 500 vehicles)
```
Bronze:  34s   -- 110,275 trips, 55 corrupt JSON lines caught, 5 malformed XML records caught
Silver:  73s   -- trips 107,729 valid / 2,546 rejected; payments 96,200 valid / 2,047 rejected
Gold:    39s   -- all 8 dims/facts/marts built
SCD2:    ~5s   -- 974 current driver versions
Recommendations: <5s -- 47 recommendations generated
Dashboard: <5s -- HTML + CSVs generated (PNG skipped, no Chrome available)
```
Total: under 3 minutes for the full pipeline at this scale.

### 4. SCD Type 2 versioning, specifically
- Fresh load: 974 drivers, all `version=1, is_current=true`
- Simulated a real attribute change (driver 1: `status SUSPENDED ->
  INACTIVE`) directly in Silver output, re-ran `scd_type2.py`: exactly
  1 driver changed, old version correctly expired
  (`is_current=false`, `effective_end_date` set), new version 2 correctly
  inserted, and `metadata.scd2_change_log` recorded exactly the one
  changed column (not `city`, which hadn't changed) with correct old/new
  values.
- Re-ran again with no further changes: `new=0 changed=0` -- confirmed
  idempotent.

### 5. Idempotent Bronze re-runs
Ran `orchestrator.py --stage bronze` twice with the identical `batch_id`
-- confirmed exactly one partition directory per entity both times (no
duplication).

### 6. Incremental loading, full sequence
```
Run 1 (--incremental, no prior watermark): full load, watermark set for every entity
Run 2 (--incremental, nothing changed):     every one of 8 entities SKIPPED
Run 3 (--incremental, +1 new customer row):  only customers processed,
                                              exactly 1 record picked up
```

### 7. Real bugs found and fixed during this testing (not hypothetical)
- **Spark's `_corrupt_record` filter restriction**: filtering on
  `_corrupt_record` immediately after a raw JSON read is disallowed by
  Spark (would require re-parsing); fixed by caching the DataFrame first.
- **Self-overwrite hazard**: `merge_upsert()` and `scd_type2.py`'s writer
  both read an existing Parquet path and needed to write back to that
  same path -- doing so directly deletes files out from under Spark's
  still-running read task. Fixed by writing to a temp path and atomically
  swapping it into place in both places.
- **Empty-DataFrame schema inference**: an incremental Postgres read that
  returns zero new rows can't have its Spark schema inferred from an
  empty pandas DataFrame. Fixed by checking the row count first with a
  cheap `COUNT(*)` query and skipping the expensive path entirely when
  there's nothing new.
- **`ON CONFLICT` against a dropped constraint**: the seed-data insert for
  `source.customers` used `ON CONFLICT (customer_id)`, which broke once
  the primary key was deliberately relaxed (needed to allow injected
  duplicate customer IDs). Fixed by removing the clause.

### 8. The Airflow DAG (`dags/umip_dag.py`) was validated with a real
installed Airflow (`DagBag` import) -- confirmed zero import errors and
the exact intended linear task dependency chain. **Not** verified: an
actual live `docker compose up airflow-webserver airflow-scheduler` run,
since no Docker daemon was available in the development environment --
see `docs/13_Airflow.md`.

## Known gaps -- run these yourself before considering this "done"

- **Docker Compose was not executed** (`docker compose up`) -- no Docker
  daemon was available in the environment this was built in. Postgres,
  PySpark, and every pipeline module were tested against a locally
  installed Postgres + PySpark instead, which exercises the same code
  paths but not the container networking/volumes/healthchecks
  `docker-compose.yml` defines. Run `docker compose up -d` yourself and
  confirm the pipeline still runs the same way through the containers.
- **Airflow's webserver/scheduler were not run live** -- see above.
- **Driver-shift overlap and duplicate-vehicle-assignment** are present in
  the generated data (by design) but have no validation rule checking for
  them -- see `README.md`'s "Known simplifications." A genuine next
  enhancement would be a window-based overlap check across `driver_shift`
  rows for the same `driver_id`.
- **Load testing beyond ~110k trips** wasn't attempted -- the numbers
  above are the actual tested ceiling, not a claim about arbitrary scale.

## Suggested unit/integration tests to add (not yet implemented)

- `pytest` cases for `validate_and_split()` covering each rejection reason
  with a small hand-built DataFrame (fast, no Spark cluster needed beyond
  local mode)
- A test that asserts `merge_upsert()` correctly replaces a changed
  primary key's row and leaves unrelated rows untouched
- A test that asserts `scd_type2.py` never creates more than one new
  version per driver per run, even if called twice with the same input
