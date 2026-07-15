# Urban Mobility Intelligence Platform — PoC

A working, end-to-end Data Engineering proof of concept simulating how a
ride-sharing platform (Uber/Ola-style) ingests, validates, and analyzes
operational data. Multi-format ingestion → Bronze → Silver (validated +
cleansed, SCD Type 1 + Type 2) → Gold (dimensions, facts, KPI marts,
rule-based recommendations) → an executive Plotly dashboard, orchestrated
either by a single Python entrypoint or an Airflow DAG, backed by
PostgreSQL for metadata/audit/error tracking.

Every command in this README was actually executed against real data
(including the full ~100k-trip / ~10k-customer enterprise-scale run)
before being written here -- see `docs/17_Testing_Guide.md` for the exact
verification steps and results.

## Architecture

```
CSV(drivers,payments,traffic,weather,driver_shift)   JSON Lines(trips)   XML(vehicles)   PostgreSQL(customers)
                                    │
                                    ▼
                      pipelines/bronze_ingestion.py
     (raw + _ingestion_timestamp/_source_name/_batch_id, per-batch partitions,
      resilient JSON/XML parsing, incremental via watermark/file-hash)
                                    │
                                    ▼
             data/bronze/<entity>/load_date=YYYY-MM-DD/batch_id=<id>/
                                    │
                                    ▼
                      pipelines/silver_cleansing.py
   (vectorized Spark validation → error.rejected_records, cleansing,
    city_tier lookup, SCD Type 1 for drivers, incremental upsert)
                                    │
                                    ▼
                            data/silver/<entity>/
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                                ▼
     pipelines/gold_transformations.py     pipelines/scd_type2.py
  (dims, fact, KPI marts, joins/agg/window)   (full driver history)
                    │                                │
                    ▼                                ▼
     data/gold/{dim_driver, dim_customer,     data/gold/dim_driver_scd2/
     dim_vehicle, fact_trip, revenue_summary_mart,
     ops_kpi_summary, cancellation_analytics,
     demand_supply_analytics}
                    │
                    ▼
        pipelines/recommendation_engine.py
        (rule-based ops recommendations → metadata.recommendations)
                    │
                    ▼
              pipelines/dashboard.py
     (Plotly HTML dashboard + Tableau-compatible CSVs)
```

Every stage of every entity logs to Postgres: `metadata.pipeline_runs`
(record/insert/update/reject counts, duration, status, host, pipeline
version) and `audit.audit_log`. Bad records go to `error.rejected_records`
with the specific failure reason; pipeline-level exceptions go to
`error.pipeline_errors`. SCD2 attribute-level changes go to
`metadata.scd2_change_log`. Recommendations go to `metadata.recommendations`.

## Prerequisites

- Docker + Docker Compose (for PostgreSQL + pgAdmin + Airflow)
- Python 3.10+
- Java 17+ (required by PySpark)
- `pip install -r requirements.txt --break-system-packages`

## Setup (from scratch)

```bash
# 1. Start Postgres + pgAdmin
docker compose up -d postgres pgadmin

# 2. Wait for Postgres to be healthy, then apply the DDL
#    (docker-compose already auto-runs scripts/db_setup.sql on first init via
#    the /docker-entrypoint-initdb.d mount; run it manually only if you need
#    to reset schemas on an already-initialized volume)
docker exec -i umip_postgres psql -U umip_admin -d umip_platform < scripts/db_setup.sql

# 3. Generate realistic mock data (writes CSV/JSON Lines/XML to data/raw/,
#    loads customers into Postgres source.customers). Defaults to
#    enterprise-scale volumes (1,000 drivers / 10,000 customers / ~110,000
#    trips / ~98,000 payments / 500 vehicles / 365 weather days / 500
#    traffic records / 15,000 driver-shift records over a 365-day window).
#    Override any volume with an env var for a faster local run, e.g.
#    UMIP_N_TRIPS=5000.
python3 scripts/generate_mock_data.py

# 4. Run the full pipeline: Bronze -> Silver -> Gold -> SCD2 -> Recommendations -> Dashboard
python3 orchestrator.py

# Or run a single stage:
python3 orchestrator.py --stage bronze
python3 orchestrator.py --stage silver
python3 orchestrator.py --stage gold
python3 orchestrator.py --stage scd2
python3 orchestrator.py --stage recommendations
python3 orchestrator.py --stage dashboard

# Or run bronze/silver incrementally (watermark-based; see docs/12_Incremental.md)
python3 orchestrator.py --incremental
```

## Running via Airflow instead

```bash
docker compose up airflow-init            # one-time: creates the Airflow metadata schema + admin user
docker compose up -d postgres pgadmin airflow-webserver airflow-scheduler
```
Open `http://localhost:8080` (login `airflow` / `airflow`), unpause the
`urban_mobility_intelligence_platform` DAG, and trigger it. The DAG runs
the exact same stage sequence as `orchestrator.py`
(bronze → silver → gold → scd2 → recommendations → dashboard), one task
per stage, with retries and failure logging. See `docs/13_Airflow.md`.

## Dashboard

After `orchestrator.py --stage dashboard` (or a full run), open
`data/dashboard/dashboard.html` in any browser -- no server needed.
Tableau/Excel/Power BI-compatible CSVs for the same marts are written
alongside it. See `docs/14_Dashboard.md`.

## Verification steps

**1. Check the orchestrator's own summary** — it prints a batch-level table at
the end of every run (records/rejects/status per layer per entity) and exits
non-zero if anything failed.

**2. Check Bronze output exists and is partitioned by date + batch:**
```bash
find data/bronze -maxdepth 3 -type d
```

**3. Check Silver rejected the right things for the right reasons:**
```sql
SELECT entity, failure_reason, COUNT(*)
FROM error.rejected_records
GROUP BY entity, failure_reason
ORDER BY entity;
```
Expect to see (at enterprise-scale volumes): `NULL_PRIMARY_KEY`,
`DUPLICATE_PRIMARY_KEY`, `RATING_OUT_OF_RANGE` (drivers),
`NEGATIVE_FARE`, `NULL_REQUIRED_FIELD:driver_id` /
`NULL_REQUIRED_FIELD:vehicle_id`, `MISMATCHED_TRIP_TIMESTAMPS` (trips),
`INVALID_PAYMENT_AMOUNT` (payments), `DUPLICATE_PRIMARY_KEY` (customers,
from the deliberately non-deduplicated `source.customers` table).

**4. Check SCD Type 2 history is versioned correctly:**
```python
import pandas as pd
h = pd.read_parquet("data/gold/dim_driver_scd2")
h[h["driver_id"] == h["driver_id"].iloc[0]].sort_values("version_number")
```
See `docs/11_SCD.md` for a worked before/after example.

**5. Check recommendations were generated:**
```sql
SELECT severity, rule_name, COUNT(*) FROM metadata.recommendations GROUP BY 1,2 ORDER BY 1;
```

**6. Check every run is traceable end-to-end by batch_id:**
```sql
SELECT layer, entity, record_count, reject_count, status
FROM metadata.pipeline_runs
WHERE batch_id = '<batch_id printed by orchestrator.py>'
ORDER BY run_id;
```

**7. pgAdmin:** open `http://localhost:5050`, log in with
`admin@umip.local` / `admin_password`, register a server pointing at host
`postgres`, port `5432`, db `umip_platform`, user `umip_admin` / `umip_password`.

## Project structure

```
docker-compose.yml            # Postgres + pgAdmin + Airflow (webserver/scheduler/init)
Dockerfile.airflow            # Airflow image + JRE + this project's requirements.txt
requirements.txt              # All Python dependencies, pinned to tested versions
dags/
  umip_dag.py                  # Airflow DAG mirroring orchestrator.py's stage sequence
scripts/
  db_setup.sql                  # DDL: source, metadata, audit, error schemas
  airflow_db_init.sql            # Creates the separate airflow_meta database
  generate_mock_data.py           # Enterprise-scale Bengaluru mock data + edge cases
pipelines/
  config.py                      # Shared paths, Postgres config, Spark session helper
  common.py                      # Shared logging, batch DB writes, watermarks, retry
  bronze_ingestion.py             # Multi-format raw ingestion, incremental, resilient parsing
  silver_cleansing.py             # Vectorized validation, rejects, SCD1, incremental upsert
  gold_transformations.py         # Dims, fact, KPI marts (joins/agg/window fns)
  scd_type2.py                    # Full driver dimension history
  recommendation_engine.py        # Rule-based operational recommendations
  dashboard.py                    # Plotly dashboard + Tableau-compatible CSV export
orchestrator.py                  # Runs every stage in order, prints batch summary
data/
  raw/                            # Generated source files (CSV/JSON Lines/XML)
  bronze/ silver/ gold/            # Medallion layers (Parquet)
  dashboard/                       # dashboard.html + CSVs (+ PNGs if kaleido/Chrome available)
logs/                              # Rotating log files, one per pipeline layer
docs/                               # 01-18: architecture, data model, SCD, incremental,
                                     # Airflow, dashboard, lineage, testing, interview prep
```

## Known simplifications (documented, not hidden)

- **Postgres reads use psycopg2 + pandas, not Spark JDBC** — this
  environment has no Maven access to fetch `org.postgresql:postgresql`. If
  your environment can reach Maven Central, swap `read_postgres_table()` in
  `config.py` for a proper `spark.read.jdbc(...)` call for true distributed
  reads at scale.
- **XML parsing uses Python's `ElementTree`**, not the `spark-xml` Maven
  package, for the same network-access reason above.
- **Incremental loading for flat files (CSV/JSON/XML) is file-level change
  detection (MD5 hash), not row-level CDC** — flat files have no internal
  change-timestamp to filter on. Only the Postgres source (`customers`)
  gets true row-level watermark incremental loading. See
  `docs/12_Incremental.md`.
- **Driver-shift overlap and "driver assigned two vehicles" are present in
  the generated data but not rejected by a validation rule** — a driver
  legitimately having a backup vehicle isn't inherently bad data, and
  detecting genuine shift-time overlap needs a cross-record temporal-window
  check, which is called out in `docs/17_Testing_Guide.md` as a good next
  enhancement rather than silently skipped.
- **Great Expectations** is not wired in as its own suite — validation logic
  is native vectorized PySpark. Converting `validate_and_split()`'s rules
  into a GE Expectation Suite per entity is a natural next step.
- **PNG dashboard export needs `kaleido` + a local Chrome/Chromium** — if
  unavailable, `dashboard.py` fails soft and still produces the HTML
  dashboard and Tableau-compatible CSVs.
