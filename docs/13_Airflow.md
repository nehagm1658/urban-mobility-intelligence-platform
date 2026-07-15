# 13. Airflow

**DAG:** `dags/umip_dag.py`, id `urban_mobility_intelligence_platform`

## What it does

Exactly mirrors `orchestrator.py`'s stage sequence -- one `BashOperator`
task per stage, each running `python3 orchestrator.py --stage <name>
--batch-id {{ run_id }}` inside the Airflow container. Airflow's own
`run_id` is used as the pipeline's `batch_id`, so every task in one DAG
run shares the same `batch_id` in `metadata.pipeline_runs`, exactly like
a manual `orchestrator.py` run does.

```
bronze_ingestion >> silver_cleansing >> gold_transformations
    >> scd_type2_driver_dimension >> recommendation_engine >> dashboard_refresh
```

This DAG does **not** reimplement any pipeline logic -- it's a thin
scheduling layer over the same Python modules the plain orchestrator
calls. That was a deliberate choice: one implementation of the pipeline,
two ways to run it.

## Configuration

- `retries: 2`, `retry_delay: 3 minutes`, `execution_timeout: 2 hours`
  per task
- `schedule="@daily"`, `catchup=False`, `max_active_runs=1`
- `on_failure_callback` logs a clear failure message per task (swap for a
  real Slack/email hook in a production deployment -- there's no real
  notification channel to send to in a PoC)

## Running it

```bash
docker compose up airflow-init            # one-time: migrates the Airflow
                                            # metadata DB + creates admin user (airflow/airflow)
docker compose up -d postgres pgadmin airflow-webserver airflow-scheduler
```
Open `http://localhost:8080`, log in, unpause
`urban_mobility_intelligence_platform`, trigger it.

`Dockerfile.airflow` extends the official Airflow image with a JRE
(PySpark needs a JVM) and this project's `requirements.txt`, and
`docker-compose.yml` mounts the whole project into the Airflow containers
at `/opt/airflow/project` so the BashOperator tasks can actually reach
`orchestrator.py` and `pipelines/`. Airflow's own metadata lives in a
separate `airflow_meta` Postgres database (created by
`scripts/airflow_db_init.sql`), kept isolated from `umip_platform` (the
pipeline's actual data) so the two are never confused.

## Verification performed

The DAG file was validated with a real installed Airflow (`DagBag`
import), not just eyeballed:
```
IMPORT ERRORS: {}
DAG IDS: ['urban_mobility_intelligence_platform']
Tasks: ['bronze_ingestion', 'silver_cleansing', 'gold_transformations',
         'scd_type2_driver_dimension', 'recommendation_engine', 'dashboard_refresh']
bronze_ingestion -> downstream: ['silver_cleansing']
silver_cleansing -> downstream: ['gold_transformations']
gold_transformations -> downstream: ['scd_type2_driver_dimension']
scd_type2_driver_dimension -> downstream: ['recommendation_engine']
recommendation_engine -> downstream: ['dashboard_refresh']
dashboard_refresh -> downstream: []
```
This confirms the DAG parses cleanly and the task dependency graph is
exactly the intended linear chain. **Not verified:** an actual live
webserver/scheduler run inside Docker -- this development environment
has no Docker daemon available, so the full `docker compose up
airflow-webserver airflow-scheduler` flow (task execution, retries firing
for real, the UI) could not be exercised end-to-end here. The DAG logic
and dependency structure are confirmed correct; running it live in your
own Docker environment is the remaining verification step.
