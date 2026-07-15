"""
dags/umip_dag.py
-------------------
Airflow DAG for the Urban Mobility Intelligence Platform. Runs the exact
same stage sequence as orchestrator.py (Bronze -> Silver -> Gold -> SCD2
-> Recommendation Engine -> Dashboard), one task per stage, so Airflow
gives you scheduling, retries, and failure alerting on top of the same
pipeline code -- it does NOT reimplement the pipeline logic.

Each task calls `python3 orchestrator.py --stage <name> --batch-id
{{ run_id }}` via BashOperator, using Airflow's run_id as the batch_id so
every task in one DAG run shares the same batch_id in
metadata.pipeline_runs -- the same "one batch_id ties every layer
together" pattern the plain orchestrator uses for a manual run.

Requires: the Airflow worker image needs this project's requirements.txt
installed (pyspark, pandas, psycopg2-binary, etc.) plus a JRE for Spark.
See Dockerfile.airflow, which extends the official Airflow image with
exactly that. docker-compose.yml builds Airflow from Dockerfile.airflow.

To run: docker compose up airflow-init && docker compose up airflow-webserver airflow-scheduler
Then open http://localhost:8080 (default Airflow login: airflow/airflow)
and trigger the `urban_mobility_intelligence_platform` DAG.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# Project root inside the Airflow container -- see docker-compose.yml,
# which mounts the whole repo to /opt/airflow/project.
PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=2),
}


def alert_on_failure(context):
    """Minimal failure notification: logs a clear message to the task log
    (visible in the Airflow UI). Swap this for a Slack/email hook in a
    real deployment -- kept simple here since there's no real notification
    channel to send to in a PoC."""
    ti = context["task_instance"]
    print(f"[ALERT] Task FAILED: dag={ti.dag_id} task={ti.task_id} run_id={ti.run_id}. "
          f"Check metadata.pipeline_runs and error.pipeline_errors for detail.")


with DAG(
    dag_id="urban_mobility_intelligence_platform",
    description="Bronze -> Silver -> Gold -> SCD2 -> Recommendations -> Dashboard",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["umip", "data-engineering"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"cd {PROJECT_DIR} && python3 orchestrator.py --stage bronze --batch-id {{{{ run_id }}}}",
        on_failure_callback=alert_on_failure,
    )

    silver = BashOperator(
        task_id="silver_cleansing",
        bash_command=f"cd {PROJECT_DIR} && python3 orchestrator.py --stage silver --batch-id {{{{ run_id }}}}",
        on_failure_callback=alert_on_failure,
    )

    gold = BashOperator(
        task_id="gold_transformations",
        bash_command=f"cd {PROJECT_DIR} && python3 orchestrator.py --stage gold --batch-id {{{{ run_id }}}}",
        on_failure_callback=alert_on_failure,
    )

    scd2 = BashOperator(
        task_id="scd_type2_driver_dimension",
        bash_command=f"cd {PROJECT_DIR} && python3 orchestrator.py --stage scd2 --batch-id {{{{ run_id }}}}",
        on_failure_callback=alert_on_failure,
    )

    recommendations = BashOperator(
        task_id="recommendation_engine",
        bash_command=f"cd {PROJECT_DIR} && python3 orchestrator.py --stage recommendations --batch-id {{{{ run_id }}}}",
        on_failure_callback=alert_on_failure,
    )

    dashboard_refresh = BashOperator(
        task_id="dashboard_refresh",
        bash_command=f"cd {PROJECT_DIR} && python3 orchestrator.py --stage dashboard --batch-id {{{{ run_id }}}}",
        on_failure_callback=alert_on_failure,
        # Dashboard is a "nice to have" refresh step -- if upstream failed
        # we still want the DAG run marked failed overall (default trigger
        # rule via the linear >> chain below already achieves that: this
        # task simply won't run if gold/scd2/recommendations fail).
    )

    # Bronze -> Silver -> Gold -> SCD2 -> Recommendations -> Dashboard.
    # SCD2 and the recommendation engine both depend on Gold's fact/dim
    # tables, and the dashboard depends on the recommendation engine's
    # output, so this stays a single linear chain (same order
    # orchestrator.py runs by default).
    bronze >> silver >> gold >> scd2 >> recommendations >> dashboard_refresh
