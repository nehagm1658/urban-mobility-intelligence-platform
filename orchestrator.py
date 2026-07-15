"""
orchestrator.py
-----------------
Main execution engine for the Urban Mobility Intelligence Platform PoC.
Runs Bronze -> Silver -> Gold -> SCD2 -> Recommendation Engine -> Dashboard
sequentially under one shared batch_id, so every pipeline_runs / audit_log
row across all layers can be tied back to a single, traceable execution.
This is the same sequence the Airflow DAG (dags/umip_dag.py) runs, just
without Airflow's scheduling/retry machinery around it.

If any stage fails, the orchestrator stops (fail-fast) and prints a
summary of what succeeded/failed. Failure detail lives in
metadata.pipeline_runs and error.pipeline_errors in Postgres.

Usage:
    python3 orchestrator.py                    # full run, all stages
    python3 orchestrator.py --stage bronze      # run a single stage only
    python3 orchestrator.py --stage silver
    python3 orchestrator.py --stage gold
    python3 orchestrator.py --stage scd2
    python3 orchestrator.py --stage recommendations
    python3 orchestrator.py --stage dashboard
    python3 orchestrator.py --incremental       # bronze/silver in incremental mode
    python3 orchestrator.py --batch-id abc123   # reuse a batch_id (e.g. retry)
"""
import argparse
import sys
import uuid
from datetime import datetime

sys.path.insert(0, "pipelines")

import bronze_ingestion
import silver_cleansing
import gold_transformations
import scd_type2
import recommendation_engine
import dashboard
from config import psycopg2_connect

ALL_STAGES = ["bronze", "silver", "gold", "scd2", "recommendations", "dashboard"]


def print_batch_summary(batch_id):
    conn = psycopg2_connect()
    cur = conn.cursor()
    cur.execute(
        """SELECT layer, entity, record_count, reject_count, status
           FROM metadata.pipeline_runs
           WHERE batch_id = %s
           ORDER BY run_id""",
        (batch_id,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"\n{'='*70}")
    print(f"BATCH SUMMARY  |  batch_id={batch_id}")
    print(f"{'='*70}")
    print(f"{'LAYER':<10}{'ENTITY':<24}{'RECORDS':<10}{'REJECTED':<10}{'STATUS':<10}")
    print("-" * 70)
    any_failed = False
    for layer, entity, record_count, reject_count, status in rows:
        if status != "SUCCESS":
            any_failed = True
        print(f"{layer:<10}{entity:<24}{record_count:<10}{reject_count or 0:<10}{status:<10}")
    print("-" * 70)
    print("OVERALL STATUS:", "FAILED (see above)" if any_failed else "SUCCESS")
    print(f"{'='*70}\n")
    return not any_failed


def run_pipeline(stage, batch_id, incremental):
    print(f"\n>>> Running stage: {stage.upper()} (batch_id={batch_id})\n")
    if stage == "bronze":
        bronze_ingestion.run(batch_id, incremental)
    elif stage == "silver":
        silver_cleansing.run(batch_id, incremental)
    elif stage == "gold":
        gold_transformations.run(batch_id)
    elif stage == "scd2":
        scd_type2.run(batch_id)
    elif stage == "recommendations":
        recommendation_engine.run(batch_id)
    elif stage == "dashboard":
        dashboard.run()
    else:
        raise ValueError(f"Unknown stage: {stage}")


def main():
    parser = argparse.ArgumentParser(description="Urban Mobility Intelligence Platform orchestrator")
    parser.add_argument("--stage", choices=ALL_STAGES + ["all"], default="all",
                         help="Which stage to run (default: all = every stage in order)")
    parser.add_argument("--batch-id", default=None, help="Reuse an existing batch_id (optional)")
    parser.add_argument("--incremental", action="store_true",
                         help="Run bronze/silver in incremental mode (watermark-based). Gold/SCD2/"
                              "recommendations/dashboard are unaffected -- they always read the "
                              "current full Silver/Gold state.")
    args = parser.parse_args()

    batch_id = args.batch_id or uuid.uuid4().hex[:8]
    start_time = datetime.now()

    print("Urban Mobility Intelligence Platform — Orchestrator")
    print(f"Batch ID: {batch_id}")
    print(f"Mode: {'INCREMENTAL' if args.incremental else 'FULL'}")
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    stages = ALL_STAGES if args.stage == "all" else [args.stage]

    for stage in stages:
        run_pipeline(stage, batch_id, args.incremental)

    success = print_batch_summary(batch_id)

    duration = (datetime.now() - start_time).total_seconds()
    print(f"Total orchestrator duration: {duration:.2f}s")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
