"""
pipelines/common.py
---------------------
Shared utilities used by every pipeline stage (bronze/silver/gold/scd2/
recommendation engine). Extracted from what used to be duplicated
log_pipeline_run()/log_pipeline_error() copies in each pipeline module.

Provides:
  - Rotating file + console logging, one log file per pipeline layer
  - metadata.pipeline_runs / audit.audit_log / error.pipeline_errors writers
  - Batched (executemany-based) rejected-record writer, safe at 100k+ rows
  - Watermark read/write helpers for incremental loading
  - A small retry decorator for transient DB/IO errors

Nothing in here changes what any pipeline logs or where -- it is a pure
refactor of previously-duplicated code into one place, plus new helpers
(batch reject insert, watermarks, retry) needed for the incremental /
SCD2 / recommendation-engine enhancements.
"""
import functools
import json as pyjson
import logging
import os
import platform
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

from config import PATHS, psycopg2_connect

_LOGGERS = {}


def get_logger(name):
    """One rotating-file logger per pipeline layer (bronze/silver/gold/...),
    5MB per file, 5 backups kept, plus console output at INFO level."""
    if name in _LOGGERS:
        return _LOGGERS[name]

    os.makedirs(PATHS["logs_root"], exist_ok=True)
    logger = logging.getLogger(f"umip.{name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        )
        file_handler = RotatingFileHandler(
            os.path.join(PATHS["logs_root"], f"{name}.log"),
            maxBytes=5 * 1024 * 1024, backupCount=5,
        )
        file_handler.setFormatter(fmt)
        file_handler.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(fmt)
        console_handler.setLevel(logging.INFO)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    _LOGGERS[name] = logger
    return logger


def retry(max_attempts=3, delay_seconds=2, exceptions=(Exception,)):
    """Retry decorator for transient DB/network errors (connection resets,
    momentary Postgres unavailability). Not used for business-logic
    failures -- those should fail fast and be logged, not retried blindly."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_attempts:
                        time.sleep(delay_seconds * attempt)
            raise last_exc
        return wrapper
    return decorator


@retry(max_attempts=3, delay_seconds=1)
def log_pipeline_run(batch_id, pipeline_name, layer, entity, start, end,
                      record_count, status, insert_count=0, update_count=0,
                      reject_count=0, error_message=""):
    """Writes one row to metadata.pipeline_runs and one to audit.audit_log.
    Same two-table write every pipeline stage needs; centralized here so
    bronze/silver/gold/scd2/recommendation_engine all log identically."""
    conn = psycopg2_connect()
    conn.autocommit = True
    cur = conn.cursor()
    duration = (end - start).total_seconds()
    try:
        cur.execute(
            """INSERT INTO metadata.pipeline_runs
               (batch_id, pipeline_name, layer, entity, execution_start, execution_end,
                duration_seconds, record_count, insert_count, update_count,
                reject_count, status, error_message, execution_host, pipeline_version)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (batch_id, pipeline_name, layer, entity, start, end, duration,
             record_count, insert_count, update_count, reject_count, status,
             error_message, platform.node(), "1.1.0")
        )
        cur.execute(
            """INSERT INTO audit.audit_log
               (batch_id, pipeline_name, layer, processed_records, rejected_records,
                inserted_records, updated_records, execution_duration_sec, pipeline_status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (batch_id, pipeline_name, layer, record_count, reject_count,
             insert_count, update_count, duration, status)
        )
    finally:
        cur.close()
        conn.close()


@retry(max_attempts=3, delay_seconds=1)
def log_pipeline_error(batch_id, pipeline_name, error_type, error_message):
    conn = psycopg2_connect()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO error.pipeline_errors (batch_id, pipeline_name, error_type, error_message)
               VALUES (%s,%s,%s,%s)""",
            (batch_id, pipeline_name, error_type, error_message)
        )
    finally:
        cur.close()
        conn.close()


@retry(max_attempts=3, delay_seconds=1)
def reject_records_batch(batch_id, entity, rows, pk_column, reason):
    """Batched version of the old per-row INSERT loop. Uses psycopg2.extras
    .execute_values so rejecting tens of thousands of rows (e.g. 100k trips
    with a few % failure rate) is one round trip instead of one INSERT per
    row -- the difference between seconds and minutes at enterprise volume."""
    if not rows:
        return
    from psycopg2.extras import execute_values
    conn = psycopg2_connect()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        values = [
            (batch_id, entity,
             str(r.get(pk_column)) if r.get(pk_column) is not None else None,
             reason, pyjson.dumps(r, default=str))
            for r in rows
        ]
        execute_values(
            cur,
            """INSERT INTO error.rejected_records
               (batch_id, entity, record_pk, failure_reason, raw_record_json)
               VALUES %s""",
            values,
            page_size=1000,
        )
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Watermarks (incremental loading)
# ---------------------------------------------------------------------------
@retry(max_attempts=3, delay_seconds=1)
def get_watermark(entity):
    """Returns (watermark_column, watermark_value) or (None, None) if the
    entity has never been loaded incrementally before (first run = full load)."""
    conn = psycopg2_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT watermark_column, watermark_value FROM metadata.watermarks WHERE entity = %s",
            (entity,)
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)
    finally:
        cur.close()
        conn.close()


@retry(max_attempts=3, delay_seconds=1)
def set_watermark(entity, watermark_column, watermark_value):
    conn = psycopg2_connect()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO metadata.watermarks (entity, watermark_column, watermark_value, updated_at)
               VALUES (%s,%s,%s, NOW())
               ON CONFLICT (entity) DO UPDATE
               SET watermark_column = EXCLUDED.watermark_column,
                   watermark_value = EXCLUDED.watermark_value,
                   updated_at = NOW()""",
            (entity, watermark_column, str(watermark_value))
        )
    finally:
        cur.close()
        conn.close()


@retry(max_attempts=3, delay_seconds=1)
def batch_already_processed(entity, batch_id):
    """Idempotency guard: has this exact (entity, batch_id) already completed
    SUCCESS for this layer? Lets orchestrator re-runs with --batch-id be
    safely re-triggered (e.g. after a partial Airflow retry) without
    double-counting records."""
    conn = psycopg2_connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT 1 FROM metadata.pipeline_runs
               WHERE batch_id = %s AND entity = %s AND status = 'SUCCESS' LIMIT 1""",
            (batch_id, entity)
        )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def now():
    return datetime.now()
