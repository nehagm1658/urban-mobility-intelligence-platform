"""
pipelines/bronze_ingestion.py
-------------------------------
Reads every raw source (CSV, JSON, XML, Postgres) with PySpark, stamps
ingestion metadata (batch_id, source_name, ingestion_timestamp), and
writes RAW, UNVALIDATED Parquet to data/bronze/<entity>/. No cleansing,
no filtering, no schema enforcement happens here — Bronze is a mirror
of the source, plus metadata.

Every entity's run is logged to metadata.pipeline_runs and audit.audit_log
in Postgres (via pipelines/common.py, shared with every other layer).

IDEMPOTENCY: each run writes to data/bronze/<entity>/load_date=YYYY-MM-DD/
batch_id=<batch_id>/ -- a distinct partition per batch. Re-running the same
batch_id overwrites only that batch's partition, never duplicates it. Silver
reads the union of all batch partitions, same as before.

INCREMENTAL LOADING (--incremental flag; full load is still the default):
  - Postgres source (customers): true watermark incremental using the
    `updated_at` column -- only rows changed since the last successful
    watermark are pulled.
  - Flat file sources (CSV/JSON/XML): these are simulated point-in-time
    file drops with no internal change-timestamp, so there is nothing to
    filter *within* a file. "Incremental" for these means change
    detection at the file level -- an MD5 hash of the file is stored as
    its watermark, and the file is skipped entirely (no bronze write, no
    reprocessing downstream) if unchanged since the last successful run.
    See docs/12_Incremental.md.

RESILIENT PARSING: corrupted JSON rows and malformed XML records no longer
fail the entire file. Bad records are counted, logged individually to
error.pipeline_errors as PARSING_ERROR, and excluded; good records in the
same file still flow through to Bronze.

Run: python3 pipelines/bronze_ingestion.py [batch_id] [--incremental]
"""
import argparse
import hashlib
import os
import sys
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from config import get_spark, PATHS, read_postgres_table, psycopg2_connect
from common import (
    get_logger, log_pipeline_run, log_pipeline_error,
    get_watermark, set_watermark, now,
)

log = get_logger("bronze")


def add_bronze_metadata(df, source_name, batch_id):
    return (
        df.withColumn("_ingestion_timestamp", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
          .withColumn("_source_name", F.lit(source_name))
          .withColumn("_batch_id", F.lit(batch_id))
    )


def write_bronze(df, entity, bronze_root, batch_id):
    """Writes to a load_date/batch_id partition. Re-running the same
    batch_id overwrites only that batch's own partition path, so re-runs
    are idempotent, and a new batch_id never collides with an older one."""
    load_date = datetime.now().strftime("%Y-%m-%d")
    out_path = os.path.join(bronze_root, entity, f"load_date={load_date}", f"batch_id={batch_id}")
    df.write.mode("overwrite").parquet(out_path)
    return out_path


def file_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_csv(spark, entity, filename, raw_root, batch_id, bronze_root, incremental=False):
    start = now()
    pipeline_name = f"bronze_{entity}"
    try:
        path = os.path.join(raw_root, entity, filename)

        if incremental:
            _, last_hash = get_watermark(entity)
            current_hash = file_md5(path)
            if last_hash == current_hash:
                end = now()
                log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, 0, "SUCCESS")
                log.info(f"[bronze:{entity}] SKIPPED (unchanged since last run, incremental mode)")
                print(f"[bronze:{entity}] SKIPPED (no change, incremental mode)")
                return

        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        df = add_bronze_metadata(df, entity, batch_id)
        count = df.count()
        write_bronze(df, entity, bronze_root, batch_id)
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, count, "SUCCESS", insert_count=count)
        if incremental:
            set_watermark(entity, "file_md5", file_md5(path))
        log.info(f"[bronze:{entity}] SUCCESS records={count}")
        print(f"[bronze:{entity}] SUCCESS records={count}")
    except Exception as e:
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, 0, "FAILED", error_message=str(e))
        log_pipeline_error(batch_id, pipeline_name, "PARSING_ERROR", str(e))
        log.error(f"[bronze:{entity}] FAILED: {e}")
        print(f"[bronze:{entity}] FAILED: {e}")


def ingest_json(spark, entity, filename, raw_root, batch_id, bronze_root, incremental=False):
    """PERMISSIVE mode: malformed JSON records land in _corrupt_record
    instead of failing the whole file. Corrupt rows are logged individually
    and excluded; well-formed rows still flow to Bronze."""
    start = now()
    pipeline_name = f"bronze_{entity}"
    try:
        path = os.path.join(raw_root, entity, filename)

        if incremental:
            _, last_hash = get_watermark(entity)
            current_hash = file_md5(path)
            if last_hash == current_hash:
                end = now()
                log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, 0, "SUCCESS")
                log.info(f"[bronze:{entity}] SKIPPED (unchanged since last run, incremental mode)")
                print(f"[bronze:{entity}] SKIPPED (no change, incremental mode)")
                return

        raw_df = (
            spark.read
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(path)
        )
        # Spark disallows filtering directly on _corrupt_record right after
        # a raw read (the query would need to re-parse the source, which
        # Spark blocks since 2.3) -- caching first materializes the parsed
        # result so the corrupt-record filter below is safe.
        raw_df = raw_df.cache()

        has_corrupt_col = "_corrupt_record" in raw_df.columns
        corrupt_count = 0
        if has_corrupt_col:
            corrupt_rows = raw_df.filter(F.col("_corrupt_record").isNotNull())
            corrupt_count = corrupt_rows.count()
            if corrupt_count > 0:
                for r in corrupt_rows.limit(50).collect():
                    log_pipeline_error(batch_id, pipeline_name, "PARSING_ERROR",
                                        f"Corrupted JSON record: {str(r['_corrupt_record'])[:500]}")
            df = raw_df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
        else:
            df = raw_df

        df = add_bronze_metadata(df, entity, batch_id)
        count = df.count()
        write_bronze(df, entity, bronze_root, batch_id)
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, count, "SUCCESS",
                          insert_count=count, reject_count=corrupt_count)
        if incremental:
            set_watermark(entity, "file_md5", file_md5(path))
        log.info(f"[bronze:{entity}] SUCCESS records={count} corrupt_skipped={corrupt_count}")
        print(f"[bronze:{entity}] SUCCESS records={count} corrupt_skipped={corrupt_count}")
    except Exception as e:
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, 0, "FAILED", error_message=str(e))
        log_pipeline_error(batch_id, pipeline_name, "PARSING_ERROR", str(e))
        log.error(f"[bronze:{entity}] FAILED: {e}")
        print(f"[bronze:{entity}] FAILED: {e}")


def ingest_xml(spark, entity, filename, raw_root, batch_id, bronze_root, record_tag="vehicle", incremental=False):
    """
    PySpark has no built-in XML reader without an external Maven package
    (spark-xml), unusable in restricted-network environments. So we parse
    XML with Python's stdlib ElementTree into row dicts, then hand that off
    to Spark as a DataFrame.

    Each <vehicle> element is parsed independently inside its own
    try/except: a single malformed record is counted and logged as
    PARSING_ERROR, not fatal to the whole file. A document that isn't valid
    XML at all still fails the whole ingest (unchanged) -- there's no way
    to salvage a byte stream that doesn't parse.
    """
    start = now()
    pipeline_name = f"bronze_{entity}"
    try:
        path = os.path.join(raw_root, entity, filename)

        if incremental:
            _, last_hash = get_watermark(entity)
            current_hash = file_md5(path)
            if last_hash == current_hash:
                end = now()
                log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, 0, "SUCCESS")
                log.info(f"[bronze:{entity}] SKIPPED (unchanged since last run, incremental mode)")
                print(f"[bronze:{entity}] SKIPPED (no change, incremental mode)")
                return

        tree = ET.parse(path)
        root = tree.getroot()

        rows = []
        malformed_count = 0
        all_keys = set()
        raw_elements = root.findall(f".//{record_tag}")
        for record_el in raw_elements:
            try:
                row = {child.tag: child.text for child in record_el}
                if not row:
                    raise ValueError("empty <%s> element (no child fields)" % record_tag)
                rows.append(row)
                all_keys.update(row.keys())
            except Exception as rec_err:
                malformed_count += 1
                log_pipeline_error(batch_id, pipeline_name, "PARSING_ERROR",
                                    f"Malformed XML <{record_tag}> record: {rec_err}")

        if not rows:
            raise ValueError(f"No valid <{record_tag}> records parsed from {filename}")

        all_keys = sorted(all_keys)
        for row in rows:
            for k in all_keys:
                row.setdefault(k, None)

        schema = StructType([StructField(k, StringType(), True) for k in all_keys])
        df = spark.createDataFrame(rows, schema=schema)
        df = add_bronze_metadata(df, entity, batch_id)
        count = df.count()
        write_bronze(df, entity, bronze_root, batch_id)
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, count, "SUCCESS",
                          insert_count=count, reject_count=malformed_count)
        if incremental:
            set_watermark(entity, "file_md5", file_md5(path))
        log.info(f"[bronze:{entity}] SUCCESS records={count} malformed_skipped={malformed_count}")
        print(f"[bronze:{entity}] SUCCESS records={count} malformed_skipped={malformed_count}")
    except Exception as e:
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, 0, "FAILED", error_message=str(e))
        log_pipeline_error(batch_id, pipeline_name, "PARSING_ERROR", str(e))
        log.error(f"[bronze:{entity}] FAILED: {e}")
        print(f"[bronze:{entity}] FAILED: {e}")


def _read_incremental(spark, table_name, watermark_col, last_value):
    """Postgres source, watermark-filtered: only rows changed since the
    last successful batch. Checks the row count first with a cheap query --
    an empty pandas DataFrame has no dtype information, so Spark can't
    infer a schema from it (spark.createDataFrame would raise
    CANNOT_INFER_EMPTY_SCHEMA). Returns None when there's nothing new."""
    import pandas as pd
    conn = psycopg2_connect()
    try:
        count_df = pd.read_sql(
            f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE {watermark_col} > %(wm)s",
            conn, params={"wm": last_value}
        )
        if count_df["cnt"].iloc[0] == 0:
            return None
        pdf = pd.read_sql(
            f"SELECT * FROM {table_name} WHERE {watermark_col} > %(wm)s",
            conn, params={"wm": last_value}
        )
    finally:
        conn.close()
    return spark.createDataFrame(pdf)


def ingest_postgres(spark, entity, table_name, batch_id, bronze_root, incremental=False):
    start = now()
    pipeline_name = f"bronze_{entity}"
    try:
        watermark_col = "updated_at"
        if incremental:
            _, last_value = get_watermark(entity)
            if last_value:
                df = _read_incremental(spark, table_name, watermark_col, last_value)
                if df is None:
                    end = now()
                    log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, 0, "SUCCESS")
                    log.info(f"[bronze:{entity}] SKIPPED (no new/changed rows since last watermark)")
                    print(f"[bronze:{entity}] SKIPPED (no new/changed rows, incremental mode)")
                    return
            else:
                df = read_postgres_table(spark, table_name)
        else:
            df = read_postgres_table(spark, table_name)

        df = add_bronze_metadata(df, entity, batch_id)
        count = df.count()

        write_bronze(df, entity, bronze_root, batch_id)
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, count, "SUCCESS", insert_count=count)

        if incremental:
            max_updated_at = df.agg(F.max(watermark_col)).collect()[0][0]
            if max_updated_at:
                set_watermark(entity, watermark_col, max_updated_at)

        log.info(f"[bronze:{entity}] SUCCESS records={count}")
        print(f"[bronze:{entity}] SUCCESS records={count}")
    except Exception as e:
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "bronze", entity, start, end, 0, "FAILED", error_message=str(e))
        log_pipeline_error(batch_id, pipeline_name, "PARSING_ERROR", str(e))
        log.error(f"[bronze:{entity}] FAILED: {e}")
        print(f"[bronze:{entity}] FAILED: {e}")


def run(batch_id=None, incremental=False):
    batch_id = batch_id or uuid.uuid4().hex[:8]
    spark = get_spark("BronzeIngestion")
    raw_root = PATHS["raw_root"]
    bronze_root = PATHS["bronze_root"]

    mode_label = "INCREMENTAL" if incremental else "FULL"
    print(f"===== Bronze Ingestion | batch_id={batch_id} | mode={mode_label} =====")
    log.info(f"Bronze Ingestion started | batch_id={batch_id} | mode={mode_label}")

    ingest_csv(spark, "drivers", "drivers.csv", raw_root, batch_id, bronze_root, incremental)
    ingest_csv(spark, "payments", "payments.csv", raw_root, batch_id, bronze_root, incremental)
    ingest_csv(spark, "traffic", "traffic.csv", raw_root, batch_id, bronze_root, incremental)
    ingest_csv(spark, "weather", "weather.csv", raw_root, batch_id, bronze_root, incremental)
    ingest_csv(spark, "driver_shift", "driver_shift.csv", raw_root, batch_id, bronze_root, incremental)
    ingest_json(spark, "trips", "trips.json", raw_root, batch_id, bronze_root, incremental)
    ingest_xml(spark, "vehicles", "vehicles.xml", raw_root, batch_id, bronze_root, record_tag="vehicle", incremental=incremental)
    ingest_postgres(spark, "customers", "source.customers", batch_id, bronze_root, incremental)

    print(f"===== Bronze Ingestion complete | batch_id={batch_id} =====")
    log.info(f"Bronze Ingestion complete | batch_id={batch_id}")
    spark.stop()
    return batch_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze ingestion")
    parser.add_argument("batch_id", nargs="?", default=None)
    parser.add_argument("--incremental", action="store_true",
                         help="Skip unchanged file sources; watermark-filter Postgres sources")
    args = parser.parse_args()
    run(args.batch_id, args.incremental)
