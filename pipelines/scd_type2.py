"""
pipelines/scd_type2.py
------------------------
Full SCD Type 2 history for the Driver dimension, run after Silver (it
reads data/silver/drivers, the same SCD1-cleansed snapshot gold_transformations
also reads -- SCD1 and SCD2 are not alternatives here, they're two different
consumers of the same clean data: SCD1 (silver_cleansing.py) always shows the
CURRENT driver attributes; SCD2 (this module) additionally keeps every prior
version around with effective dates, so you can answer "what was this
driver's city/status/rating as of last month" -- something SCD1 can't do.

Tracked attributes: city, status, rating_bucket (rating rounded to the
nearest 0.5, since raw rating drifts trip-to-trip and we only want to
version *meaningful* changes, not noise).

Output: data/gold/dim_driver_scd2/ (Parquet, same storage pattern as every
other Gold table) with:
    driver_id, driver_name, city, status, rating_bucket,
    effective_start_date, effective_end_date, is_current, version_number

Change log: every attribute-level change that produces a new version is
also written to metadata.scd2_change_log in Postgres (old_value/new_value
per changed column), giving a queryable audit trail of *why* a new version
exists, not just that one does.

Run: python3 pipelines/scd_type2.py [batch_id]
"""
import os
import sys
import uuid
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

from config import get_spark, PATHS, psycopg2_connect
from common import get_logger, log_pipeline_run, now

log = get_logger("scd2")

TRACKED_COLUMNS = ["city", "status", "rating_bucket"]
SCD2_TABLE_NAME = "dim_driver_scd2"


def _write_change_log(batch_id, changes):
    """changes: list of dicts with driver_id, changed_column, old_value,
    new_value, old_version, new_version."""
    if not changes:
        return
    from psycopg2.extras import execute_values
    conn = psycopg2_connect()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        values = [
            (batch_id, c["driver_id"], c["changed_column"], c["old_value"],
             c["new_value"], c["old_version"], c["new_version"])
            for c in changes
        ]
        execute_values(
            cur,
            """INSERT INTO metadata.scd2_change_log
               (batch_id, driver_id, changed_column, old_value, new_value, old_version, new_version)
               VALUES %s""",
            values, page_size=500,
        )
    finally:
        cur.close()
        conn.close()


def run(batch_id=None):
    batch_id = batch_id or uuid.uuid4().hex[:8]
    spark = get_spark("SCDType2")
    silver_root = PATHS["silver_root"]
    gold_root = PATHS["gold_root"]
    start = now()
    pipeline_name = "scd2_dim_driver"

    print(f"===== SCD Type 2 (Driver dimension) | batch_id={batch_id} =====")
    log.info(f"SCD2 started | batch_id={batch_id}")

    try:
        current_drivers = (
            spark.read.parquet(os.path.join(silver_root, "drivers"))
            .withColumn("rating_bucket", (F.round(F.col("rating") * 2) / 2).cast("double"))
            .select("driver_id", "driver_name", "city", "status", "rating_bucket")
            .dropDuplicates(["driver_id"])
        )

        scd2_path = os.path.join(gold_root, SCD2_TABLE_NAME)
        today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_count = 0
        update_count = 0
        changes_for_log = []

        if os.path.exists(scd2_path) and os.listdir(scd2_path):
            existing = spark.read.parquet(scd2_path)

            # Rename the "old" side's columns up front (old_city, old_status,
            # ...) instead of relying on join aliases like "old.city" --
            # those qualifiers only work while building filter/select
            # expressions, but silently disappear once you .collect() a row
            # into a plain dict (duplicate field names just overwrite each
            # other). Renaming avoids that trap entirely.
            existing_current = existing.filter(F.col("is_current") == True).select(  # noqa: E712
                F.col("driver_id"),
                F.col("driver_name").alias("old_driver_name"),
                F.col("city").alias("old_city"),
                F.col("status").alias("old_status"),
                F.col("rating_bucket").alias("old_rating_bucket"),
                F.col("version_number").alias("old_version_number"),
            )

            joined = current_drivers.join(existing_current, on="driver_id", how="left")

            is_new_driver = F.col("old_version_number").isNull()
            changed_condition = F.lit(False)
            for col_name in TRACKED_COLUMNS:
                changed_condition = changed_condition | (~F.col(col_name).eqNullSafe(F.col(f"old_{col_name}")))

            changed_or_new = joined.filter(is_new_driver | changed_condition)

            # Build the change log by comparing old_* vs new columns on the
            # driver-side rows that actually had a prior version (skip
            # brand-new drivers -- nothing to compare them against).
            changed_rows = changed_or_new.filter(~is_new_driver).collect()
            new_driver_count = changed_or_new.filter(is_new_driver).count()
            insert_count = new_driver_count
            update_count = len(changed_rows)

            for row in changed_rows:
                d = row.asDict()
                old_version = d["old_version_number"]
                for col_name in TRACKED_COLUMNS:
                    old_val, new_val = d[f"old_{col_name}"], d[col_name]
                    if str(old_val) != str(new_val):
                        changes_for_log.append({
                            "driver_id": d["driver_id"], "changed_column": col_name,
                            "old_value": str(old_val), "new_value": str(new_val),
                            "old_version": old_version, "new_version": old_version + 1,
                        })

            changed_driver_ids = [row["driver_id"] for row in changed_or_new.select("driver_id").collect()]

            # Expire the current row for every changed/new driver that had one
            expired = (
                existing.filter(F.col("driver_id").isin(changed_driver_ids) & (F.col("is_current") == True))  # noqa: E712
                .withColumn("is_current", F.lit(False))
                .withColumn("effective_end_date", F.lit(today))
            )
            # Every other historical + current row is untouched
            untouched = existing.filter(
                ~(F.col("driver_id").isin(changed_driver_ids) & (F.col("is_current") == True))  # noqa: E712
            )

            new_versions_df = (
                changed_or_new
                .withColumn("version_number", (F.coalesce(F.col("old_version_number"), F.lit(0)) + 1).cast(IntegerType()))
                .withColumn("effective_start_date", F.lit(today))
                .withColumn("effective_end_date", F.lit(None).cast("string"))
                .withColumn("is_current", F.lit(True))
                .select("driver_id", "driver_name", "city", "status", "rating_bucket",
                        "effective_start_date", "effective_end_date", "is_current", "version_number")
            )

            output_columns = ["driver_id", "driver_name", "city", "status", "rating_bucket",
                               "effective_start_date", "effective_end_date", "is_current", "version_number"]
            final_df = (
                untouched.select(*output_columns)
                .unionByName(expired.select(*output_columns))
                .unionByName(new_versions_df)
            )
        else:
            insert_count = current_drivers.count()
            final_df = (
                current_drivers
                .withColumn("effective_start_date", F.lit(today))
                .withColumn("effective_end_date", F.lit(None).cast("string"))
                .withColumn("is_current", F.lit(True))
                .withColumn("version_number", F.lit(1).cast(IntegerType()))
            )

        # Never write directly back over a path Spark is lazily still reading
        # from (final_df's plan may reference scd2_path via `existing`) --
        # write to a temp path, then atomically swap it into place.
        tmp_path = scd2_path + f"_tmp_{batch_id}"
        final_df.write.mode("overwrite").parquet(tmp_path)
        total_current = spark.read.parquet(tmp_path).filter(F.col("is_current") == True).count()  # noqa: E712

        import shutil
        if os.path.exists(scd2_path):
            shutil.rmtree(scd2_path)
        os.rename(tmp_path, scd2_path)

        _write_change_log(batch_id, changes_for_log)

        end = now()
        log_pipeline_run(batch_id, pipeline_name, "gold", SCD2_TABLE_NAME, start, end, total_current,
                          "SUCCESS", insert_count=insert_count, update_count=update_count)
        log.info(f"[scd2:dim_driver] SUCCESS current_drivers={total_current} new={insert_count} changed={update_count}")
        print(f"[scd2:dim_driver] SUCCESS current_drivers={total_current} new={insert_count} changed={update_count}")

    except Exception as e:
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "gold", SCD2_TABLE_NAME, start, end, 0, "FAILED", error_message=str(e))
        log.error(f"[scd2:dim_driver] FAILED: {e}")
        print(f"[scd2:dim_driver] FAILED: {e}")
        raise
    finally:
        spark.stop()

    print(f"===== SCD Type 2 complete | batch_id={batch_id} =====")
    return batch_id


if __name__ == "__main__":
    provided_batch_id = sys.argv[1] if len(sys.argv) > 1 else None
    run(provided_batch_id)
