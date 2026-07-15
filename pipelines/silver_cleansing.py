"""
pipelines/silver_cleansing.py
--------------------------------
Reads Bronze Parquet for each entity, applies:
  - Schema / null / primary-key / data-type / duplicate validation
  - Rejects bad records into error.rejected_records (Postgres) with reason
  - Cleansing & standardization (trim strings, cast types, uppercase codes)
  - Lookup mapping (city -> city_tier) as an enrichment example
  - SCD Type 1 (overwrite) for the Drivers dimension: driver_name, city,
    status always reflect the latest known value, no history kept.
    (Full SCD Type 2 history for drivers lives in pipelines/scd_type2.py,
    which reads this same Silver output and runs after it.)

Writes clean, validated Parquet to data/silver/<entity>/.

VALIDATION ENGINE: validation runs as native Spark column expressions
(window functions + UDFs), not a Python for-loop over df.collect() rows.
The old row-by-row driver-side loop worked for a few thousand rows but
doesn't scale to the platform's 100k-trip target -- collecting the full
dataset to the driver and looping serially becomes the bottleneck. Now
only REJECTED rows (typically a small % of the batch) are ever collected
to the driver, to serialize their JSON into Postgres; valid rows stay in
Spark end to end. Reject DB writes are batched (execute_values), not one
INSERT per row.

INCREMENTAL LOADING (--incremental flag; full re-cleanse is still the
default and unchanged): validates only Bronze partitions written since
Silver's last successful watermark, then merges (upsert by primary key)
into the existing Silver Parquet rather than overwriting it wholesale.

Run: python3 pipelines/silver_cleansing.py [batch_id] [--incremental]
"""
import argparse
import os
import sys
import uuid
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from config import get_spark, PATHS
from common import (
    get_logger, log_pipeline_run, reject_records_batch,
    get_watermark, set_watermark, now,
)

log = get_logger("silver")

CITY_TIER_LOOKUP = {
    "Bengaluru": "Tier-1", "Mumbai": "Tier-1", "Delhi": "Tier-1",
    "Hyderabad": "Tier-1", "Chennai": "Tier-1",
}

SILVER_WATERMARK_SUFFIX = "_silver"


def latest_bronze_df(spark, entity, bronze_root, incremental=False):
    """Full mode: reads ALL Bronze partitions (every batch_id ever ingested)
    -- the platform re-cleanses full history each run, simplest to reason
    about for a batch-oriented PoC. Incremental mode: reads only partitions
    with _ingestion_timestamp newer than Silver's last successful
    watermark for this entity."""
    path = os.path.join(bronze_root, entity)
    df = spark.read.parquet(path)
    if incremental:
        wm_col, wm_val = get_watermark(entity + SILVER_WATERMARK_SUFFIX)
        if wm_val:
            df = df.filter(F.col("_ingestion_timestamp") > F.lit(wm_val))
    return df


def validate_and_split(df, pk_column, required_columns, numeric_checks=None, custom_checks=None):
    """
    Vectorized validation. Returns (valid_df, rejected_df) where rejected_df
    has an extra `_reject_reason` column. Semantics match the original
    row-by-row implementation exactly (same checks, same precedence order:
    null PK -> duplicate PK -> null required field -> numeric/business rule
    -> custom cross-column rule), just executed as Spark expressions/UDFs
    instead of a Python loop.
    """
    numeric_checks = numeric_checks or []
    custom_checks = custom_checks or []

    df = df.withColumn("_row_id", F.monotonically_increasing_id())

    pk_is_null = F.col(pk_column).isNull() | (F.trim(F.col(pk_column).cast("string")) == "")
    dup_window = Window.partitionBy(pk_column).orderBy("_row_id")
    df = df.withColumn(
        "_pk_rank",
        F.when(pk_is_null, F.lit(1)).otherwise(F.row_number().over(dup_window))
    )

    # Each check is its own independent, flat column (null when that check
    # passes) -- avoids nesting one when().otherwise() inside another, which
    # for wide entities like trips generates Java bytecode past Spark's 64KB
    # per-method codegen limit (still correct when that happens, since Spark
    # falls back to interpreted evaluation, but much slower at 100k+ rows).
    # coalesce() then picks the first non-null in priority order, same
    # precedence as the original: PK -> duplicate -> required -> numeric ->
    # custom.
    candidate_reasons = [
        F.when(pk_is_null, F.lit("NULL_PRIMARY_KEY")),
        F.when(F.col("_pk_rank") > 1, F.lit("DUPLICATE_PRIMARY_KEY")),
    ]

    for c in required_columns:
        cond = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        candidate_reasons.append(F.when(cond, F.lit(f"NULL_REQUIRED_FIELD:{c}")))

    for column, predicate_fn, reason_label in numeric_checks:
        def _check(v, fn=predicate_fn, label=reason_label, col=column):
            if v is None:
                return None
            try:
                ok = bool(fn(float(v)))
            except (TypeError, ValueError):
                return f"INVALID_DATA_TYPE:{col}"
            return None if ok else label
        check_udf = F.udf(_check, "string")
        candidate_reasons.append(check_udf(F.col(column)))

    custom_cols = [c for c in df.columns if not c.startswith("_")]
    for predicate_fn, reason_label in custom_checks:
        def _check_custom(row, fn=predicate_fn, label=reason_label):
            try:
                ok = bool(fn(row.asDict()))
            except (TypeError, ValueError):
                return label
            return None if ok else label
        check_udf = F.udf(_check_custom, "string")
        candidate_reasons.append(check_udf(F.struct(*custom_cols)))

    df = df.withColumn("_reject_reason", F.coalesce(*candidate_reasons))
    valid_df = df.filter(F.col("_reject_reason").isNull()).drop("_reject_reason", "_row_id", "_pk_rank")
    rejected_df = df.filter(F.col("_reject_reason").isNotNull()).drop("_row_id", "_pk_rank")
    return valid_df, rejected_df


def merge_upsert(spark, silver_root, entity, pk_column, new_valid_df):
    """Incremental-mode write: upsert new_valid_df into the existing Silver
    dataset by primary key (new version wins), instead of overwriting the
    whole table. If no prior Silver output exists yet, this is just a
    first full write.

    Writes to a temp path first, then atomically swaps it into place --
    writing directly back over out_path while merged_df's lazy plan still
    references out_path (via existing_df) causes Spark to delete files out
    from under its own still-running read task."""
    import shutil
    out_path = os.path.join(silver_root, entity)
    if os.path.exists(out_path) and os.listdir(out_path):
        existing_df = spark.read.parquet(out_path)
        unchanged_df = existing_df.join(
            new_valid_df.select(pk_column).distinct(), on=pk_column, how="left_anti"
        )
        merged_df = unchanged_df.unionByName(new_valid_df, allowMissingColumns=True)
    else:
        merged_df = new_valid_df

    tmp_path = out_path + "_tmp_merge"
    merged_df.write.mode("overwrite").parquet(tmp_path)
    count = spark.read.parquet(tmp_path).count()
    if os.path.exists(out_path):
        shutil.rmtree(out_path)
    os.rename(tmp_path, out_path)
    return count


def run_entity_validation(spark, entity, pk_column, required_columns, numeric_checks,
                           batch_id, bronze_root, silver_root, extra_transform=None,
                           custom_checks=None, incremental=False):
    start = now()
    pipeline_name = f"silver_{entity}"
    try:
        df = latest_bronze_df(spark, entity, bronze_root, incremental)

        if incremental and df.rdd.isEmpty():
            end = now()
            log_pipeline_run(batch_id, pipeline_name, "silver", entity, start, end, 0, "SUCCESS")
            log.info(f"[silver:{entity}] SKIPPED (no new Bronze rows since last watermark)")
            print(f"[silver:{entity}] SKIPPED (no new rows, incremental mode)")
            return

        valid_df, rejected_df = validate_and_split(df, pk_column, required_columns, numeric_checks, custom_checks)

        reject_reason_counts = (
            rejected_df.groupBy("_reject_reason").count().collect() if rejected_df.take(1) else []
        )
        total_rejected = sum(r["count"] for r in reject_reason_counts)

        for r in reject_reason_counts:
            reason = r["_reject_reason"]
            reason_rows_df = rejected_df.filter(F.col("_reject_reason") == reason).drop("_reject_reason")
            rows = [row.asDict() for row in reason_rows_df.collect()]
            reject_records_batch(batch_id, entity, rows, pk_column, reason)

        if extra_transform:
            valid_df = extra_transform(valid_df)

        if incremental:
            valid_count = merge_upsert(spark, silver_root, entity, pk_column, valid_df)
        else:
            out_path = os.path.join(silver_root, entity)
            valid_df.write.mode("overwrite").parquet(out_path)
            valid_count = valid_df.count()

        if incremental:
            max_ts = df.agg(F.max("_ingestion_timestamp")).collect()[0][0]
            if max_ts:
                set_watermark(entity + SILVER_WATERMARK_SUFFIX, "_ingestion_timestamp", max_ts)

        end = now()
        log_pipeline_run(batch_id, pipeline_name, "silver", entity, start, end, valid_count, "SUCCESS",
                          insert_count=valid_count, reject_count=total_rejected)
        log.info(f"[silver:{entity}] SUCCESS valid={valid_count} rejected={total_rejected}")
        print(f"[silver:{entity}] SUCCESS valid={valid_count} rejected={total_rejected}")

    except Exception as e:
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "silver", entity, start, end, 0, "FAILED", error_message=str(e))
        log.error(f"[silver:{entity}] FAILED: {e}")
        print(f"[silver:{entity}] FAILED: {e}")


# ---------------------------------------------------------------------------
# Entity-specific enrichment / SCD transforms
# ---------------------------------------------------------------------------
def enrich_drivers_scd1(df):
    """
    SCD Type 1 for the Driver dimension: we simply keep the latest values
    (no history). Since Bronze already reflects the current source state,
    SCD1 here means: standardize + overwrite silver/drivers entirely with
    the latest cleansed snapshot (that overwrite IS the SCD1 behavior --
    old attribute values are not retained anywhere in Silver; full history
    IS retained separately by scd_type2.py, which runs on this output).
    """
    mapping_expr = F.create_map([F.lit(x) for pair in CITY_TIER_LOOKUP.items() for x in pair])
    df = (
        df.withColumn("driver_name", F.trim(F.col("driver_name")))
          .withColumn("city", F.trim(F.col("city")))
          .withColumn("status", F.upper(F.trim(F.col("status"))))
          .withColumn("rating", F.col("rating").cast(DoubleType()))
          .withColumn("driver_id", F.col("driver_id").cast(IntegerType()))
          .withColumn("city_tier", mapping_expr[F.col("city")])
          .withColumn("_scd_type", F.lit("SCD1"))
          .withColumn("_silver_updated_at", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    )
    return df


def enrich_customers(df):
    mapping_expr = F.create_map([F.lit(x) for pair in CITY_TIER_LOOKUP.items() for x in pair])
    return (
        df.withColumn("customer_name", F.trim(F.col("customer_name")))
          .withColumn("city", F.trim(F.col("city")))
          .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
          .withColumn("city_tier", mapping_expr[F.col("city")])
    )


def enrich_vehicles(df):
    return (
        df.withColumn("vehicle_id", F.col("vehicle_id").cast(IntegerType()))
          .withColumn("driver_id", F.col("driver_id").cast(IntegerType()))
          .withColumn("model_year", F.col("model_year").cast(IntegerType()))
          .withColumn("registration_number", F.upper(F.trim(F.col("registration_number"))))
    )


def enrich_trips(df):
    mapping_expr = F.create_map([F.lit(x) for pair in CITY_TIER_LOOKUP.items() for x in pair])
    return (
        df.withColumn("trip_id", F.col("trip_id").cast(IntegerType()))
          .withColumn("driver_id", F.col("driver_id").cast(IntegerType()))
          .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
          .withColumn("vehicle_id", F.col("vehicle_id").cast(IntegerType()))
          .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
          .withColumn("distance_km", F.col("distance_km").cast(DoubleType()))
          .withColumn("request_time", F.col("request_time").cast(TimestampType()))
          .withColumn("drop_time", F.col("drop_time").cast(TimestampType()))
          .withColumn("city_tier", mapping_expr[F.col("city")])
          .withColumn(
              "trip_duration_minutes",
              (F.unix_timestamp("drop_time") - F.unix_timestamp("request_time")) / 60.0
          )
    )


def enrich_payments(df):
    return (
        df.withColumn("payment_id", F.col("payment_id").cast(IntegerType()))
          .withColumn("trip_id", F.col("trip_id").cast(IntegerType()))
          .withColumn("amount", F.col("amount").cast(DoubleType()))
          .withColumn("payment_mode", F.upper(F.trim(F.col("payment_mode"))))
    )


def enrich_driver_shift(df):
    return (
        df.withColumn("shift_id", F.col("shift_id").cast(IntegerType()))
          .withColumn("driver_id", F.col("driver_id").cast(IntegerType()))
          .withColumn("online_hours", F.col("online_hours").cast(DoubleType()))
    )


def enrich_traffic(df):
    return df.withColumn("traffic_id", F.col("traffic_id").cast(IntegerType()))


def enrich_weather(df):
    return df.withColumn("weather_id", F.col("weather_id").cast(IntegerType()))


def run(batch_id=None, incremental=False):
    batch_id = batch_id or uuid.uuid4().hex[:8]
    spark = get_spark("SilverCleansing")
    bronze_root = PATHS["bronze_root"]
    silver_root = PATHS["silver_root"]

    mode_label = "INCREMENTAL" if incremental else "FULL"
    print(f"===== Silver Cleansing | batch_id={batch_id} | mode={mode_label} =====")
    log.info(f"Silver Cleansing started | batch_id={batch_id} | mode={mode_label}")

    run_entity_validation(
        spark, "drivers", "driver_id", ["driver_name", "city", "status"],
        [("rating", lambda v: 1.0 <= v <= 5.0, "RATING_OUT_OF_RANGE")],
        batch_id, bronze_root, silver_root, extra_transform=enrich_drivers_scd1, incremental=incremental,
    )

    run_entity_validation(
        spark, "customers", "customer_id", ["customer_name", "city"],
        [], batch_id, bronze_root, silver_root, extra_transform=enrich_customers, incremental=incremental,
    )

    run_entity_validation(
        spark, "vehicles", "vehicle_id", ["driver_id", "vehicle_type", "registration_number"],
        [], batch_id, bronze_root, silver_root, extra_transform=enrich_vehicles, incremental=incremental,
    )

    def drop_after_request(row):
        req, drp = row.get("request_time"), row.get("drop_time")
        if req is None or drp is None:
            return True  # handled separately by null/required checks
        return str(drp) >= str(req)

    run_entity_validation(
        spark, "trips", "trip_id", ["driver_id", "customer_id", "vehicle_id", "request_time"],
        [("fare_amount", lambda v: v >= 0, "NEGATIVE_FARE")],
        batch_id, bronze_root, silver_root, extra_transform=enrich_trips,
        custom_checks=[(drop_after_request, "MISMATCHED_TRIP_TIMESTAMPS")], incremental=incremental,
    )

    run_entity_validation(
        spark, "payments", "payment_id", ["trip_id", "amount", "payment_mode"],
        [("amount", lambda v: v > 0, "INVALID_PAYMENT_AMOUNT")],
        batch_id, bronze_root, silver_root, extra_transform=enrich_payments, incremental=incremental,
    )

    run_entity_validation(
        spark, "driver_shift", "shift_id", ["driver_id", "shift_date"],
        [], batch_id, bronze_root, silver_root, extra_transform=enrich_driver_shift, incremental=incremental,
    )

    run_entity_validation(
        spark, "traffic", "traffic_id", ["city", "zone_name"],
        [], batch_id, bronze_root, silver_root, extra_transform=enrich_traffic, incremental=incremental,
    )

    run_entity_validation(
        spark, "weather", "weather_id", ["city", "weather_date"],
        [], batch_id, bronze_root, silver_root, extra_transform=enrich_weather, incremental=incremental,
    )

    print(f"===== Silver Cleansing complete | batch_id={batch_id} =====")
    log.info(f"Silver Cleansing complete | batch_id={batch_id}")
    spark.stop()
    return batch_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver cleansing")
    parser.add_argument("batch_id", nargs="?", default=None)
    parser.add_argument("--incremental", action="store_true",
                         help="Process only new Bronze rows since last watermark; upsert into Silver")
    args = parser.parse_args()
    run(args.batch_id, args.incremental)
