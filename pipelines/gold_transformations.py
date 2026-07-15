"""
pipelines/gold_transformations.py
------------------------------------
Builds the business-ready Gold layer from Silver:

    Dimensions : dim_driver, dim_customer, dim_vehicle
    Fact       : fact_trip (joined with payments, drivers, vehicles, customers)
    Marts      : revenue_summary_mart, ops_kpi_summary, cancellation_analytics,
                 demand_supply_analytics

Demonstrates: joins, aggregations, window functions, derived columns.

Run: python3 pipelines/gold_transformations.py [batch_id]
"""
import os
import sys
import uuid
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config import get_spark, PATHS
from common import get_logger, log_pipeline_run, now

log = get_logger("gold")


def build_and_log(name, batch_id, build_fn, out_root):
    start = now()
    try:
        df = build_fn()
        count = df.count()
        out_path = os.path.join(out_root, name)
        df.write.mode("overwrite").parquet(out_path)
        end = now()
        log_pipeline_run(batch_id, f"gold_{name}", "gold", name, start, end, count, "SUCCESS", insert_count=count)
        log.info(f"[gold:{name}] SUCCESS records={count}")
        print(f"[gold:{name}] SUCCESS records={count}")
        return df
    except Exception as e:
        end = now()
        log_pipeline_run(batch_id, f"gold_{name}", "gold", name, start, end, 0, "FAILED", error_message=str(e))
        log.error(f"[gold:{name}] FAILED: {e}")
        print(f"[gold:{name}] FAILED: {e}")
        return None


def run(batch_id=None):
    batch_id = batch_id or uuid.uuid4().hex[:8]
    spark = get_spark("GoldTransformations")
    silver_root = PATHS["silver_root"]
    gold_root = PATHS["gold_root"]

    print(f"===== Gold Transformations | batch_id={batch_id} =====")

    drivers = spark.read.parquet(os.path.join(silver_root, "drivers"))
    customers = spark.read.parquet(os.path.join(silver_root, "customers"))
    vehicles = spark.read.parquet(os.path.join(silver_root, "vehicles"))
    trips = spark.read.parquet(os.path.join(silver_root, "trips"))
    payments = spark.read.parquet(os.path.join(silver_root, "payments"))
    shifts = spark.read.parquet(os.path.join(silver_root, "driver_shift"))

    trips.cache()
    drivers.cache()

    # -----------------------------------------------------------------
    # DIMENSION: dim_driver — driver stats via aggregation + window function
    # -----------------------------------------------------------------
    def build_dim_driver():
        trip_counts = (
            trips.groupBy("driver_id")
                 .agg(
                     F.count("*").alias("total_trips"),
                     F.sum(F.when(F.col("trip_status") == "COMPLETED", 1).otherwise(0)).alias("completed_trips"),
                 )
        )
        dim = (
            drivers.join(trip_counts, on="driver_id", how="left")
                   .fillna({"total_trips": 0, "completed_trips": 0})
        )
        # Window function: rank drivers by rating within their city
        city_window = Window.partitionBy("city").orderBy(F.desc("rating"))
        dim = dim.withColumn("rating_rank_in_city", F.rank().over(city_window))
        return dim.select("driver_id", "driver_name", "city", "city_tier", "rating",
                           "status", "total_trips", "completed_trips", "rating_rank_in_city")

    dim_driver = build_and_log("dim_driver", batch_id, build_dim_driver, gold_root)

    # -----------------------------------------------------------------
    # DIMENSION: dim_customer
    # -----------------------------------------------------------------
    def build_dim_customer():
        trip_counts = trips.groupBy("customer_id").agg(F.count("*").alias("total_trips_taken"))
        dim = (
            customers.join(trip_counts, on="customer_id", how="left")
                     .fillna({"total_trips_taken": 0})
        )
        return dim.select("customer_id", "customer_name", "city", "city_tier",
                           "is_premium", "total_trips_taken")

    dim_customer = build_and_log("dim_customer", batch_id, build_dim_customer, gold_root)

    # -----------------------------------------------------------------
    # DIMENSION: dim_vehicle
    # -----------------------------------------------------------------
    def build_dim_vehicle():
        trip_counts = trips.groupBy("vehicle_id").agg(F.count("*").alias("total_trips"))
        dim = vehicles.join(trip_counts, on="vehicle_id", how="left").fillna({"total_trips": 0})
        return dim.select("vehicle_id", "driver_id", "vehicle_type", "make",
                           "model_year", "registration_number", "is_active", "total_trips")

    dim_vehicle = build_and_log("dim_vehicle", batch_id, build_dim_vehicle, gold_root)

    # -----------------------------------------------------------------
    # FACT: fact_trip — joins trips + payments + drivers + vehicles + customers
    # -----------------------------------------------------------------
    def build_fact_trip():
        fact = (
            trips.join(payments.select("trip_id", "amount", "payment_mode", "payment_status"),
                       on="trip_id", how="left")
                 .join(drivers.select(F.col("driver_id"), F.col("city_tier").alias("driver_city_tier")),
                       on="driver_id", how="left")
                 .join(vehicles.select("vehicle_id", "vehicle_type"), on="vehicle_id", how="left")
        )
        fact = fact.withColumn("hour_of_day", F.hour("request_time"))
        fact = fact.withColumn(
            "trip_time_bucket",
            F.when(F.col("hour_of_day").between(6, 10), "MORNING_PEAK")
             .when(F.col("hour_of_day").between(17, 21), "EVENING_PEAK")
             .otherwise("OFF_PEAK")
        )
        return fact.select(
            "trip_id", "driver_id", "customer_id", "vehicle_id", "city", "pickup_zone", "drop_zone",
            "request_time", "drop_time", "trip_date", "trip_status", "distance_km", "fare_amount",
            "trip_duration_minutes", "amount", "payment_mode", "payment_status",
            "vehicle_type", "hour_of_day", "trip_time_bucket"
        )

    fact_trip = build_and_log("fact_trip", batch_id, build_fact_trip, gold_root)
    fact_trip.cache()

    # -----------------------------------------------------------------
    # MART: revenue_summary_mart — revenue aggregations by city/zone/day
    # -----------------------------------------------------------------
    def build_revenue_mart():
        completed = fact_trip.filter(F.col("trip_status") == "COMPLETED")
        return (
            completed.groupBy("trip_date", "city", "pickup_zone")
                     .agg(
                         F.sum("fare_amount").alias("total_revenue"),
                         F.count("*").alias("total_trips"),
                         F.avg("fare_amount").alias("avg_fare"),
                         F.avg("distance_km").alias("avg_distance_km"),
                     )
                     .withColumn("revenue_per_trip", F.round(F.col("total_revenue") / F.col("total_trips"), 2))
        )

    build_and_log("revenue_summary_mart", batch_id, build_revenue_mart, gold_root)

    # -----------------------------------------------------------------
    # MART: ops_kpi_summary — driver utilization, wait proxy, revenue per driver
    # -----------------------------------------------------------------
    def build_ops_kpi_summary():
        shift_hours = shifts.groupBy("driver_id").agg(F.sum("online_hours").alias("total_online_hours"))
        driver_trip_stats = (
            fact_trip.groupBy("driver_id")
                     .agg(
                         F.count("*").alias("total_trips"),
                         F.sum(F.when(F.col("trip_status") == "COMPLETED", 1).otherwise(0)).alias("completed_trips"),
                         F.sum(F.coalesce(F.col("fare_amount"), F.lit(0.0))).alias("total_revenue"),
                     )
        )
        kpi = driver_trip_stats.join(shift_hours, on="driver_id", how="left").fillna({"total_online_hours": 0})
        kpi = kpi.withColumn(
            "driver_utilization",
            F.when(F.col("total_online_hours") > 0, F.round(F.col("completed_trips") / F.col("total_online_hours"), 3))
             .otherwise(F.lit(0.0))
        )
        kpi = kpi.withColumn(
            "revenue_per_driver",
            F.round(F.col("total_revenue") / F.when(F.col("completed_trips") > 0, F.col("completed_trips")).otherwise(F.lit(1)), 2)
        )
        return kpi

    build_and_log("ops_kpi_summary", batch_id, build_ops_kpi_summary, gold_root)

    # -----------------------------------------------------------------
    # MART: cancellation_analytics — cancellation rate by city/zone/hour
    # -----------------------------------------------------------------
    def build_cancellation_analytics():
        return (
            fact_trip.groupBy("city", "pickup_zone", "hour_of_day")
                     .agg(
                         F.count("*").alias("total_trip_requests"),
                         F.sum(F.when(F.col("trip_status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_trips"),
                     )
                     .withColumn(
                         "cancellation_rate",
                         F.round(F.col("cancelled_trips") / F.col("total_trip_requests"), 3)
                     )
        )

    build_and_log("cancellation_analytics", batch_id, build_cancellation_analytics, gold_root)

    # -----------------------------------------------------------------
    # MART: demand_supply_analytics — trips requested vs distinct drivers active, per zone/hour
    # -----------------------------------------------------------------
    def build_demand_supply_analytics():
        demand = (
            fact_trip.groupBy("city", "pickup_zone", "hour_of_day")
                     .agg(F.count("*").alias("demand_trip_requests"))
        )
        supply = (
            fact_trip.groupBy("city", "pickup_zone", "hour_of_day")
                     .agg(F.countDistinct("driver_id").alias("active_drivers"))
        )
        combined = demand.join(supply, on=["city", "pickup_zone", "hour_of_day"], how="inner")
        combined = combined.withColumn(
            "demand_supply_ratio",
            F.round(F.col("demand_trip_requests") / F.col("active_drivers"), 2)
        )
        # Window function: rank zones by demand-supply ratio within each city
        city_window = Window.partitionBy("city").orderBy(F.desc("demand_supply_ratio"))
        combined = combined.withColumn("shortage_rank_in_city", F.rank().over(city_window))
        return combined

    build_and_log("demand_supply_analytics", batch_id, build_demand_supply_analytics, gold_root)

    print(f"===== Gold Transformations complete | batch_id={batch_id} =====")
    spark.stop()
    return batch_id


if __name__ == "__main__":
    provided_batch_id = sys.argv[1] if len(sys.argv) > 1 else None
    run(provided_batch_id)
