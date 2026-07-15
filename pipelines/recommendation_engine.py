"""
pipelines/recommendation_engine.py
-------------------------------------
Reads the Gold marts and applies simple, explainable business rules to
produce operational recommendations. No machine learning -- every rule
is a plain threshold check a business analyst could read and agree with.

Uses pandas, not Spark. The Gold marts here are small, already-aggregated
tables (a few hundred to a few thousand rows) -- reading them with Spark
would just add cluster overhead for no benefit. Spark is for the
heavy-lifting layers (Bronze/Silver/Gold); the recommendation engine is a
simple reporting step on top of that work, so it uses the simplest tool
that does the job.

Rules implemented:
  1. High cancellation rate in a zone/hour  -> deploy more drivers
  2. High demand-supply ratio in a zone     -> increase fleet
  3. Low driver utilization                 -> reduce active drivers
  4. High revenue zone                      -> increase incentives
  5. Bad weather correlated with more cancellations -> warn ops team
  6. Heavy traffic correlated with longer trips      -> predict delays

Each recommendation row: rule_name, scope_type, scope_value, metric_value,
recommendation text, severity (INFO/WARNING/CRITICAL).

Output: metadata.recommendations (Postgres) + data/gold/recommendations/
(Parquet, for the dashboard and Tableau-compatible export).

Run: python3 pipelines/recommendation_engine.py [batch_id]
"""
import os
import sys
import uuid

import pandas as pd
import pyarrow.parquet as pq

from config import PATHS, psycopg2_connect
from common import get_logger, log_pipeline_run, now

log = get_logger("recommendation_engine")

# Thresholds -- deliberately simple constants, not config-driven, since
# these are business judgement calls an analyst would tune directly here.
CANCELLATION_RATE_WARNING = 0.25
CANCELLATION_RATE_CRITICAL = 0.40
DEMAND_SUPPLY_RATIO_HIGH = 1.3
LOW_UTILIZATION_THRESHOLD = 0.30
TOP_REVENUE_ZONE_COUNT = 5


def read_gold_parquet(entity):
    return pd.read_parquet(os.path.join(PATHS["gold_root"], entity))


def read_silver_parquet(entity):
    return pd.read_parquet(os.path.join(PATHS["silver_root"], entity))


def rule_high_cancellation(df):
    """cancellation_analytics is already grouped by city/zone/hour. Flag
    any group where cancellation_rate crosses the warning/critical line."""
    recs = []
    flagged = df[df["cancellation_rate"] >= CANCELLATION_RATE_WARNING]
    for _, row in flagged.iterrows():
        severity = "CRITICAL" if row["cancellation_rate"] >= CANCELLATION_RATE_CRITICAL else "WARNING"
        recs.append({
            "rule_name": "HIGH_CANCELLATION_RATE",
            "scope_type": "ZONE",
            "scope_value": f"{row['city']} / {row['pickup_zone']} / hour {row['hour_of_day']}",
            "metric_value": round(float(row["cancellation_rate"]), 3),
            "recommendation": "Deploy more drivers to this zone during this hour to reduce cancellations.",
            "severity": severity,
        })
    return recs


def rule_high_demand_supply(df):
    recs = []
    flagged = df[df["demand_supply_ratio"] >= DEMAND_SUPPLY_RATIO_HIGH]
    for _, row in flagged.iterrows():
        recs.append({
            "rule_name": "HIGH_DEMAND_SUPPLY_RATIO",
            "scope_type": "ZONE",
            "scope_value": f"{row['city']} / {row['pickup_zone']} / hour {row['hour_of_day']}",
            "metric_value": round(float(row["demand_supply_ratio"]), 2),
            "recommendation": "Demand is outpacing available drivers -- increase fleet size in this zone.",
            "severity": "WARNING",
        })
    return recs


def rule_low_driver_utilization(df):
    recs = []
    flagged = df[(df["driver_utilization"] < LOW_UTILIZATION_THRESHOLD) & (df["total_trips"] >= 3)]
    for _, row in flagged.iterrows():
        recs.append({
            "rule_name": "LOW_DRIVER_UTILIZATION",
            "scope_type": "DRIVER",
            "scope_value": str(int(row["driver_id"])),
            "metric_value": round(float(row["driver_utilization"]), 3),
            "recommendation": "Driver utilization is low relative to online hours -- review scheduling or reduce active drivers in low-demand slots.",
            "severity": "INFO",
        })
    return recs


def rule_high_revenue_zone(df):
    """revenue_summary_mart is daily-grained; aggregate to zone level first,
    then flag the top N zones by total revenue as incentive candidates."""
    recs = []
    by_zone = (
        df.groupby(["city", "pickup_zone"], as_index=False)["total_revenue"]
        .sum()
        .sort_values("total_revenue", ascending=False)
        .head(TOP_REVENUE_ZONE_COUNT)
    )
    for _, row in by_zone.iterrows():
        recs.append({
            "rule_name": "HIGH_REVENUE_ZONE",
            "scope_type": "ZONE",
            "scope_value": f"{row['city']} / {row['pickup_zone']}",
            "metric_value": round(float(row["total_revenue"]), 2),
            "recommendation": "Top revenue-generating zone -- consider driver incentives to sustain supply here.",
            "severity": "INFO",
        })
    return recs


def rule_weather_impact(fact_trip, weather):
    """Joins trips to same-day weather by city and trip_date. Flags any
    weather condition where the cancellation rate on those days is
    meaningfully higher than the platform average."""
    recs = []
    weather = weather.rename(columns={"weather_date": "trip_date", "condition": "weather_condition"})
    merged = fact_trip.merge(weather[["city", "trip_date", "weather_condition"]], on=["city", "trip_date"], how="inner")
    if merged.empty:
        return recs

    overall_cancel_rate = (merged["trip_status"] == "CANCELLED").mean()
    by_condition = merged.groupby("weather_condition").agg(
        trips=("trip_id", "count"),
        cancelled=("trip_status", lambda s: (s == "CANCELLED").sum()),
    )
    by_condition["cancel_rate"] = by_condition["cancelled"] / by_condition["trips"]

    for condition, row in by_condition.iterrows():
        if row["trips"] >= 20 and row["cancel_rate"] > overall_cancel_rate * 1.3:
            recs.append({
                "rule_name": "WEATHER_IMPACT",
                "scope_type": "PLATFORM",
                "scope_value": str(condition),
                "metric_value": round(float(row["cancel_rate"]), 3),
                "recommendation": f"'{condition}' weather correlates with a higher cancellation rate than average -- warn operations team and consider surge adjustments.",
                "severity": "WARNING",
            })
    return recs


def rule_traffic_impact(fact_trip, traffic):
    """Joins trips to traffic records by city/zone. Flags zones where heavy
    traffic correlates with meaningfully longer average trip duration."""
    recs = []
    traffic_heavy = traffic[traffic["congestion_level"].isin(["HIGH", "SEVERE"])]
    if traffic_heavy.empty:
        return recs

    heavy_zones = set(zip(traffic_heavy["city"], traffic_heavy["zone_name"]))
    fact_trip["zone_key"] = list(zip(fact_trip["city"], fact_trip["pickup_zone"]))
    completed = fact_trip[fact_trip["trip_status"] == "COMPLETED"]

    overall_avg_duration = completed["trip_duration_minutes"].mean()
    for city, zone in heavy_zones:
        zone_trips = completed[(completed["city"] == city) & (completed["pickup_zone"] == zone)]
        if len(zone_trips) < 10:
            continue
        avg_duration = zone_trips["trip_duration_minutes"].mean()
        if avg_duration > overall_avg_duration * 1.25:
            recs.append({
                "rule_name": "TRAFFIC_IMPACT",
                "scope_type": "ZONE",
                "scope_value": f"{city} / {zone}",
                "metric_value": round(float(avg_duration), 1),
                "recommendation": "Heavy traffic congestion is inflating trip durations in this zone -- predict delays and notify customers proactively.",
                "severity": "WARNING",
            })
    return recs


def write_recommendations(batch_id, recs):
    if not recs:
        return
    for r in recs:
        r["batch_id"] = batch_id

    from psycopg2.extras import execute_values
    conn = psycopg2_connect()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        values = [
            (r["batch_id"], r["rule_name"], r["scope_type"], r["scope_value"],
             r["metric_value"], r["recommendation"], r["severity"])
            for r in recs
        ]
        execute_values(
            cur,
            """INSERT INTO metadata.recommendations
               (batch_id, rule_name, scope_type, scope_value, metric_value, recommendation, severity)
               VALUES %s""",
            values, page_size=500,
        )
    finally:
        cur.close()
        conn.close()

    out_df = pd.DataFrame(recs)
    out_path = os.path.join(PATHS["gold_root"], "recommendations")
    out_df.to_parquet(out_path, index=False)


def run(batch_id=None):
    batch_id = batch_id or uuid.uuid4().hex[:8]
    start = now()
    pipeline_name = "recommendation_engine"
    print(f"===== Recommendation Engine | batch_id={batch_id} =====")
    log.info(f"Recommendation Engine started | batch_id={batch_id}")

    try:
        cancellation_df = read_gold_parquet("cancellation_analytics")
        demand_supply_df = read_gold_parquet("demand_supply_analytics")
        ops_kpi_df = read_gold_parquet("ops_kpi_summary")
        revenue_df = read_gold_parquet("revenue_summary_mart")
        fact_trip_df = read_gold_parquet("fact_trip")
        weather_df = read_silver_parquet("weather")
        traffic_df = read_silver_parquet("traffic")

        recs = []
        recs += rule_high_cancellation(cancellation_df)
        recs += rule_high_demand_supply(demand_supply_df)
        recs += rule_low_driver_utilization(ops_kpi_df)
        recs += rule_high_revenue_zone(revenue_df)
        recs += rule_weather_impact(fact_trip_df, weather_df)
        recs += rule_traffic_impact(fact_trip_df, traffic_df)

        write_recommendations(batch_id, recs)

        end = now()
        log_pipeline_run(batch_id, pipeline_name, "gold", "recommendations", start, end,
                          len(recs), "SUCCESS", insert_count=len(recs))
        log.info(f"[recommendation_engine] SUCCESS recommendations={len(recs)}")
        print(f"[recommendation_engine] SUCCESS recommendations={len(recs)}")

        for r in recs[:10]:
            print(f"  [{r['severity']}] {r['rule_name']} -- {r['scope_value']}: {r['recommendation']}")
        if len(recs) > 10:
            print(f"  ... and {len(recs) - 10} more (see metadata.recommendations)")

    except Exception as e:
        end = now()
        log_pipeline_run(batch_id, pipeline_name, "gold", "recommendations", start, end, 0, "FAILED", error_message=str(e))
        log.error(f"[recommendation_engine] FAILED: {e}")
        print(f"[recommendation_engine] FAILED: {e}")
        raise

    print(f"===== Recommendation Engine complete | batch_id={batch_id} =====")
    return batch_id


if __name__ == "__main__":
    provided_batch_id = sys.argv[1] if len(sys.argv) > 1 else None
    run(provided_batch_id)
