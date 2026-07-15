"""
pipelines/dashboard.py
-------------------------
Builds the executive dashboard from Gold marts + the recommendation
engine output. Three outputs, all written to data/dashboard/:

  1. dashboard.html  -- one interactive Plotly dashboard (opens in any
     browser, no server needed)
  2. *.png            -- a static snapshot of each chart, for slides/README
  3. *.csv             -- one Tableau-compatible CSV per mart (flat,
     already-aggregated tables Tableau/Excel/Power BI can load directly)

Sections covered (per the platform's dashboard requirements):
  Revenue trend, Driver utilization, Trips, Demand vs Supply, Peak hours,
  Zone performance, Cancellation %, Recommendations, Pipeline health
  (from metadata.pipeline_runs).

Uses pandas + plotly, not Spark -- same reasoning as the recommendation
engine: these are small, already-aggregated Gold tables, and a dashboard
script's job is to read and chart them simply, not to do heavy
distributed computation.

Run: python3 pipelines/dashboard.py
"""
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import PATHS, psycopg2_connect
from common import get_logger

log = get_logger("dashboard")


def read_gold(entity):
    return pd.read_parquet(os.path.join(PATHS["gold_root"], entity))


def read_pipeline_health():
    import warnings
    conn = psycopg2_connect()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            return pd.read_sql(
                """SELECT layer, entity, status, record_count, reject_count, duration_seconds, created_at
                   FROM metadata.pipeline_runs
                   ORDER BY created_at DESC LIMIT 50""",
                conn,
            )
    finally:
        conn.close()


def build_revenue_trend_fig(revenue_df):
    daily = revenue_df.groupby("trip_date", as_index=False)["total_revenue"].sum().sort_values("trip_date")
    fig = px.line(daily, x="trip_date", y="total_revenue", title="Revenue Trend (Daily)", markers=True)
    return fig


def build_zone_performance_fig(revenue_df):
    by_zone = (
        revenue_df.groupby(["city", "pickup_zone"], as_index=False)["total_revenue"]
        .sum().sort_values("total_revenue", ascending=False).head(15)
    )
    by_zone["zone_label"] = by_zone["city"] + " / " + by_zone["pickup_zone"]
    fig = px.bar(by_zone, x="zone_label", y="total_revenue", title="Top 15 Zones by Revenue")
    return fig


def build_driver_utilization_fig(ops_kpi_df):
    fig = px.histogram(ops_kpi_df, x="driver_utilization", nbins=20,
                        title="Driver Utilization Distribution")
    return fig


def build_demand_supply_fig(demand_supply_df):
    top_shortage = demand_supply_df.sort_values("demand_supply_ratio", ascending=False).head(15)
    top_shortage["zone_label"] = top_shortage["city"] + " / " + top_shortage["pickup_zone"] + " @" + top_shortage["hour_of_day"].astype(str) + "h"
    fig = px.bar(top_shortage, x="zone_label", y="demand_supply_ratio",
                 title="Top 15 Demand-vs-Supply Shortage Zones/Hours")
    return fig


def build_peak_hours_fig(fact_trip_df):
    by_hour = fact_trip_df.groupby("hour_of_day", as_index=False)["trip_id"].count().rename(columns={"trip_id": "trip_count"})
    fig = px.bar(by_hour.sort_values("hour_of_day"), x="hour_of_day", y="trip_count", title="Trips by Hour of Day")
    return fig


def build_cancellation_fig(cancellation_df):
    by_city = cancellation_df.groupby("city", as_index=False).agg(
        total_requests=("total_trip_requests", "sum"), cancelled=("cancelled_trips", "sum")
    )
    by_city["cancellation_pct"] = (by_city["cancelled"] / by_city["total_requests"] * 100).round(1)
    fig = px.bar(by_city, x="city", y="cancellation_pct", title="Cancellation % by City")
    return fig


def build_top_drivers_fig(ops_kpi_df):
    top = ops_kpi_df.sort_values("total_revenue", ascending=False).head(10)
    fig = px.bar(top, x="driver_id", y="total_revenue", title="Top 10 Drivers by Revenue")
    fig.update_xaxes(type="category")
    return fig


def build_pipeline_health_fig(health_df):
    if health_df.empty:
        return go.Figure()
    fig = px.scatter(health_df, x="created_at", y="duration_seconds", color="status",
                      hover_data=["layer", "entity", "record_count", "reject_count"],
                      title="Pipeline Run Duration & Status (last 50 runs)")
    return fig


def build_recommendations_table_fig(recs_df):
    if recs_df.empty:
        return go.Figure()
    top = recs_df.sort_values("severity").head(20)
    fig = go.Figure(data=[go.Table(
        header=dict(values=["Severity", "Rule", "Scope", "Recommendation"], fill_color="#2c3e50", font=dict(color="white")),
        cells=dict(values=[top["severity"], top["rule_name"], top["scope_value"], top["recommendation"]]),
    )])
    fig.update_layout(title="Top Operational Recommendations")
    return fig


def build_html_dashboard(figures, out_path):
    """Stitches all figures into one static HTML file, each section
    stacked vertically -- simplest possible layout, no dashboard
    framework or server needed."""
    with open(out_path, "w") as f:
        f.write("<html><head><title>Urban Mobility Intelligence Platform - Executive Dashboard</title>")
        f.write("<style>body{font-family:Arial, sans-serif;background:#f4f6f8;margin:0;padding:20px;}"
                "h1{color:#2c3e50;} .chart{background:white;border-radius:8px;padding:10px;margin-bottom:20px;"
                "box-shadow:0 1px 4px rgba(0,0,0,0.1);}</style></head><body>")
        f.write("<h1>Urban Mobility Intelligence Platform</h1><p>Executive Dashboard</p>")
        for name, fig in figures.items():
            f.write(f'<div class="chart">{fig.to_html(full_html=False, include_plotlyjs="cdn")}</div>')
        f.write("</body></html>")


def export_tableau_csvs(marts, out_dir):
    for name, df in marts.items():
        df.to_csv(os.path.join(out_dir, f"{name}.csv"), index=False)


def export_png_snapshots(figures, out_dir):
    """PNG export needs the `kaleido` package. If it's not installed, we
    skip PNGs and keep the HTML/CSV outputs (which don't depend on it) --
    fail soft rather than blocking the whole dashboard run."""
    try:
        for name, fig in figures.items():
            fig.write_image(os.path.join(out_dir, f"{name}.png"), width=1000, height=600)
        return True
    except Exception as e:
        log.warning(f"PNG export skipped (kaleido not available or failed): {e}")
        print(f"[dashboard] PNG export skipped: {e}")
        return False


def run():
    print("===== Dashboard Generation =====")
    log.info("Dashboard generation started")

    out_dir = PATHS["dashboard_root"]
    os.makedirs(out_dir, exist_ok=True)

    revenue_df = read_gold("revenue_summary_mart")
    ops_kpi_df = read_gold("ops_kpi_summary")
    demand_supply_df = read_gold("demand_supply_analytics")
    cancellation_df = read_gold("cancellation_analytics")
    fact_trip_df = read_gold("fact_trip")
    try:
        recs_df = read_gold("recommendations")
    except Exception:
        recs_df = pd.DataFrame(columns=["severity", "rule_name", "scope_value", "recommendation"])
    health_df = read_pipeline_health()

    figures = {
        "revenue_trend": build_revenue_trend_fig(revenue_df),
        "zone_performance": build_zone_performance_fig(revenue_df),
        "driver_utilization": build_driver_utilization_fig(ops_kpi_df),
        "demand_supply": build_demand_supply_fig(demand_supply_df),
        "peak_hours": build_peak_hours_fig(fact_trip_df),
        "cancellation_pct": build_cancellation_fig(cancellation_df),
        "top_drivers": build_top_drivers_fig(ops_kpi_df),
        "pipeline_health": build_pipeline_health_fig(health_df),
        "recommendations": build_recommendations_table_fig(recs_df),
    }

    build_html_dashboard(figures, os.path.join(out_dir, "dashboard.html"))
    png_ok = export_png_snapshots(figures, out_dir)
    export_tableau_csvs({
        "revenue_summary_mart": revenue_df,
        "ops_kpi_summary": ops_kpi_df,
        "demand_supply_analytics": demand_supply_df,
        "cancellation_analytics": cancellation_df,
        "recommendations": recs_df,
    }, out_dir)

    log.info(f"Dashboard generation complete | png_export={png_ok}")
    print(f"[dashboard] SUCCESS -- HTML: {out_dir}/dashboard.html, CSVs + {'PNGs' if png_ok else 'no PNGs (kaleido unavailable)'} in {out_dir}/")
    print("===== Dashboard Generation complete =====")


if __name__ == "__main__":
    run()
