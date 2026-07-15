# 15. Recommendation Engine

**Module:** `pipelines/recommendation_engine.py`

Pure rule-based, no machine learning -- every rule is a plain threshold
check a business analyst could read, agree with, and tune the constants
on without needing to understand a model.

## Rules

| Rule | Reads | Threshold | Recommendation |
|---|---|---|---|
| `HIGH_CANCELLATION_RATE` | `cancellation_analytics` | cancellation_rate ≥ 25% (WARNING) / ≥ 40% (CRITICAL) | Deploy more drivers to this zone/hour |
| `HIGH_DEMAND_SUPPLY_RATIO` | `demand_supply_analytics` | ratio ≥ 1.3 | Increase fleet size in this zone |
| `LOW_DRIVER_UTILIZATION` | `ops_kpi_summary` | utilization < 30% (min 3 trips) | Review scheduling / reduce active drivers |
| `HIGH_REVENUE_ZONE` | `revenue_summary_mart` | top 5 zones by total revenue | Consider driver incentives to sustain supply |
| `WEATHER_IMPACT` | `fact_trip` joined to `silver/weather` by city+date | cancellation rate under a condition > 1.3x platform average (min 20 trips) | Warn operations team, consider surge adjustment |
| `TRAFFIC_IMPACT` | `fact_trip` joined to `silver/traffic` by city+zone | avg trip duration in HIGH/SEVERE-congestion zones > 1.25x platform average (min 10 trips) | Predict delays, notify customers proactively |

Every triggered rule produces one row: `rule_name`, `scope_type`
(`ZONE`/`DRIVER`/`PLATFORM`), `scope_value`, `metric_value`,
`recommendation` text, `severity` (`INFO`/`WARNING`/`CRITICAL`).

## Output

- `metadata.recommendations` (Postgres, queryable)
- `data/gold/recommendations` (Parquet, feeds the dashboard's
  recommendations table and Tableau CSV export)

## Why pandas, not Spark

Every mart this reads is already a small, aggregated Gold table -- see
`docs/14_Dashboard.md` for the same reasoning. Using Spark here would add
cluster overhead for tables with a few hundred to a few thousand rows.

## Sample output (from an actual test run)

```
[recommendation_engine] SUCCESS recommendations=47
  [CRITICAL] HIGH_CANCELLATION_RATE -- Bengaluru / Whitefield / hour 23: Deploy more drivers...
  [WARNING] HIGH_DEMAND_SUPPLY_RATIO -- Bengaluru / Electronic City / hour 8h: Demand is outpacing...
  [WARNING] TRAFFIC_IMPACT -- Bengaluru / Koramangala: Heavy traffic congestion is inflating...
  [INFO] HIGH_REVENUE_ZONE -- Bengaluru / Indiranagar: Top revenue-generating zone...
  ...
```

## Tuning

Thresholds are plain module-level constants at the top of
`recommendation_engine.py` (`CANCELLATION_RATE_WARNING`,
`DEMAND_SUPPLY_RATIO_HIGH`, etc.) -- deliberately not made
config/YAML-driven, since these are business judgment calls an analyst
would want to see and adjust directly in the rule they affect, not
abstracted into a separate config file.
