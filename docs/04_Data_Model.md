# 04. Data Model

## Source -> Bronze -> Silver -> Gold column lineage summary

This is a summary; see `docs/16_Data_Lineage.md` for full column-level
lineage of every Gold table.

| Entity | Source format | Bronze | Silver | Gold |
|---|---|---|---|---|
| drivers | CSV | raw mirror + metadata | validated, cleansed, city_tier added, SCD1 | `dim_driver` (current), `dim_driver_scd2` (history) |
| customers | PostgreSQL (`source.customers`) | raw mirror + metadata | validated, deduplicated, city_tier added | `dim_customer` |
| vehicles | XML | raw mirror + metadata (malformed records dropped at Bronze) | validated, cleansed | `dim_vehicle` |
| trips | JSON Lines | raw mirror + metadata (corrupt lines dropped at Bronze) | validated, cleansed, city_tier + trip_duration_minutes added | `fact_trip`, and every KPI mart |
| payments | CSV | raw mirror + metadata | validated, cleansed | joined into `fact_trip` |
| driver_shift | CSV | raw mirror + metadata | validated, cleansed | `ops_kpi_summary` (via online_hours) |
| traffic | CSV | raw mirror + metadata | validated, cleansed | `recommendation_engine.py` (traffic-impact rule) |
| weather | CSV | raw mirror + metadata | validated, cleansed | `recommendation_engine.py` (weather-impact rule) |

## Why Silver revalidates every field type explicitly

Bronze intentionally does **no** type casting -- CSV/JSON/XML sources are
read as close to "raw" as PySpark's inference allows, so Bronze is a
faithful mirror of the source (useful when debugging "was this bad data
already bad at the source, or did our pipeline break it?"). All real type
casting (`driver_id` to `IntegerType`, `fare_amount` to `DoubleType`,
timestamps to `TimestampType`) happens explicitly in Silver's
`enrich_*()` functions, immediately after validation, so a value that
fails to cast is caught by the `INVALID_DATA_TYPE:<column>` rejection
reason rather than silently becoming `null` upstream.

## Grain of each Gold table

| Table | Grain |
|---|---|
| `dim_driver` | one row per driver (current state) |
| `dim_driver_scd2` | one row per driver per version (full history) |
| `dim_customer` | one row per customer |
| `dim_vehicle` | one row per vehicle |
| `fact_trip` | one row per trip |
| `revenue_summary_mart` | one row per (trip_date, city, pickup_zone) |
| `ops_kpi_summary` | one row per driver |
| `cancellation_analytics` | one row per (city, pickup_zone, hour_of_day) |
| `demand_supply_analytics` | one row per (city, pickup_zone, hour_of_day) |
| `recommendations` | one row per triggered rule instance |
