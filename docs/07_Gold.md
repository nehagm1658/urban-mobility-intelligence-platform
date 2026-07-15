# 07. Gold Layer

**Modules:** `pipelines/gold_transformations.py`, `pipelines/scd_type2.py`

## Dimensions

- `dim_driver` -- current driver attributes (from Silver's SCD1 output)
- `dim_driver_scd2` -- full driver history, see `docs/11_SCD.md`
- `dim_customer`, `dim_vehicle` -- straightforward cleansed dimensions

## Fact

- `fact_trip` -- one row per trip, joined with payments (left join --
  cancelled trips or trips missing a payment record still appear, with
  null payment columns; see the "completed ride without payment" /
  "cancelled ride with payment" data-quality patterns injected by
  `generate_mock_data.py`), enriched with `hour_of_day` and
  `trip_time_bucket` for time-based analytics.

## KPI marts

- `revenue_summary_mart` -- daily revenue by city/zone
- `ops_kpi_summary` -- per-driver utilization (`online_hours` from
  `driver_shift` vs. actual trip time), revenue per driver
- `cancellation_analytics` -- cancellation rate by city/zone/hour
- `demand_supply_analytics` -- demand-vs-active-drivers ratio by
  city/zone/hour, with a `shortage_rank_in_city` window function

All Gold writes are **full overwrite** on every run (not incremental) --
Gold recomputes from the current full Silver state each time, which is
simple to reason about and cheap enough at this platform's data volumes
(Gold builds run in well under a minute end to end even at 100k+ trips).
Only Bronze and Silver support `--incremental` mode.

## SCD Type 2 (`scd_type2.py`)

Runs after Gold, reads Silver's `drivers` table (the same SCD1-cleansed
snapshot `dim_driver` also reads), and maintains
`data/gold/dim_driver_scd2` with full version history. See
`docs/11_SCD.md` for the detailed mechanics and a worked example.

## Sample run output (enterprise scale)

```
[gold:dim_driver] SUCCESS records=974
[gold:dim_customer] SUCCESS records=10000
[gold:dim_vehicle] SUCCESS records=500
[gold:fact_trip] SUCCESS records=107912
[gold:revenue_summary_mart] SUCCESS records=752
[gold:ops_kpi_summary] SUCCESS records=1000
[gold:cancellation_analytics] SUCCESS records=384
[gold:demand_supply_analytics] SUCCESS records=384
===== Gold Transformations complete ===== (~39s wall time)
```
