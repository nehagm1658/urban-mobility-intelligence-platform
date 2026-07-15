# 14. Dashboard

**Module:** `pipelines/dashboard.py`

## Outputs (all written to `data/dashboard/`)

1. **`dashboard.html`** -- one self-contained interactive Plotly
   dashboard. Opens in any browser, no server needed (Plotly's JS is
   pulled from a CDN).
2. **`*.png`** -- a static snapshot of every chart, for slides/README
   embedding. Requires the `kaleido` package plus a local Chrome/Chromium
   -- if unavailable, `export_png_snapshots()` fails soft (logs a
   warning, returns `False`) and the HTML/CSV outputs still generate
   normally. This was tested in an environment without Chrome available:
   the dashboard completed successfully with PNG export skipped, exactly
   as designed.
3. **`*.csv`** -- one Tableau/Excel/Power BI-compatible CSV per Gold mart
   (`revenue_summary_mart`, `ops_kpi_summary`, `demand_supply_analytics`,
   `cancellation_analytics`, `recommendations`) -- flat, already-aggregated
   tables any BI tool can load directly with no transformation needed.

## Sections covered

| Chart | Source mart | Answers |
|---|---|---|
| Revenue trend | `revenue_summary_mart` | Is revenue growing/shrinking day over day? |
| Zone performance | `revenue_summary_mart` | Which 15 zones generate the most revenue? |
| Driver utilization | `ops_kpi_summary` | How is utilization distributed across the driver base? |
| Demand vs supply | `demand_supply_analytics` | Which zone/hour combinations are most under-supplied? |
| Peak hours | `fact_trip` | When does trip volume peak during the day? |
| Cancellation % | `cancellation_analytics` | Which cities have the worst cancellation rates? |
| Top drivers | `ops_kpi_summary` | Who are the top 10 revenue-generating drivers? |
| Pipeline health | `metadata.pipeline_runs` | Are recent pipeline runs succeeding, and how long are they taking? |
| Recommendations | `metadata.recommendations` | What should operations act on today? |

## Why pandas/Plotly, not Spark, for this module

Every input here is a small, already-aggregated Gold table (hundreds to a
few thousand rows) -- the heavy distributed computation already happened
in Bronze/Silver/Gold. Reading these with Spark for a reporting step
would add cluster startup overhead for zero benefit; pandas is simpler
and just as fast at this size. Same reasoning as the recommendation
engine (`docs/15_Recommendation_Engine.md`).

## Running it

```bash
python3 orchestrator.py --stage dashboard   # after at least one full gold + recommendations run
# or, standalone:
python3 pipelines/dashboard.py
```
