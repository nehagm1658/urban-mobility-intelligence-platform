# 08. Metadata

**Table:** `metadata.pipeline_runs` (one row per pipeline stage per entity
per run)

| Column | Purpose |
|---|---|
| `batch_id` | Ties every layer's run for one execution together (same batch_id flows through bronze -> silver -> gold -> scd2 -> recommendations) |
| `pipeline_name`, `layer`, `entity` | What ran |
| `execution_start`, `execution_end`, `duration_seconds` | Timing |
| `record_count`, `insert_count`, `update_count`, `reject_count` | Volume |
| `status` | `SUCCESS` / `FAILED` |
| `error_message` | Populated on failure |
| `execution_host`, `pipeline_version` | Where/what version ran (useful once you have more than one worker) |

Also: `metadata.watermarks` (incremental loading state, see
`docs/12_Incremental.md`), `metadata.scd2_change_log` (SCD2 audit trail,
see `docs/11_SCD.md`), `metadata.recommendations` (recommendation engine
output, see `docs/15_Recommendation_Engine.md`).

## Why this matters

Every one of these tables is written by `pipelines/common.py`'s
`log_pipeline_run()` -- a single shared function every pipeline stage
calls, rather than each module reimplementing its own logging (which is
what this project's earlier baseline did, with three near-identical
copies of the same function). One shared implementation means a schema
change or a bug fix here applies everywhere at once.

## Query: "what happened in my last run?"

```sql
SELECT layer, entity, record_count, reject_count, status, duration_seconds
FROM metadata.pipeline_runs
WHERE batch_id = (SELECT batch_id FROM metadata.pipeline_runs ORDER BY created_at DESC LIMIT 1)
ORDER BY run_id;
```
