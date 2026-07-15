# 09. Audit

**Table:** `audit.audit_log`

A second, complementary log to `metadata.pipeline_runs`, written by the
same `log_pipeline_run()` call in `pipelines/common.py` (one function
call, two tables -- they're always consistent with each other since
there's no separate audit-writing code path to drift out of sync).

| Column | Purpose |
|---|---|
| `batch_id`, `pipeline_name`, `layer` | Same identifiers as `metadata.pipeline_runs` |
| `processed_records`, `rejected_records`, `inserted_records`, `updated_records` | Volume, mirrored from the same run |
| `execution_duration_sec` | Timing |
| `pipeline_status` | `SUCCESS` / `FAILED` |
| `logged_at` | Wall-clock time the row was written |

## Why two tables instead of one

`metadata.pipeline_runs` is the primary operational table (indexed by
`batch_id` and `entity`, used for retries/idempotency checks via
`common.batch_already_processed()`). `audit.audit_log` is a simpler,
append-only ledger meant for compliance-style questions ("show me
everything that touched customer data in the last 90 days") without
needing to understand the full pipeline_runs schema. In a larger
deployment these would likely have different retention policies and
access controls, which is the real reason to keep them as separate
tables rather than one.

## Query: daily rejection rate trend

```sql
SELECT DATE(logged_at) AS day, layer,
       SUM(rejected_records)::float / NULLIF(SUM(processed_records), 0) AS reject_rate
FROM audit.audit_log
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```
