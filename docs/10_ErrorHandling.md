# 10. Error Handling

Two separate error tables, for two separate kinds of failure:

## `error.rejected_records` -- individual bad records

One row per record that failed a Silver validation rule.

| Column | Purpose |
|---|---|
| `batch_id`, `entity` | Which run, which entity |
| `record_pk` | The primary key value (as text; may be null if the PK itself was the problem) |
| `failure_reason` | e.g. `NEGATIVE_FARE`, `NULL_PRIMARY_KEY`, `DUPLICATE_PRIMARY_KEY` -- see `docs/06_Silver.md` for the full list |
| `raw_record_json` | The complete original record, as JSON, so you can see exactly what was rejected without re-deriving it from Bronze |

Written by `pipelines/common.py`'s `reject_records_batch()`, which uses
`psycopg2.extras.execute_values` to insert every rejected record for a
given reason in one batched round trip, not one `INSERT` per row. At
enterprise scale (silver rejected ~4,600 rows across trips+payments in
one run during testing) this is the difference between a couple of
seconds and potentially minutes.

## `error.pipeline_errors` -- pipeline-level exceptions

One row per exception the pipeline itself hit (not a single bad record --
something that broke the run): a file that couldn't be parsed at all, a
Postgres connection failure, an unexpected schema. Categorized by
`error_type`: `PARSING_ERROR`, `SCHEMA_ERROR`, `TRANSFORMATION_ERROR`,
`DATABASE_ERROR`.

## Retry behavior

Every Postgres write in `pipelines/common.py` (log writes, watermark
writes, rejected-record writes) is wrapped in a `@retry` decorator: up to
3 attempts with a short backoff, for transient failures like a momentary
connection drop. This is deliberately narrow -- it retries *writing the
log*, not *re-running business logic that already failed*. A record that
failed validation stays failed; retrying that would be silently hiding a
real data quality problem, not fixing a transient blip.

## Query: what's failing right now?

```sql
SELECT entity, failure_reason, COUNT(*) AS n
FROM error.rejected_records
WHERE batch_id = '<latest batch_id>'
GROUP BY 1, 2
ORDER BY n DESC;

SELECT pipeline_name, error_type, error_message, occurred_at
FROM error.pipeline_errors
ORDER BY occurred_at DESC
LIMIT 20;
```
