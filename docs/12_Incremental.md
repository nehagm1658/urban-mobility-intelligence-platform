# 12. Incremental Loading

Run with `python3 orchestrator.py --incremental` (applies to Bronze and
Silver only -- Gold, SCD2, recommendations, and the dashboard always
recompute from the current full Silver/Gold state, since they're cheap
enough at this platform's scale that incremental complexity wouldn't pay
for itself there; see `docs/07_Gold.md`).

## Two different incremental strategies, because there are two different
kinds of source

**PostgreSQL source (`customers`) -- true row-level watermark incremental.**
`metadata.watermarks` stores the last-seen `updated_at` value per entity.
Each incremental run pulls only `WHERE updated_at > <last watermark>`,
via `pipelines/bronze_ingestion.py::_read_incremental()`. Verified in
testing: inserting exactly one new customer row and re-running
incrementally picked up exactly that one row, nothing else.

**Flat-file sources (CSV/JSON/XML) -- file-level change detection.**
These are simulated point-in-time file drops with no internal
change-timestamp column to filter on -- there's nothing to filter *within*
a file, since a CSV export doesn't know which of its own rows are "new."
So "incremental" for these means: hash the file (MD5), compare to the
watermark from last time, and **skip the entire file** (no Bronze write,
no downstream reprocessing) if it's unchanged. This is honestly not
row-level CDC and isn't presented as such -- it's the correct incremental
strategy for a source format that genuinely can't support anything finer.

## Idempotency

Every Bronze write goes to `data/bronze/<entity>/load_date=.../batch_id=.../`
-- re-running the same `batch_id` overwrites only that batch's own
partition, never duplicates it. Verified in testing: running
`orchestrator.py --stage bronze --batch-id <same id>` twice in a row left
exactly one partition directory per entity, with matching record counts
both times.

## Silver's incremental upsert

In incremental mode, Silver validates only the new Bronze partitions
since its own watermark, then **upserts by primary key** into the
existing Silver Parquet (`merge_upsert()`): unchanged rows are kept
as-is, rows with a matching primary key in the new batch are replaced,
and genuinely new primary keys are added. This -- not a full
re-cleanse -- is what makes incremental mode actually cheaper than full
mode at scale.

## Verified end-to-end incremental test sequence

```
Run 1 (--incremental, first ever run): full load, watermark set
Run 2 (--incremental, no source changes): every source SKIPPED
Run 3 (--incremental, one new customer row inserted): only customers
       processed, exactly 1 record picked up, watermark advanced
```
All three runs produced the expected `SUCCESS` / `SKIPPED` output with
zero errors -- see `docs/17_Testing_Guide.md` for full console output.
