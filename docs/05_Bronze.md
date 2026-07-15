# 05. Bronze Layer

**Module:** `pipelines/bronze_ingestion.py`

## What it does

Reads every raw source with PySpark (or, for XML, Python's `ElementTree`
adapted into a Spark DataFrame -- see below), stamps ingestion metadata,
and writes an unvalidated, uncast mirror of the source to Parquet. No
business logic runs here -- Bronze answers "what did the source actually
send us," which matters when debugging whether bad data originated
upstream or was introduced by the pipeline.

Metadata columns added to every entity: `_ingestion_timestamp`,
`_source_name`, `_batch_id`.

## Partitioning and idempotency

```
data/bronze/<entity>/load_date=YYYY-MM-DD/batch_id=<batch_id>/
```

Each run writes to its own `batch_id` partition. Re-running the exact
same `batch_id` (e.g. retrying a failed Airflow task) overwrites only
that partition -- it never duplicates data, and it never touches other
batches' partitions. Silver reads the union of every batch partition ever
written (full mode) or only new ones since its last watermark
(incremental mode).

## Resilient parsing

- **JSON (trips):** the source file is JSON Lines format (one JSON object
  per line), read in Spark's `PERMISSIVE` mode with
  `columnNameOfCorruptRecord`. A malformed line lands in a
  `_corrupt_record` column instead of failing the whole file; corrupt
  lines are logged individually to `error.pipeline_errors` as
  `PARSING_ERROR` and excluded, while every well-formed line in the same
  file still flows through.
- **XML (vehicles):** parsed with Python's `ElementTree` (no
  network-dependent `spark-xml` Maven package needed), one `<vehicle>`
  element at a time. A malformed individual record (e.g. an empty
  `<vehicle/>` with no child fields) is caught, logged, and skipped
  without failing the rest of the file. A document that isn't valid XML
  at all still fails the whole ingest -- there's no way to salvage a byte
  stream that doesn't parse as XML.
- **CSV (drivers, payments, traffic, weather, driver_shift):** standard
  Spark CSV reader with header + schema inference. CSV doesn't have a
  natural per-row corruption failure mode the way JSON/XML do, so no
  special handling is needed here.
- **Postgres (customers):** read via `psycopg2` + pandas ->
  `spark.createDataFrame()` (see `docs/02_Architecture.md` for why, not
  Spark JDBC).

## Incremental loading

See `docs/12_Incremental.md` for the full explanation. Summary: Postgres
gets true row-level watermark filtering (`updated_at` column); flat files
get file-level change detection (skip re-ingesting an unchanged file
entirely, via an MD5 hash watermark).

## Sample run output

```
===== Bronze Ingestion | batch_id=2a98df28 | mode=FULL =====
[bronze:drivers] SUCCESS records=1010
[bronze:payments] SUCCESS records=98247
[bronze:trips] SUCCESS records=110275 corrupt_skipped=55
[bronze:vehicles] SUCCESS records=500 malformed_skipped=5
[bronze:customers] SUCCESS records=10030
===== Bronze Ingestion complete | batch_id=2a98df28 =====
```
(actual output from a full enterprise-scale run of this project, ~34s wall time)
