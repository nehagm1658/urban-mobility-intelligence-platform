# 06. Silver Layer

**Module:** `pipelines/silver_cleansing.py`

## Validation engine

Every entity runs through `validate_and_split()`, which checks (in this
fixed priority order -- a record can only be rejected for the *first*
reason it fails):

1. `NULL_PRIMARY_KEY` -- primary key is null or blank
2. `DUPLICATE_PRIMARY_KEY` -- primary key seen more than once in this
   batch (first occurrence, by row-insertion order, is kept)
3. `NULL_REQUIRED_FIELD:<column>` -- one per required column
4. Numeric/business-rule checks (e.g. `NEGATIVE_FARE`,
   `RATING_OUT_OF_RANGE`, `INVALID_PAYMENT_AMOUNT`), or
   `INVALID_DATA_TYPE:<column>` if the value can't even be cast to a
   number
5. Custom cross-column checks (currently: `MISMATCHED_TRIP_TIMESTAMPS`,
   drop_time before request_time)

**This runs as native Spark column expressions (`coalesce()` over a list
of independently-evaluated candidate-reason columns), not a Python loop
over `df.collect()`.** Only rejected rows (typically a low single-digit
percentage of a batch) are ever collected to the driver, to serialize
their JSON for `error.rejected_records`; valid rows stay in Spark end to
end and are written directly. See `docs/17_Testing_Guide.md` for the
before/after performance comparison at 100k+ rows.

## Cleansing & enrichment

After validation, each entity gets its own `enrich_*()` transform:
explicit type casting (Bronze deliberately does none), string
trimming/uppercasing, and a `city_tier` lookup enrichment (Bengaluru/
Mumbai/Delhi/Hyderabad/Chennai -> `Tier-1`) joined onto drivers,
customers, and trips.

## SCD Type 1 (drivers)

`enrich_drivers_scd1()` overwrites `data/silver/drivers` with the latest
cleansed snapshot on every run -- old attribute values are not retained
here. This is what "SCD Type 1" means: always-current, no history. Full
history for the same dimension is kept separately by
`pipelines/scd_type2.py` (see `docs/11_SCD.md`), which reads this same
Silver output.

## Incremental upsert

In `--incremental` mode, Silver reads only Bronze partitions newer than
its own watermark, validates just those, and **upserts** (by primary key)
into the existing Silver Parquet rather than overwriting the whole table
-- see `merge_upsert()` and `docs/12_Incremental.md`.

## A hazard worth knowing about: reading and overwriting the same path

`merge_upsert()` (and `scd_type2.py`'s writer) both read an existing
Parquet path and then need to write an updated version back to that same
path. Writing directly over a path Spark's lazy execution plan still
references causes files to be deleted out from under a still-running read
task. Both writers avoid this by writing to a temp path first, then
atomically swapping it into place. This is a real bug that was caught and
fixed during testing of this project (see the git-style before/after in
`docs/17_Testing_Guide.md`), not a hypothetical -- worth knowing if you
extend either module.

## Sample run output (enterprise-scale, ~130k rows across trips+payments)

```
[silver:drivers] SUCCESS valid=974 rejected=36
[silver:customers] SUCCESS valid=10000 rejected=30
[silver:trips] SUCCESS valid=107729 rejected=2546
[silver:payments] SUCCESS valid=96200 rejected=2047
===== Silver Cleansing complete =====
```
(~73s wall time for the whole Silver layer at this scale)
