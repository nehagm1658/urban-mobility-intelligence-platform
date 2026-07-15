# 11. SCD (Slowly Changing Dimensions)

The platform implements **both** SCD Type 1 and Type 2 for the same
dimension (drivers) -- they're not alternatives, they're two different
consumers of the same clean Silver data, answering two different
questions.

## SCD Type 1 -- `pipelines/silver_cleansing.py::enrich_drivers_scd1()`

Always shows the *current* driver attributes. Every Silver run overwrites
`data/silver/drivers` (and, downstream, `data/gold/dim_driver`) entirely
with the latest cleansed snapshot. No history is kept here -- if a
driver's city changes, the old city is simply gone from this table.
This is the right model for "what is this driver's city right now,"
which is most queries.

## SCD Type 2 -- `pipelines/scd_type2.py`

Answers "what was this driver's city/status/rating tier **as of any
point in time**." Tracked columns: `city`, `status`, `rating_bucket`
(rating rounded to the nearest 0.5, so trip-to-trip rating noise doesn't
create a new version every run -- only meaningful changes do).

Output: `data/gold/dim_driver_scd2`, with `effective_start_date`,
`effective_end_date`, `is_current`, `version_number`. A change to any
tracked attribute expires the current row (`is_current = false`,
`effective_end_date` set) and inserts a new version
(`version_number + 1`, `is_current = true`, `effective_end_date = NULL`).

Every attribute-level change is also logged to
`metadata.scd2_change_log` (old value, new value, old/new version) --
a queryable audit trail of *why* a new version exists, not just that one
does.

## Worked example (from an actual test run of this project)

Driver 1 started as `city=Bengaluru, status=SUSPENDED`. After simulating
a source change to `status=INACTIVE`:

```
driver_id | driver_name  | city   | status    | version | is_current | effective_end_date
1         | Matthew Rana | Mumbai | SUSPENDED  | 1       | false      | 2026-07-08 14:53:04
1         | Matthew Rana | Mumbai | INACTIVE   | 2       | true       | NULL
```
```sql
-- metadata.scd2_change_log
driver_id | changed_column | old_value | new_value | old_version | new_version
1         | status          | SUSPENDED | INACTIVE  | 1           | 2
```

Re-running `scd_type2.py` again with no further changes produced `new=0
changed=0` -- confirmed idempotent, no phantom versions created on a
no-op re-run.

## Why not just SCD Type 2 everywhere?

Every other Gold dimension (`dim_customer`, `dim_vehicle`) is SCD Type 1
only, on purpose. Full history is expensive to maintain and query, and
the platform doesn't currently have a business question that needs
"what was this customer's phone number 3 months ago." Driver attributes
are the one place a real question exists (audit/compliance-style "what
was this driver's status when this trip happened"), so that's the one
dimension that gets the extra complexity.
