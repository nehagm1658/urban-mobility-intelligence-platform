# 18. Project Demo & Interview Questions

## 60-second demo script

1. `python3 scripts/generate_mock_data.py` -- generates enterprise-scale
   Bengaluru mock data with deliberate data-quality problems.
2. `python3 orchestrator.py` -- runs Bronze -> Silver -> Gold -> SCD2 ->
   Recommendations -> Dashboard, prints a batch summary table at the end.
3. Open `data/dashboard/dashboard.html` -- show the revenue trend,
   cancellation %, and recommendations table.
4. `SELECT entity, failure_reason, COUNT(*) FROM error.rejected_records
   GROUP BY 1,2;` -- show real rejected records with real reasons.
5. Show `data/gold/dim_driver_scd2` for one driver with 2+ versions --
   demonstrate SCD Type 2 history.
6. `python3 orchestrator.py --incremental` a second time with no source
   changes -- show every stage printing `SKIPPED`, demonstrating
   idempotent incremental loading.

## Likely interview questions and how this project answers them

**"Walk me through your architecture."**
Medallion (Bronze/Silver/Gold), see `docs/02_Architecture.md`. Emphasize:
Bronze is a raw mirror with metadata only, no business logic; Silver is
where validation and type casting happen (deliberately, one place, not
scattered); Gold is where joins/aggregations/marts live, consumed by both
a recommendation engine and a dashboard.

**"How do you handle bad data?"**
Point to the fixed-priority validation chain in `silver_cleansing.py`
(`docs/06_Silver.md`) and `error.rejected_records` (`docs/10_ErrorHandling.md`)
-- every rejected record keeps its full original JSON and a specific
reason, nothing is silently dropped.

**"How would this scale to 10x the data?"**
Validation already runs as native Spark expressions across the cluster,
not a Python loop (this was an actual rewrite made during development --
see `docs/17_Testing_Guide.md` for the before/after). The remaining
bottleneck at much larger scale would be the Postgres reads/writes
(psycopg2 + pandas, not Spark JDBC) -- explain the JDBC tradeoff in
`docs/02_Architecture.md` and what you'd change.

**"How do you avoid reprocessing data you've already processed?"**
Batch-scoped Bronze partitions (`load_date=.../batch_id=...`) make
re-running the same batch idempotent by construction, and `--incremental`
mode with watermarks (`docs/12_Incremental.md`) avoids reprocessing
unchanged sources at all. Both were tested, not just designed -- see
`docs/17_Testing_Guide.md`.

**"What's the difference between your SCD1 and SCD2 implementations, and
why do you have both?"**
See `docs/11_SCD.md` -- SCD1 (drivers, always-current) and SCD2 (driver
history) read the *same* Silver output but answer different questions.
Be ready to explain the `rating_bucket` rounding decision (avoiding
version churn from noisy trip-to-trip rating changes).

**"Why isn't your recommendation engine using machine learning?"**
Deliberate choice -- explainability. Every rule in
`docs/15_Recommendation_Engine.md` is a threshold an operations analyst
can read and immediately understand/challenge, versus a model whose
reasoning would need to be separately explained. Be ready to say what
you'd add ML for if asked (e.g., demand forecasting) and why that's a
different problem than "should we alert on this zone right now."

**"What would you do differently / what's not finished?"**
Have a real, specific answer ready -- see `README.md`'s "Known
simplifications" and `docs/17_Testing_Guide.md`'s "Known gaps." Don't
claim things that weren't verified (e.g., a live Airflow run inside
Docker) were tested when they weren't -- explain exactly what was and
wasn't exercised, and why.

**"Show me a bug you actually hit while building this."**
Three real ones are documented with fixes in
`docs/17_Testing_Guide.md` #7 -- the Spark `_corrupt_record` filter
restriction, the self-overwrite-same-path hazard in the incremental
upsert path, and the empty-DataFrame schema inference failure. All three
are the kind of "gotcha" a real Spark/Postgres pipeline runs into, and
having concrete examples (not hypotheticals) is a strong interview
signal.

## Future improvements (honest list, not padding)

- Cross-record validation (driver shift overlap) -- see
  `docs/17_Testing_Guide.md`
- Spark JDBC for Postgres reads instead of psycopg2 + pandas, if/when
  Maven access is available
- A Great Expectations suite wrapping `validate_and_split()`'s rules
- `pytest` unit tests (see `docs/17_Testing_Guide.md`'s suggested list)
- A real live Airflow run inside Docker, and a Docker Compose run of the
  whole stack, in an environment with a Docker daemon available
