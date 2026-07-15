# 01. Business Requirements

## Problem

A ride-sharing platform (modeled on Uber/Ola operating in Bengaluru, with
Mumbai and Hyderabad as secondary cities) needs a data platform that can:

1. Ingest operational data from multiple, independently-owned source
   systems (a driver/payments/traffic/weather CSV export, a trips event
   feed in JSON, a legacy vehicle-registry system exporting XML, and a
   live customer database) without every source needing to agree on a
   common format.
2. Guarantee that bad data (nulls, duplicates, out-of-range values,
   inconsistent cross-field data) never silently corrupts downstream
   analytics -- every rejected record must be traceable to a specific
   reason.
3. Answer operational questions daily: Where are we losing trips to
   cancellations? Which zones are under-supplied with drivers? Which
   drivers are under-utilized? Which zones generate the most revenue?
   Is weather or traffic measurably hurting the platform right now?
4. Keep a full history of driver attribute changes (city, status, rating
   tier) for audit and "what did we know about this driver on date X"
   questions -- not just the latest snapshot.
5. Scale to enterprise data volumes (100k+ trips/payments, 10k+
   customers) without falling over, and re-run safely without
   duplicating data if a job is retried.

## Users

- **Operations analysts** -- read the dashboard daily, act on
  recommendations (deploy more drivers, adjust incentives).
- **Data engineers** -- own the pipeline, investigate rejected records,
  extend the platform with new sources/marts.
- **Interviewers evaluating this PoC** -- want to see the medallion
  pattern, validation, SCD, incremental loading, and orchestration done
  correctly and explainably, not necessarily every possible enterprise
  feature.

## In scope for this PoC

- Bronze/Silver/Gold medallion architecture with PySpark
- Multi-format ingestion (CSV, JSON, XML, PostgreSQL)
- Schema/null/PK/duplicate/business-rule validation with a full audit
  trail of rejections
- SCD Type 1 (drivers, always-current) and SCD Type 2 (drivers, full
  history)
- Incremental loading with watermarks (Postgres source) and file-level
  change detection (flat-file sources)
- Idempotent, batch_id-scoped execution safe to retry
- Rule-based (non-ML) operational recommendations
- An executive Plotly dashboard + Tableau-compatible exports
- Airflow DAG orchestration alongside a plain-Python orchestrator
- Full metadata/audit/error logging in Postgres

## Explicitly out of scope

- Real-time/streaming ingestion (this is batch, run daily)
- Machine learning demand forecasting (the recommendation engine is
  deliberately rule-based and explainable)
- Multi-tenant / multi-region deployment
- Authentication/authorization on the dashboard (single-analyst PoC)
