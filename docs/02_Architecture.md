# 02. Architecture

## Medallion architecture, end to end

```mermaid
flowchart TD
    subgraph Sources
        CSV["CSV: drivers, payments,<br/>traffic, weather, driver_shift"]
        JSONL["JSON Lines: trips"]
        XML["XML: vehicles"]
        PG["PostgreSQL: source.customers"]
    end

    subgraph Bronze["Bronze -- pipelines/bronze_ingestion.py"]
        B1["Raw mirror + ingestion metadata<br/>(_batch_id, _source_name, _ingestion_timestamp)<br/>Partitioned by load_date/batch_id<br/>Resilient JSON/XML parsing<br/>Incremental: watermark (PG) / file-hash (flat files)"]
    end

    subgraph Silver["Silver -- pipelines/silver_cleansing.py"]
        S1["Vectorized Spark validation<br/>(null/PK/dup/business-rule)"]
        S2["Cleansing + standardization<br/>+ city_tier lookup"]
        S3["SCD Type 1 (drivers)"]
        S4["Incremental upsert by PK"]
    end

    subgraph Gold["Gold -- pipelines/gold_transformations.py + scd_type2.py"]
        G1["Dimensions: dim_driver, dim_customer, dim_vehicle"]
        G2["Fact: fact_trip"]
        G3["KPI marts: revenue_summary_mart,<br/>ops_kpi_summary, cancellation_analytics,<br/>demand_supply_analytics"]
        G4["dim_driver_scd2<br/>(full driver history)"]
    end

    subgraph Consumption
        R["recommendation_engine.py<br/>rule-based ops recommendations"]
        D["dashboard.py<br/>Plotly HTML + Tableau CSVs"]
    end

    subgraph PG_Meta["PostgreSQL: metadata / audit / error schemas"]
        M["pipeline_runs, watermarks,<br/>scd2_change_log, recommendations"]
        A["audit_log"]
        E["rejected_records, pipeline_errors"]
    end

    CSV --> B1
    JSONL --> B1
    XML --> B1
    PG --> B1
    B1 --> S1 --> S2 --> S3 --> S4
    S4 --> G1
    S4 --> G2
    G1 --> G3
    G2 --> G3
    S4 --> G4
    G3 --> R
    G2 --> R
    R --> D
    G3 --> D

    Bronze -.logs.-> PG_Meta
    Silver -.logs + rejects.-> PG_Meta
    Gold -.logs.-> PG_Meta
```

## Orchestration

Two equivalent ways to run the same pipeline code:

```mermaid
flowchart LR
    subgraph Manual["orchestrator.py (manual / cron)"]
        O1[bronze] --> O2[silver] --> O3[gold] --> O4[scd2] --> O5[recommendations] --> O6[dashboard]
    end
    subgraph Airflow["dags/umip_dag.py (scheduled, retries, alerting)"]
        A1[bronze_ingestion] --> A2[silver_cleansing] --> A3[gold_transformations] --> A4[scd_type2_driver_dimension] --> A5[recommendation_engine] --> A6[dashboard_refresh]
    end
```

Both call the exact same pipeline modules -- Airflow's BashOperator tasks
literally run `python3 orchestrator.py --stage <name>`. There is only one
implementation of the pipeline logic; Airflow just adds scheduling,
retries, and failure alerting on top.

## Why PySpark, not plain Pandas

Bronze and Silver operate on the platform's largest tables (110k+ raw
trips, 98k+ payments) where a distributed, lazily-evaluated engine matters
for both correctness (schema enforcement, window functions for duplicate
detection) and performance (validation runs as Spark column expressions,
not a Python loop -- see `docs/17_Testing_Guide.md` for the before/after).
Gold's KPI marts and the recommendation engine / dashboard operate on much
smaller, already-aggregated tables (hundreds to low thousands of rows),
where pandas is simpler and just as fast -- using Spark there would only
add cluster startup overhead for no benefit. This project deliberately
uses each tool where it earns its complexity, not uniformly everywhere.

## Why psycopg2 + pandas instead of Spark JDBC for Postgres

`config.read_postgres_table()` pulls Postgres tables into a pandas
DataFrame, then hands that to `spark.createDataFrame()`. The "correct"
enterprise approach is `spark.read.jdbc(...)` with the PostgreSQL JDBC
driver -- but fetching that driver requires Maven Central access, which
isn't available in every environment (including the one this project was
built and tested in). The psycopg2 + pandas path works everywhere and is
fine at the platform's data volumes (thousands to tens of thousands of
rows); swap it for JDBC if you need true distributed reads from Postgres
at much larger scale.
