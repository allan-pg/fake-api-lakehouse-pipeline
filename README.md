# Incremental Lakehouse Pipeline

A hands-on data engineering project built to practice **incremental data processing, idempotency, late-arriving data, backfilling, and Lakehouse architecture**.

## Architecture

```text
Fake API
   │
   │ Incremental data generation
   ▼
S3 Landing Zone
   │
   │ Incremental ingestion
   ▼
Databricks
   │
   │ Metadata-driven Lakeflow Pipeline
   ▼
Bronze
   │
   ▼
Silver
   │
   ▼
Gold
```

## Approach

The project uses a **Fake API** to continuously generate synthetic customers, orders, and other datasets. Data is incrementally extracted from the API and stored in an **Amazon S3 landing zone**.

Databricks is connected to S3 through **Unity Catalog** and uses a **metadata-driven Lakeflow Declarative Pipeline** to ingest the data into the Lakehouse.

Instead of creating a separate pipeline for every table, ingestion is driven by configuration metadata:

```text
ingestion_config
       │
       ▼
Generic ingestion pipeline
       │
       ├── customers → Bronze
       ├── orders    → Bronze
       └── ...
```

An ingestion state table stores the **latest successfully processed watermark**, allowing subsequent runs to process only new or changed records.

The pipeline is designed to be **idempotent**, allowing data to be safely reprocessed without creating duplicates.

The project will also explore **late-arriving data, lookback windows, backfilling, schema evolution, data quality, and failure recovery** as the pipeline evolves.

## Goals

* Practice production-style incremental ingestion
* Build metadata-driven pipelines that scale across many datasets
* Understand watermarking and state management
* Implement idempotent processing and upserts
* Practice backfills and late-arriving data
* Build a Bronze/Silver/Gold Lakehouse architecture
* Gain hands-on experience with Databricks, Spark, Delta Lake, S3, and Lakeflow
