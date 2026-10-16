# NYC Taxi Lakehouse: Senior Data Engineering Interview Guide

This guide contains the architectural decisions, trade-offs, and deep-dive technical explanations behind the **NYC Taxi Lakehouse** project. It is structured to help you answer questions confidently in Senior / Lead Data Engineering interviews.

---

## Table of Contents
1. [Architecture & Design Rationale](#1-architecture--design-rationale)
2. [Storage Layer: Delta Lake vs. Raw Parquet](#2-storage-layer-delta-lake-vs-raw-parquet)
3. [Idempotency & Fault Tolerance](#3-idempotency--fault-tolerance)
4. [Data Quality & Quarantine Architecture](#4-data-quality--quarantine-architecture)
5. [Orchestration & Containerization](#5-orchestration--containerization)
6. [Performance Optimization & Benchmarking](#6-performance-optimization--benchmarking)
7. [Enterprise Cloud Mapping (AWS & Azure)](#7-enterprise-cloud-mapping-aws--azure)

---

## 1. Architecture & Design Rationale

### Q: Why did you choose a Medallion Lakehouse Architecture over a traditional Data Warehouse?
**Answer:**
A traditional Data Warehouse (e.g., Snowflake, Redshift) requires loading data directly into proprietary storage formats with upfront schema enforcement. This causes two major issues in enterprise pipelines:
1. **Schema Evolution Fragility:** Unannounced schema changes or upstream payload anomalies cause ingest failures.
2. **Cost & Vendor Lock-in:** Storing raw, multi-terabyte datasets in warehouse compute layers is significantly more expensive than open object storage.

**The Medallion Lakehouse solves this by separating compute from storage:**
- **Bronze (Raw Ingest):** Preserves immutable history and original payloads in open format (Parquet/Delta) with audit metadata (`_ingestion_ts`, `_source_file`). If downstream logic changes, we can reprocess historical data from Bronze without re-querying upstream sources.
- **Silver (Conformed & Validated):** Applies domain cleaning, deduplication, type casting, and schema enforcement. Bad records are routed to a quarantine table rather than killing the batch.
- **Gold (Aggregated Business Layer):** Pre-computes star/snowflake aggregations for sub-second query performance in BI tools (Metabase) and analytics engines (DuckDB).

---

## 2. Storage Layer: Delta Lake vs. Raw Parquet

### Q: Why use Delta Lake instead of plain Parquet files on S3/MinIO?
**Answer:**
While Parquet is an outstanding columnar format, it lacks ACID guarantees when written across distributed nodes:
1. **ACID Transactions & Atomicity:** A distributed Spark job writing 100 Parquet part files can fail halfway through. With plain Parquet, downstream readers will see corrupted or partial data. Delta uses an atomic transaction log (`_delta_log/`), committing files only when the entire job succeeds.
2. **Idempotent Overwrites (`dynamic` partition overwrite):** When re-running a batch for a specific date, Delta replaces *only* the matching partition without locking or wiping unrelated partitions.
3. **Time Travel & Auditing:** Delta log versions enable rollback to earlier snapshots (`VERSION AS OF` or `TIMESTAMP AS OF`) for debugging regressions.
4. **Metadata Caching & Statistics Skipping:** Delta stores column min/max stats in the log, allowing Spark's Catalyst optimizer to skip entire Parquet files without reading footers.

### Q: Why did you export Gold tables to plain Parquet for DuckDB/Metabase instead of reading Delta directly?
**Answer:**
In modern production pipelines, decoupling the storage layer from downstream analytics is critical. During implementation, DuckDB's `delta_scan` extension relied on the Rust DeltaKernel FFI, which had hardcoded AWS EC2 metadata polling and ignored custom S3 endpoints. Furthermore, querying raw Delta directories via standard file globbing (`read_parquet('*.parquet')`) risks reading tombstoned files from past overwrite commits.

**Engineering Decision:**
At the end of the Gold stage, Spark writes clean single-partition snapshots to `s3a://gold/export/`. DuckDB reads these via native `httpfs` in milliseconds with zero metadata polling issues, 100% S3/MinIO compatibility, and guaranteed duplicate-free results.

---

## 3. Idempotency & Fault Tolerance

### Q: How do you guarantee idempotency across your pipeline?
**Answer:**
A pipeline is idempotent if running it $N$ times produces the exact same state as running it once, without duplicating records or corrupting state. We guarantee idempotency at every stage:
1. **Ingestion & S3 Upload:** The download and upload scripts check for file existence and MD5/size hashes before downloading or re-uploading (`scripts/upload_to_minio.py`).
2. **Bronze Layer:** Spark uses `mode("overwrite")` with schema enforcement.
3. **Silver Layer:** 
   - Primary key deduplication: `row_number() OVER (PARTITION BY VendorID, tpep_pickup_datetime, PULocationID, DOLocationID ORDER BY _ingestion_ts DESC) == 1`.
   - `spark.sql.sources.partitionOverwriteMode = dynamic`: Spark overwrites only the specific `pickup_date` partitions present in the incoming batch.
4. **Gold Layer:** Overwrite mode on daily, hourly, and zone aggregations.
5. **Verification Suite:** `scripts/verify_idempotency.py` runs a back-to-back full pipeline execution and asserts `Diff == 0` across all tables.

---

## 4. Data Quality & Quarantine Architecture

### Q: Why quarantine invalid data instead of failing the pipeline immediately?
**Answer:**
In mission-critical production pipelines (e.g., millions of taxi trips, financial transactions, or clickstream events), a small fraction of records will always contain anomalies (e.g., negative passenger counts, GPS sensor errors, future pickup dates).
- **If you fail the pipeline:** Executive dashboards go stale, business operations stall, and downstream SLAs are violated for 99.9% of good data due to 0.1% anomalies.
- **If you silently drop records:** Data loss occurs, and upstream source teams never get notified to fix data quality bugs.

**The Quarantine Pattern:**
1. Records are evaluated against explicit validation rules:
   - Positive passenger count ($> 0$)
   - Valid trip distance ($0.1 \le \text{distance} \le 500$ miles)
   - Positive fare amount ($> \$0$)
   - Reasonable timestamp bounds (within Jan-Feb 2024; drop future/historical outlier timestamps)
   - Valid pickup & dropoff location IDs ($1 \le \text{ID} \le 265$)
2. Clean records pass to `s3a://silver/yellow_taxi`.
3. Failed records are tagged with a human-readable `reject_reason` and routed to `s3a://silver/yellow_taxi_quarantine`.
4. Downstream dbt models run 16 automated assertions against clean data, while alerting engineers if the quarantine rate exceeds threshold bounds.

---

## 5. Orchestration & Containerization

### Q: Why Airflow DockerOperator / DOOD over PythonOperator?
**Answer:**
In modern enterprise data platforms, orchestrators should **never** execute heavy compute inside their own worker processes:
1. **Dependency Isolation:** Airflow runs Python 3.10/3.11 with its own dependencies. Spark requires Java, Hadoop binaries, and Delta jars. dbt requires its own adapter libraries. Combining them creates dependency hell.
2. **Resource Boundaries:** Spark needs 2 GB RAM; Airflow needs 1.5 GB. Using Docker-out-of-Docker (DOOD) via `/var/run/docker.sock`, Airflow acts purely as a coordinator, triggering isolated Docker containers that are bounded by Docker memory limits (`mem_limit`).
3. **Reproducibility:** Upgrading a Spark jar or Python package never breaks Airflow's core scheduler.

---

## 6. Performance Optimization & Benchmarking

### Q: What did your Spark performance benchmarks prove?
**Answer:**
Using `scripts/spark_benchmark.py`, we executed reproducible empirical tests against the 5.9M record dataset:

| Optimization | Comparison | Technical Mechanism |
| :--- | :--- | :--- |
| **Columnar Storage** | Raw CSV vs. Delta/Parquet | Column pruning reads only queried attributes; binary serialization eliminates string parsing. |
| **Partition Pruning** | Unpartitioned vs. Partitioned by `pickup_date` | Catalyst optimizer prunes non-matching directory trees, skipping 95% of data files on range queries. |
| **In-Memory Caching** | Without Cache vs. `.cache()` in RAM | Eliminates redundant DAG re-evaluation and repeated S3 network I/O across multi-pass aggregations. |

---

## 7. Enterprise Cloud Mapping (AWS & Azure)

### Q: How does this local lakehouse translate to AWS or Azure in production?

| Component | Local Lakehouse Architecture | AWS Enterprise Equivalent | Azure Enterprise Equivalent |
| :--- | :--- | :--- | :--- |
| **Object Storage** | MinIO (S3 API) | **Amazon S3** (Lifecycle tiers) | **Azure Data Lake Storage (ADLS) Gen2** |
| **Compute Engine** | PySpark 3.5.1 on Docker | **AWS EMR** / **AWS Glue 4.0** | **Azure Databricks** / **Azure Synapse Spark** |
| **Table Format** | Delta Lake 3.2.0 | **Delta Lake** / **Apache Iceberg** | **Delta Lake** (Native Databricks runtime) |
| **Orchestration** | Apache Airflow 2.9.2 (DOOD) | **Amazon MWAA** (Managed Airflow) | **Azure Data Factory (ADF)** / Managed Airflow |
| **Data Quality & Modeling** | dbt-core + DuckDB | **dbt Cloud** / **dbt-core on ECS** | **dbt Cloud** / **dbt-core on Azure Container Instances** |
| **Relational Serving** | PostgreSQL 15 | **Amazon RDS PostgreSQL** / **Aurora** | **Azure Database for PostgreSQL** |
| **BI & Analytics** | Metabase v0.49 | **Amazon QuickSight** / Metabase on ECS | **Power BI Service** |
| **Metadata Catalog** | Delta Log + DuckDB Catalog | **AWS Glue Data Catalog** | **Microsoft Purview** / Unity Catalog |
