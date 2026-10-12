"""
load_gold_to_postgres.py
Syncs Lakehouse Gold analytics tables and pipeline audit logs from MinIO S3
into Postgres using DuckDB's zero-copy vectorized bridge.

This provides a lightweight, instant relational serving layer for BI tools
like Metabase without incurring heavy JVM memory overhead or Delta connector issues.
"""

import os
import sys
import duckdb

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("AIRFLOW_DB_USER", "airflow")
PG_PASS = os.getenv("AIRFLOW_DB_PASSWORD", "airflow")
PG_DB = os.getenv("AIRFLOW_DB_NAME", "airflow")


def sync_gold_to_postgres():
    print("=" * 60)
    print("SYNCING GOLD TABLES & AUDIT LOGS TO POSTGRES VIA DUCKDB")
    print("=" * 60)

    # In-memory DuckDB instance
    con = duckdb.connect()

    # Configure S3 HTTPFS access
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint = '{MINIO_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id = '{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key = '{MINIO_SECRET_KEY}';")
    con.execute("SET s3_use_ssl = false;")
    con.execute("SET s3_url_style = 'path';")

    # Install & attach to PostgreSQL
    con.execute("INSTALL postgres; LOAD postgres;")
    attach_query = f"ATTACH 'host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASS} dbname={PG_DB}' AS pg (TYPE POSTGRES);"
    con.execute(attach_query)
    print("Connected to PostgreSQL successfully.")

    # Tables to sync: (table_name, s3_source_glob)
    tables = [
        ("gold_daily_trips_revenue", "s3://gold/export/daily_trips_revenue/*.parquet"),
        ("gold_pickup_zone_metrics", "s3://gold/export/pickup_zone_metrics/*.parquet"),
        ("gold_hourly_fare_tip_metrics", "s3://gold/export/hourly_fare_tip_metrics/*.parquet"),
        ("pipeline_audit", "s3://audit/pipeline_audit/*.snappy.parquet"),
    ]

    for table_name, s3_path in tables:
        print(f"Loading {table_name} from {s3_path}...")
        # Drop if exists and create table in Postgres
        con.execute(f"DROP TABLE IF EXISTS pg.{table_name};")
        con.execute(f"CREATE TABLE pg.{table_name} AS SELECT * FROM read_parquet('{s3_path}');")
        
        # Verify count
        count = con.execute(f"SELECT COUNT(*) FROM pg.{table_name};").fetchone()[0]
        print(f"  -> Successfully synced {table_name}: {count:,} rows")

    print("=" * 60)
    print("ALL GOLD & AUDIT TABLES SYNCED TO POSTGRES SUCCESSFULLY!")
    print("=" * 60)


if __name__ == "__main__":
    sync_gold_to_postgres()
