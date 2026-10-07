{{ config(materialized='view') }}

-- DuckDB views Gold Hourly Metrics directly from MinIO S3 parquet files
SELECT
    pickup_hour,
    total_trips,
    avg_fare,
    avg_tip,
    avg_tip_percentage,
    avg_total_amount
FROM read_parquet('s3://gold/export/hourly_fare_tip_metrics/*.parquet')
