{{ config(materialized='view') }}

-- DuckDB views Gold Pickup Zone Metrics directly from MinIO S3 parquet files
SELECT
    pickup_location_id,
    borough,
    zone,
    service_zone,
    total_trips,
    total_revenue,
    avg_fare,
    avg_tip,
    avg_distance
FROM read_parquet('s3://gold/export/pickup_zone_metrics/*.parquet')
