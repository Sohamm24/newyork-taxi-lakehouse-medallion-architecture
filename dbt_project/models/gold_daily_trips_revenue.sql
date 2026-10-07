{{ config(materialized='view') }}

-- DuckDB views Gold Daily Trips & Revenue directly from MinIO S3 parquet files
SELECT
    pickup_date,
    total_trips,
    total_revenue,
    total_fare,
    total_tip,
    avg_trip_distance,
    avg_total_amount
FROM read_parquet('s3://gold/export/daily_trips_revenue/*.parquet')
