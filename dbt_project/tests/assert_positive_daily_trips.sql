-- Custom Test: Verifies that every recorded date has strictly positive daily trips (> 0) and non-negative revenue.
-- Returns failing rows (if any). A passing test produces 0 rows.
SELECT
    pickup_date,
    total_trips,
    total_revenue
FROM {{ ref('gold_daily_trips_revenue') }}
WHERE total_trips <= 0 OR total_revenue < 0
