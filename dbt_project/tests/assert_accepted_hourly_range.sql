-- Accepted Range Test: Verifies that pickup_hour is between 0 and 23 and average fare is non-negative.
SELECT
    pickup_hour,
    avg_fare
FROM {{ ref('gold_hourly_fare_tip_metrics') }}
WHERE pickup_hour < 0 OR pickup_hour > 23 OR avg_fare < 0
