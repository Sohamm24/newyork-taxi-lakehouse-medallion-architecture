-- Accepted Range Test: Verifies that zone-level total revenue is non-negative (>= 0).
SELECT
    pickup_location_id,
    total_revenue
FROM {{ ref('gold_pickup_zone_metrics') }}
WHERE total_revenue < 0
