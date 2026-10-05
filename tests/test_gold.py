from datetime import datetime
import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType,
    TimestampType,
)
from spark_jobs.gold import (
    build_daily_trips_revenue,
    build_pickup_zone_metrics,
    build_hourly_fare_tip_metrics,
)

SILVER_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("pickup_date", DateType(), True),
])

ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])

def test_build_daily_trips_revenue(spark):
    """Verifies that daily trips, total revenue, and average distance calculate correctly."""
    data = [
        (1, datetime(2024, 1, 15, 10, 0), datetime(2024, 1, 15, 10, 15), 1, 2.0, 161, 237, 10.0, 2.0, 15.0, datetime(2024, 1, 15).date()),
        (1, datetime(2024, 1, 15, 11, 0), datetime(2024, 1, 15, 11, 20), 2, 4.0, 161, 237, 20.0, 5.0, 30.0, datetime(2024, 1, 15).date()),
        (1, datetime(2024, 1, 16, 12, 0), datetime(2024, 1, 16, 12, 30), 1, 3.0, 161, 237, 15.0, 3.0, 20.0, datetime(2024, 1, 16).date()),
    ]
    df = spark.createDataFrame(data, schema=SILVER_SCHEMA)
    gold_daily = build_daily_trips_revenue(df).collect()

    assert len(gold_daily) == 2  # 2 distinct dates
    day_15 = [r for r in gold_daily if str(r.pickup_date) == "2024-01-15"][0]
    assert day_15.total_trips == 2
    assert day_15.total_revenue == 45.0
    assert day_15.total_fare == 30.0
    assert day_15.total_tip == 7.0
    assert day_15.avg_trip_distance == 3.0

def test_build_pickup_zone_metrics(spark):
    """Verifies joining trips with zone lookup table and grouping metrics by zone."""
    trip_data = [
        (1, datetime(2024, 1, 15, 10, 0), datetime(2024, 1, 15, 10, 15), 1, 2.0, 161, 237, 10.0, 2.0, 15.0, datetime(2024, 1, 15).date()),
        (1, datetime(2024, 1, 15, 11, 0), datetime(2024, 1, 15, 11, 20), 2, 3.0, 161, 237, 15.0, 3.0, 20.0, datetime(2024, 1, 15).date()),
        (1, datetime(2024, 1, 15, 12, 0), datetime(2024, 1, 15, 12, 20), 1, 5.0, 132, 237, 50.0, 10.0, 65.0, datetime(2024, 1, 15).date()),
    ]
    zone_data = [
        (161, "Manhattan", "Midtown Center", "Yellow Zone"),
        (132, "Queens", "JFK Airport", "Airports"),
    ]
    trips_df = spark.createDataFrame(trip_data, schema=SILVER_SCHEMA)
    zones_df = spark.createDataFrame(zone_data, schema=ZONE_SCHEMA)

    gold_zones = build_pickup_zone_metrics(trips_df, zones_df).collect()

    assert len(gold_zones) == 2
    top_zone = gold_zones[0]  # Ordered by total_trips desc -> Location 161
    assert top_zone.pickup_location_id == 161
    assert top_zone.borough == "Manhattan"
    assert top_zone.zone == "Midtown Center"
    assert top_zone.total_trips == 2
    assert top_zone.total_revenue == 35.0

def test_build_hourly_fare_tip_metrics(spark):
    """Verifies extracting hour of day and calculating trip count and average tip percentage."""
    data = [
        (1, datetime(2024, 1, 15, 14, 10), datetime(2024, 1, 15, 14, 25), 1, 2.0, 161, 237, 10.0, 2.0, 15.0, datetime(2024, 1, 15).date()),
        (1, datetime(2024, 1, 15, 14, 40), datetime(2024, 1, 15, 14, 55), 1, 2.0, 161, 237, 20.0, 4.0, 26.0, datetime(2024, 1, 15).date()),
        (1, datetime(2024, 1, 15, 20, 15), datetime(2024, 1, 15, 20, 30), 1, 1.0, 161, 237, 10.0, 3.0, 15.0, datetime(2024, 1, 15).date()),
    ]
    df = spark.createDataFrame(data, schema=SILVER_SCHEMA)
    gold_hourly = build_hourly_fare_tip_metrics(df).collect()

    assert len(gold_hourly) == 2  # Hours 14 and 20
    hour_14 = [r for r in gold_hourly if r.pickup_hour == 14][0]
    assert hour_14.total_trips == 2
    assert hour_14.avg_fare == 15.0
    assert hour_14.avg_tip == 3.0
    assert hour_14.avg_tip_percentage == 20.0  # (2/10 + 4/20)/2 * 100 = 20%
