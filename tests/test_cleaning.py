from datetime import datetime, timedelta
import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)
from spark_jobs.silver import clean_and_validate_taxi_data

# Base schema replicating Bronze raw payload for unit test records
TEST_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
])

def create_row(
    vendor_id=1,
    pickup=datetime(2024, 1, 15, 10, 0, 0),
    dropoff=None,
    passengers=1,
    distance=2.5,
    pu_loc=161,
    do_loc=237,
    fare=15.0,
    tip=3.0,
    total=20.0,
):
    """Helper creating a standard valid test record with optional overrides."""
    if dropoff is None and pickup is not None:
        dropoff = pickup + timedelta(minutes=20)

    return (
        vendor_id,
        pickup,
        dropoff,
        passengers,
        distance,
        1,       # RatecodeID
        "N",     # store_and_fwd_flag
        pu_loc,
        do_loc,
        1,       # payment_type
        fare,
        0.5,     # extra
        0.5,     # mta_tax
        tip,
        0.0,     # tolls_amount
        1.0,     # improvement_surcharge
        total,
        2.5,     # congestion_surcharge
        0.0,     # Airport_fee
    )

def test_valid_record_passes(spark):
    """Verifies that a well-formed trip record passes validation without being quarantined."""
    data = [create_row()]
    df = spark.createDataFrame(data, schema=TEST_SCHEMA)
    valid_df, quarantine_df = clean_and_validate_taxi_data(df, expected_year=2024, expected_months=[1, 2])

    assert valid_df.count() == 1
    assert quarantine_df.count() == 0
    assert "pickup_date" in valid_df.columns

def test_null_key_column_quarantined(spark):
    """Verifies that records missing critical keys (pickup, fare, distance) are quarantined."""
    data = [
        create_row(pickup=None),
        create_row(fare=None),
        create_row(distance=None),
        create_row(pu_loc=None),
    ]
    df = spark.createDataFrame(data, schema=TEST_SCHEMA)
    valid_df, quarantine_df = clean_and_validate_taxi_data(df, expected_year=2024, expected_months=[1, 2])

    assert valid_df.count() == 0
    assert quarantine_df.count() == 4
    reasons = [row.reject_reason for row in quarantine_df.collect()]
    assert all(r == "NULL_KEY_COLUMN" for r in reasons)

def test_invalid_fare_quarantined(spark):
    """Verifies that non-positive fares are quarantined."""
    data = [
        create_row(fare=0.0),
        create_row(fare=-12.5),
    ]
    df = spark.createDataFrame(data, schema=TEST_SCHEMA)
    valid_df, quarantine_df = clean_and_validate_taxi_data(df, expected_year=2024, expected_months=[1, 2])

    assert valid_df.count() == 0
    assert quarantine_df.count() == 2
    reasons = [row.reject_reason for row in quarantine_df.collect()]
    assert all(r == "INVALID_FARE_AMOUNT" for r in reasons)

def test_invalid_distance_quarantined(spark):
    """Verifies zero, negative, or absurd distances (> 500 miles) are quarantined."""
    data = [
        create_row(distance=0.0),
        create_row(distance=-1.5),
        create_row(distance=501.0),
    ]
    df = spark.createDataFrame(data, schema=TEST_SCHEMA)
    valid_df, quarantine_df = clean_and_validate_taxi_data(df, expected_year=2024, expected_months=[1, 2])

    assert valid_df.count() == 0
    assert quarantine_df.count() == 3
    reasons = [row.reject_reason for row in quarantine_df.collect()]
    assert all(r == "INVALID_TRIP_DISTANCE" for r in reasons)

def test_dropoff_before_pickup_quarantined(spark):
    """Verifies negative or zero duration trips are rejected."""
    data = [
        create_row(pickup=datetime(2024, 1, 15, 12, 0, 0), dropoff=datetime(2024, 1, 15, 11, 45, 0)),
        create_row(pickup=datetime(2024, 1, 15, 12, 0, 0), dropoff=datetime(2024, 1, 15, 12, 0, 0)),
    ]
    df = spark.createDataFrame(data, schema=TEST_SCHEMA)
    valid_df, quarantine_df = clean_and_validate_taxi_data(df, expected_year=2024, expected_months=[1, 2])

    assert valid_df.count() == 0
    assert quarantine_df.count() == 2
    reasons = [row.reject_reason for row in quarantine_df.collect()]
    assert all(r == "DROPOFF_BEFORE_PICKUP" for r in reasons)

def test_passenger_count_anomalies_quarantined(spark):
    """Verifies 0, negative, >9, or null passenger counts are rejected."""
    data = [
        create_row(passengers=0),
        create_row(passengers=-1),
        create_row(passengers=10),
        create_row(passengers=None),
    ]
    df = spark.createDataFrame(data, schema=TEST_SCHEMA)
    valid_df, quarantine_df = clean_and_validate_taxi_data(df, expected_year=2024, expected_months=[1, 2])

    assert valid_df.count() == 0
    assert quarantine_df.count() == 4
    reasons = [row.reject_reason for row in quarantine_df.collect()]
    assert all(r == "INVALID_PASSENGER_COUNT" for r in reasons)

def test_out_of_bounds_timestamp_quarantined(spark):
    """Verifies GPS glitches outside target date range are rejected."""
    data = [
        create_row(pickup=datetime(2008, 1, 1, 12, 0, 0)),   # Wrong year
        create_row(pickup=datetime(2024, 5, 1, 12, 0, 0)),   # Month not in [1, 2]
    ]
    df = spark.createDataFrame(data, schema=TEST_SCHEMA)
    valid_df, quarantine_df = clean_and_validate_taxi_data(df, expected_year=2024, expected_months=[1, 2])

    assert valid_df.count() == 0
    assert quarantine_df.count() == 2
    reasons = [row.reject_reason for row in quarantine_df.collect()]
    assert all(r == "OUT_OF_BOUNDS_TIMESTAMP" for r in reasons)

def test_deduplication(spark):
    """Verifies identical duplicates across the business key are reduced to 1 row."""
    row = create_row()
    data = [row, row]  # Duplicate record
    df = spark.createDataFrame(data, schema=TEST_SCHEMA)
    valid_df, quarantine_df = clean_and_validate_taxi_data(df, expected_year=2024, expected_months=[1, 2])

    assert valid_df.count() == 1
    assert quarantine_df.count() == 0
