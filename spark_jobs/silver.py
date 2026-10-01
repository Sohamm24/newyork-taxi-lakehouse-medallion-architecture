import sys
from typing import Tuple, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    TimestampType,
    StringType,
)
from spark_jobs.common import get_spark_session, get_s3_path, write_audit_record

def apply_schema_and_types(df: DataFrame) -> DataFrame:
    """
    Explicitly casts Bronze fields to strong analytical types.
    Ensures consistent data representation across heterogeneous parquet files.
    """
    return (
        df
        .withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("RatecodeID", F.col("RatecodeID").cast(IntegerType()))
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast(StringType()))
        .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
        .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("extra", F.col("extra").cast(DoubleType()))
        .withColumn("mta_tax", F.col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast(DoubleType()))
        .withColumn("Airport_fee", F.col("Airport_fee").cast(DoubleType()))
        # Derived partitioning column
        .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
    )

def clean_and_validate_taxi_data(
    df: DataFrame,
    expected_year: int = 2024,
    expected_months: List[int] = [1, 2],
) -> Tuple[DataFrame, DataFrame]:
    """
    Applies data quality and validation rules according to Silver layer specifications.
    
    Returns:
        valid_df: Clean, deduplicated DataFrame ready for analytics.
        quarantine_df: Rejected records with an explicit 'reject_reason' column.
    """
    typed_df = apply_schema_and_types(df)

    # 1. Flag records with nulls in business key columns
    null_key_cond = (
        F.col("tpep_pickup_datetime").isNull()
        | F.col("tpep_dropoff_datetime").isNull()
        | F.col("PULocationID").isNull()
        | F.col("DOLocationID").isNull()
        | F.col("fare_amount").isNull()
        | F.col("trip_distance").isNull()
    )

    # 2. Flag records with invalid fare or trip distance
    invalid_fare_cond = F.col("fare_amount") <= 0.0
    invalid_distance_cond = (F.col("trip_distance") <= 0.0) | (F.col("trip_distance") > 500.0)

    # 3. Flag chronologically invalid trips
    chronology_cond = F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")

    # 4. Flag passenger count anomalies
    passenger_cond = (
        F.col("passenger_count").isNull()
        | (F.col("passenger_count") <= 0)
        | (F.col("passenger_count") > 9)
    )

    # 5. Flag out-of-range timestamps (GPS/meter date glitches outside dataset window)
    timestamp_cond = (
        (F.year(F.col("tpep_pickup_datetime")) != expected_year)
        | (~F.month(F.col("tpep_pickup_datetime")).isin(expected_months))
    )

    # Assign reject_reason hierarchically
    flagged_df = typed_df.withColumn(
        "reject_reason",
        F.when(null_key_cond, F.lit("NULL_KEY_COLUMN"))
        .when(invalid_fare_cond, F.lit("INVALID_FARE_AMOUNT"))
        .when(invalid_distance_cond, F.lit("INVALID_TRIP_DISTANCE"))
        .when(chronology_cond, F.lit("DROPOFF_BEFORE_PICKUP"))
        .when(passenger_cond, F.lit("INVALID_PASSENGER_COUNT"))
        .when(timestamp_cond, F.lit("OUT_OF_BOUNDS_TIMESTAMP"))
        .otherwise(F.lit(None))
    )

    # Separate quarantine vs valid records
    quarantine_df = (
        flagged_df
        .filter(F.col("reject_reason").isNotNull())
        .withColumn("_quarantine_ts", F.current_timestamp())
    )

    valid_raw_df = (
        flagged_df
        .filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
    )

    # Deduplicate valid records by business key
    dedup_cols = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "trip_distance",
    ]
    valid_df = valid_raw_df.dropDuplicates(subset=dedup_cols)

    return valid_df, quarantine_df

def run_silver_pipeline():
    """
    Executes the Silver transformation pipeline:
    - Reads Bronze Delta table
    - Cleans, validates, deduplicates, and partitions by pickup_date
    - Routes rejected records to quarantine table
    - Writes execution metrics to the audit table
    """
    spark = get_spark_session("silver-cleaning")
    print("=== STARTING SILVER PIPELINE ===")

    try:
        bronze_trips_path = get_s3_path("bronze", "yellow_taxi_raw")
        silver_trips_path = get_s3_path("silver", "yellow_taxi")
        quarantine_path = get_s3_path("silver", "yellow_taxi_quarantine")

        print(f"Reading Bronze records from: {bronze_trips_path}")
        bronze_df = spark.read.format("delta").load(bronze_trips_path)
        rows_in = bronze_df.count()

        print(f"Applying data validation and quarantine routing on {rows_in:,} rows...")
        valid_df, quarantine_df = clean_and_validate_taxi_data(
            bronze_df,
            expected_year=2024,
            expected_months=[1, 2],
        )

        # Cache quarantine and valid counts
        quarantine_df.cache()
        valid_df.cache()

        rows_rejected = quarantine_df.count()
        rows_out = valid_df.count()

        print(f"Validation summary: {rows_out:,} valid rows, {rows_rejected:,} rejected rows.")

        # Write quarantine records
        print(f"Writing quarantine records to: {quarantine_path}")
        quarantine_df.write.format("delta").mode("append").save(quarantine_path)

        # Write clean Silver Delta table partitioned by pickup_date
        print(f"Writing valid Silver records to: {silver_trips_path} (partitioned by pickup_date)")
        (
            valid_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("pickup_date")
            .save(silver_trips_path)
        )

        # Audit logging
        write_audit_record(
            spark=spark,
            layer="silver",
            target_table="yellow_taxi",
            rows_in=rows_in,
            rows_out=rows_out,
            rows_rejected=rows_rejected,
            status="SUCCESS",
        )

        print("=== SILVER PIPELINE COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        print(f"Error during Silver pipeline: {e}", file=sys.stderr)
        write_audit_record(
            spark=spark,
            layer="silver",
            target_table="yellow_taxi",
            rows_in=0,
            rows_out=0,
            rows_rejected=0,
            status="FAILED",
            error_message=str(e),
        )
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_pipeline()
