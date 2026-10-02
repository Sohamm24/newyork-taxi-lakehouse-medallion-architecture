import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from spark_jobs.common import get_spark_session, get_s3_path, write_audit_record

def build_daily_trips_revenue(silver_df: DataFrame) -> DataFrame:
    """
    Gold Model (a): Aggregates daily trip volume, gross revenue, tips, and average distances.
    """
    return (
        silver_df
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tip"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        )
        .orderBy("pickup_date")
    )

def build_pickup_zone_metrics(silver_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Gold Model (b): Joins clean trips with the taxi zone lookup dimension table
    to calculate trip volume and revenue performance by pickup zone and borough.
    """
    # Join on PULocationID -> LocationID
    joined_df = silver_df.join(
        zones_df,
        silver_df.PULocationID == zones_df.LocationID,
        how="inner",
    )

    return (
        joined_df
        .groupBy(
            F.col("PULocationID").alias("pickup_location_id"),
            F.coalesce(F.col("Borough"), F.lit("Unknown")).alias("borough"),
            F.coalesce(F.col("Zone"), F.lit("Unknown")).alias("zone"),
            F.coalesce(F.col("service_zone"), F.lit("Unknown")).alias("service_zone"),
        )
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )
        .orderBy(F.col("total_trips").desc())
    )

def build_hourly_fare_tip_metrics(silver_df: DataFrame) -> DataFrame:
    """
    Gold Model (c): Extracts hour of day to analyze hourly demand, average fares, and tip rates.
    """
    return (
        silver_df
        .withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
            F.round(
                F.avg(
                    F.when(F.col("fare_amount") > 0, (F.col("tip_amount") / F.col("fare_amount")) * 100.0)
                    .otherwise(0.0)
                ),
                2,
            ).alias("avg_tip_percentage"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        )
        .orderBy("pickup_hour")
    )

def run_gold_pipeline():
    """
    Executes Gold Layer aggregations and writes Delta tables to MinIO gold bucket.
    Idempotent execution via Delta overwrite mode.
    """
    spark = get_spark_session("gold-aggregations")
    print("=== STARTING GOLD PIPELINE ===")

    try:
        silver_path = get_s3_path("silver", "yellow_taxi")
        zones_path = get_s3_path("bronze", "taxi_zone_lookup")

        print(f"Reading Silver trips from: {silver_path}")
        silver_df = spark.read.format("delta").load(silver_path)
        silver_rows = silver_df.count()

        print(f"Reading Zone lookup from: {zones_path}")
        zones_df = spark.read.format("delta").load(zones_path)

        # -------------------------------------------------------------
        # 1. Daily Trips & Revenue
        # -------------------------------------------------------------
        daily_path = get_s3_path("gold", "daily_trips_revenue")
        print(f"Computing daily trips and revenue -> {daily_path}...")
        daily_df = build_daily_trips_revenue(silver_df)
        daily_df.write.format("delta").mode("overwrite").save(daily_path)
        daily_count = spark.read.format("delta").load(daily_path).count()

        write_audit_record(
            spark=spark,
            layer="gold",
            target_table="daily_trips_revenue",
            rows_in=silver_rows,
            rows_out=daily_count,
            rows_rejected=0,
            status="SUCCESS",
        )

        # -------------------------------------------------------------
        # 2. Trips and Revenue by Pickup Zone
        # -------------------------------------------------------------
        zone_metrics_path = get_s3_path("gold", "pickup_zone_metrics")
        print(f"Computing pickup zone metrics -> {zone_metrics_path}...")
        zone_metrics_df = build_pickup_zone_metrics(silver_df, zones_df)
        zone_metrics_df.write.format("delta").mode("overwrite").save(zone_metrics_path)
        zone_count = spark.read.format("delta").load(zone_metrics_path).count()

        write_audit_record(
            spark=spark,
            layer="gold",
            target_table="pickup_zone_metrics",
            rows_in=silver_rows,
            rows_out=zone_count,
            rows_rejected=0,
            status="SUCCESS",
        )

        # -------------------------------------------------------------
        # 3. Average Fare and Tip by Hour of Day
        # -------------------------------------------------------------
        hourly_path = get_s3_path("gold", "hourly_fare_tip_metrics")
        print(f"Computing hourly fare and tip metrics -> {hourly_path}...")
        hourly_df = build_hourly_fare_tip_metrics(silver_df)
        hourly_df.write.format("delta").mode("overwrite").save(hourly_path)
        hourly_count = spark.read.format("delta").load(hourly_path).count()

        write_audit_record(
            spark=spark,
            layer="gold",
            target_table="hourly_fare_tip_metrics",
            rows_in=silver_rows,
            rows_out=hourly_count,
            rows_rejected=0,
            status="SUCCESS",
        )
        # -------------------------------------------------------------
        # 4. Parquet Export for Downstream DuckDB & BI
        # -------------------------------------------------------------
        # Writes an active snapshot without Delta transaction log overhead,
        # perfectly matching DuckDB httpfs reader capabilities.
        print("Exporting clean Gold Parquet snapshots for DuckDB / dbt / BI...")
        daily_df.coalesce(1).write.mode("overwrite").parquet(get_s3_path("gold", "export/daily_trips_revenue"))
        zone_metrics_df.coalesce(1).write.mode("overwrite").parquet(get_s3_path("gold", "export/pickup_zone_metrics"))
        hourly_df.coalesce(1).write.mode("overwrite").parquet(get_s3_path("gold", "export/hourly_fare_tip_metrics"))

        print("=== GOLD PIPELINE COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        print(f"Error during Gold pipeline: {e}", file=sys.stderr)
        write_audit_record(
            spark=spark,
            layer="gold",
            target_table="all_gold_tables",
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
    run_gold_pipeline()
