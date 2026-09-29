import sys
from pyspark.sql import functions as F
from spark_jobs.common import get_spark_session, get_s3_path, write_audit_record

def run_bronze_ingestion():
    """
    Ingests raw Parquet files and Zone CSV from the landing area into Bronze Delta tables.
    
    Design Rules:
    - No cleaning, filtering, or type casting occurs at the Bronze layer.
    - Exactly two metadata columns are appended:
        1. _ingestion_ts: UTC timestamp when the record entered the Lakehouse.
        2. _source_file: URI of the raw input file for full auditability and lineage.
    - Overwrite mode guarantees idempotency upon pipeline replays.
    """
    spark = get_spark_session("bronze-ingestion")
    print("=== STARTING BRONZE INGESTION ===")

    try:
        # -------------------------------------------------------------
        # 1. Ingest NYC Yellow Taxi Trip Records (Parquet)
        # -------------------------------------------------------------
        raw_trips_path = get_s3_path("bronze", "raw/yellow_tripdata_*.parquet")
        bronze_trips_path = get_s3_path("bronze", "yellow_taxi_raw")

        print(f"Reading raw taxi parquet from: {raw_trips_path}")
        raw_trips_df = spark.read.parquet(raw_trips_path)
        rows_in = raw_trips_df.count()

        # Append Lakehouse lineage columns without mutating existing payload
        bronze_trips_df = (
            raw_trips_df
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
        )

        print(f"Writing {rows_in:,} rows to Bronze Delta table: {bronze_trips_path}")
        bronze_trips_df.write.format("delta").mode("overwrite").save(bronze_trips_path)
        rows_out = spark.read.format("delta").load(bronze_trips_path).count()

        # Audit logging for trips ingestion
        write_audit_record(
            spark=spark,
            layer="bronze",
            target_table="yellow_taxi_raw",
            rows_in=rows_in,
            rows_out=rows_out,
            rows_rejected=0,
            status="SUCCESS",
        )

        # -------------------------------------------------------------
        # 2. Ingest Taxi Zone Lookup (CSV)
        # -------------------------------------------------------------
        raw_zone_path = get_s3_path("bronze", "raw/taxi_zone_lookup.csv")
        bronze_zone_path = get_s3_path("bronze", "taxi_zone_lookup")

        print(f"Reading raw taxi zone CSV from: {raw_zone_path}")
        raw_zones_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(raw_zone_path)
        )
        zone_rows_in = raw_zones_df.count()

        bronze_zones_df = (
            raw_zones_df
            .withColumn("_ingestion_ts", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
        )

        print(f"Writing {zone_rows_in:,} zones to Bronze Delta table: {bronze_zone_path}")
        bronze_zones_df.write.format("delta").mode("overwrite").save(bronze_zone_path)
        zone_rows_out = spark.read.format("delta").load(bronze_zone_path).count()

        # Audit logging for zones ingestion
        write_audit_record(
            spark=spark,
            layer="bronze",
            target_table="taxi_zone_lookup",
            rows_in=zone_rows_in,
            rows_out=zone_rows_out,
            rows_rejected=0,
            status="SUCCESS",
        )

        print("=== BRONZE INGESTION COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        print(f"Error during Bronze ingestion: {e}", file=sys.stderr)
        write_audit_record(
            spark=spark,
            layer="bronze",
            target_table="yellow_taxi_raw",
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
    run_bronze_ingestion()
