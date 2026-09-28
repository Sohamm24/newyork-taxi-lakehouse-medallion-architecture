import os
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

def get_spark_session(app_name: str = "nyc-taxi-lakehouse") -> SparkSession:
    """
    Constructs an optimized PySpark session configured for Delta Lake and MinIO S3A.
    
    Design Decisions:
    - local[2]: Restricts execution to 2 CPU cores on an 8 GB laptop.
    - driver.memory 2g: Keeps memory footprint well within host bounds.
    - shuffle.partitions 4: Dramatically speeds up local shuffles (default is 200).
    - Delta extensions & DeltaCatalog: Required for Delta ACID, MERGE, and Time Travel.
    - S3A FileSystem: Points Spark to local MinIO endpoint over HTTP.
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    driver_mem = os.getenv("SPARK_DRIVER_MEMORY", "2g")
    master = os.getenv("SPARK_MASTER", "local[2]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_mem)
        # Low shuffle partitions for single-machine local mode to prevent thread contention
        .config("spark.sql.shuffle.partitions", "4")
        # Delta Lake catalog registration
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A / MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Dynamic partition overwrite enables idempotent partition updates
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    return builder.getOrCreate()

def get_s3_path(bucket: str, path: str = "") -> str:
    """Constructs a standard s3a:// URI for MinIO storage."""
    clean_path = path.lstrip("/")
    return f"s3a://{bucket}/{clean_path}" if clean_path else f"s3a://{bucket}"

def write_audit_record(
    spark: SparkSession,
    layer: str,
    target_table: str,
    rows_in: int,
    rows_out: int,
    rows_rejected: int,
    status: str = "SUCCESS",
    error_message: str = "",
) -> None:
    """
    Writes execution metrics to the Delta audit table in s3a://audit/pipeline_audit.
    Provides complete lineage and data quality traceability across pipeline runs.
    """
    audit_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("rows_in", LongType(), False),
        StructField("rows_out", LongType(), False),
        StructField("rows_rejected", LongType(), False),
        StructField("run_timestamp", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ])

    record = [(
        str(uuid.uuid4()),
        layer,
        target_table,
        int(rows_in),
        int(rows_out),
        int(rows_rejected),
        datetime.utcnow(),
        status,
        error_message,
    )]

    audit_df = spark.createDataFrame(record, schema=audit_schema)
    audit_path = get_s3_path("audit", "pipeline_audit")

    # Append audit records so all pipeline executions are preserved
    audit_df.write.format("delta").mode("append").save(audit_path)
    print(f"[AUDIT] Layer: {layer} | Table: {target_table} | In: {rows_in:,} | Out: {rows_out:,} | Rejected: {rows_rejected:,} | Status: {status}")
