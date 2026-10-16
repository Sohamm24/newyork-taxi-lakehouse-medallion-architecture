"""
spark_benchmark.py
Empirical performance benchmarking suite for the NYC Taxi Lakehouse.

Measures and proves the performance impact of three core data engineering optimizations:
1. Columnar Storage Format: Raw CSV Scan vs. Delta Lake (Snappy Parquet) Scan.
2. Partition Pruning: Full Table Scan vs. Directory Partition-Pruned Scan.
3. In-Memory Caching: Multi-pass aggregation without cache vs. with Spark DataFrame cache.

Results are printed in ASCII format, written to docs/benchmark_results.md,
and saved as a CSV in docs/benchmark_results.csv.
"""

import os
import time
import csv
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType
from spark_jobs.common import get_spark_session, get_s3_path, write_audit_record

BENCHMARK_CSV_PATH = get_s3_path("bronze", "benchmark/taxi_csv")
BENCHMARK_DELTA_PATH = get_s3_path("bronze", "benchmark/taxi_delta")
BENCHMARK_UNPARTITIONED_PATH = get_s3_path("silver", "benchmark/taxi_unpartitioned")
SILVER_PARTITIONED_PATH = get_s3_path("silver", "yellow_taxi")
RAW_BRONZE_PATH = get_s3_path("bronze", "yellow_taxi_raw")


def prepare_benchmark_datasets(spark):
    """
    Ensures baseline datasets exist for fair apples-to-apples performance comparisons.
    Uses a 500,000 row subset for format comparison and a 1,000,000 row subset
    for partition comparison to maintain fast benchmark execution on 8 GB laptops.
    """
    print("[BENCHMARK] Verifying / preparing benchmark test fixtures in MinIO...")

    # Check if CSV benchmark fixture exists
    try:
        spark.read.option("header", "true").csv(BENCHMARK_CSV_PATH).limit(1).count()
        csv_exists = True
    except Exception:
        csv_exists = False

    if not csv_exists:
        print("[BENCHMARK] Creating 500K sample for CSV vs Delta format benchmark...")
        raw_df = spark.read.format("delta").load(RAW_BRONZE_PATH).limit(500000)
        # Select key numerical & categorical columns for format testing
        cols = [
            "VendorID", "tpep_pickup_datetime", "passenger_count",
            "trip_distance", "RatecodeID", "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "tip_amount", "total_amount"
        ]
        sample_df = raw_df.select(*cols)

        # Write as CSV (repartition(2) keeps reasonable file count)
        sample_df.repartition(2).write.mode("overwrite").option("header", "true").csv(BENCHMARK_CSV_PATH)

        # Write as Delta (Snappy Parquet with statistics)
        sample_df.repartition(2).write.format("delta").mode("overwrite").save(BENCHMARK_DELTA_PATH)
        print("[BENCHMARK] CSV and Delta benchmark datasets written successfully.")

    # Check if unpartitioned silver fixture exists
    try:
        spark.read.format("delta").load(BENCHMARK_UNPARTITIONED_PATH).limit(1).count()
        unpart_exists = True
    except Exception:
        unpart_exists = False

    if not unpart_exists:
        print("[BENCHMARK] Creating unpartitioned baseline from Silver for partition pruning benchmark...")
        silver_df = spark.read.format("delta").load(SILVER_PARTITIONED_PATH)
        # Write unpartitioned copy of 1,000,000 rows
        silver_df.limit(1000000).write.format("delta").mode("overwrite").save(BENCHMARK_UNPARTITIONED_PATH)
        print("[BENCHMARK] Unpartitioned benchmark dataset written successfully.")


def run_format_benchmark(spark):
    """
    Benchmark 1: Raw CSV vs. Delta Lake Storage Format.
    
    Why Delta/Parquet wins:
    - Columnar projection: Reads only queried columns; CSV reads every character of every column.
    - Binary serialization & Snappy compression: 70-80% smaller I/O footprint over network/disk.
    - Zero type-parsing: Parquet stores native binary types; CSV requires string tokenization and casting.
    """
    print("\n--- Running Benchmark 1: Storage Format (CSV vs. Delta) ---")

    # 1. Query CSV
    csv_start = time.perf_counter()
    csv_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(BENCHMARK_CSV_PATH)
    )
    csv_agg = (
        csv_df.filter(F.col("trip_distance") > 3.0)
        .groupBy("passenger_count")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("total_amount").alias("avg_total"),
            F.sum("trip_distance").alias("sum_dist"),
        )
        .collect()
    )
    csv_duration = round(time.perf_counter() - csv_start, 3)
    print(f"  -> Raw CSV Scan & Aggregation: {csv_duration} s (Rows returned: {len(csv_agg)})")

    # 2. Query Delta
    delta_start = time.perf_counter()
    delta_df = spark.read.format("delta").load(BENCHMARK_DELTA_PATH)
    delta_agg = (
        delta_df.filter(F.col("trip_distance") > 3.0)
        .groupBy("passenger_count")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("total_amount").alias("avg_total"),
            F.sum("trip_distance").alias("sum_dist"),
        )
        .collect()
    )
    delta_duration = round(time.perf_counter() - delta_start, 3)
    speedup = round(csv_duration / max(delta_duration, 0.001), 2)
    print(f"  -> Delta Lake Scan & Aggregation: {delta_duration} s (Rows returned: {len(delta_agg)})")
    print(f"  -> Speedup: {speedup}x faster")

    return {
        "test_name": "Storage Format (500K rows)",
        "baseline": f"Raw CSV ({csv_duration}s)",
        "optimized": f"Delta Lake ({delta_duration}s)",
        "speedup": f"{speedup}x",
        "explanation": "Delta/Parquet uses columnar projection, native binary data types, and Snappy compression, avoiding CSV text parsing overhead."
    }


def run_partition_benchmark(spark):
    """
    Benchmark 2: Unpartitioned vs. Partitioned Scan (Partition Pruning).
    
    Why Partitioning wins:
    - Prunes directory tree at the catalog/driver level before file scanning begins.
    - On a 3-day query across 60 days of data, 95% of data files are skipped entirely.
    """
    print("\n--- Running Benchmark 2: Partition Pruning (Unpartitioned vs. Partitioned) ---")
    target_dates = ["2024-01-15", "2024-01-16", "2024-01-17"]

    # 1. Unpartitioned Scan
    unpart_start = time.perf_counter()
    unpart_df = spark.read.format("delta").load(BENCHMARK_UNPARTITIONED_PATH)
    unpart_res = (
        unpart_df.filter(F.col("pickup_date").isin(target_dates))
        .groupBy("pickup_date")
        .agg(F.count("*").alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
        .collect()
    )
    unpart_duration = round(time.perf_counter() - unpart_start, 3)
    print(f"  -> Unpartitioned Table Scan: {unpart_duration} s (Rows returned: {len(unpart_res)})")

    # 2. Partitioned Scan (Pruned to 3 directories out of 60)
    part_start = time.perf_counter()
    part_df = spark.read.format("delta").load(SILVER_PARTITIONED_PATH)
    part_res = (
        part_df.filter(F.col("pickup_date").isin(target_dates))
        .groupBy("pickup_date")
        .agg(F.count("*").alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
        .collect()
    )
    part_duration = round(time.perf_counter() - part_start, 3)
    speedup = round(unpart_duration / max(part_duration, 0.001), 2)
    print(f"  -> Partition-Pruned Scan: {part_duration} s (Rows returned: {len(part_res)})")
    print(f"  -> Speedup: {speedup}x faster")

    return {
        "test_name": "Partition Pruning (3 days out of 60)",
        "baseline": f"Unpartitioned ({unpart_duration}s)",
        "optimized": f"Partitioned by pickup_date ({part_duration}s)",
        "speedup": f"{speedup}x",
        "explanation": "Spark skips non-matching partition directories entirely (partition pruning), reading only 3 partition folders instead of scanning all data."
    }


def run_caching_benchmark(spark):
    """
    Benchmark 3: Multi-pass aggregation without vs. with Spark DataFrame caching.
    
    Why Caching wins on iterative operations:
    - Without cache: Spark evaluates the lineage graph and re-reads storage for every terminal action.
    - With cache: The filtered dataset is materialized in off-heap JVM memory on the first pass;
      subsequent actions read directly from RAM in milliseconds.
    """
    print("\n--- Running Benchmark 3: In-Memory Caching (Multi-Pass Aggregation) ---")

    # 1. Without Caching (Re-evaluates lineage each time)
    no_cache_start = time.perf_counter()
    df_base = spark.read.format("delta").load(SILVER_PARTITIONED_PATH).filter(F.col("payment_type") == 1)

    # Pass 1: Fare stats by passenger count
    res1 = df_base.groupBy("passenger_count").agg(F.avg("fare_amount"), F.count("*")).collect()
    # Pass 2: Tip stats by RatecodeID
    res2 = df_base.groupBy("RatecodeID").agg(F.avg("tip_amount"), F.max("trip_distance")).collect()
    # Pass 3: Global averages
    res3 = df_base.agg(F.avg("total_amount"), F.sum("tolls_amount")).collect()

    no_cache_duration = round(time.perf_counter() - no_cache_start, 3)
    print(f"  -> Multi-pass without Cache: {no_cache_duration} s (3 separate full pipeline evaluations)")

    # 2. With Caching
    cache_start = time.perf_counter()
    df_cached = spark.read.format("delta").load(SILVER_PARTITIONED_PATH).filter(F.col("payment_type") == 1).cache()
    # Force materialization into memory
    df_cached.count()

    # Pass 1 from memory
    c_res1 = df_cached.groupBy("passenger_count").agg(F.avg("fare_amount"), F.count("*")).collect()
    # Pass 2 from memory
    c_res2 = df_cached.groupBy("RatecodeID").agg(F.avg("tip_amount"), F.max("trip_distance")).collect()
    # Pass 3 from memory
    c_res3 = df_cached.agg(F.avg("total_amount"), F.sum("tolls_amount")).collect()

    # Unpersist memory
    df_cached.unpersist()

    cache_duration = round(time.perf_counter() - cache_start, 3)
    speedup = round(no_cache_duration / max(cache_duration, 0.001), 2)
    print(f"  -> Multi-pass with Cache: {cache_duration} s (Materialize once + 3 in-memory passes)")
    print(f"  -> Speedup: {speedup}x faster")

    return {
        "test_name": "DataFrame Caching (3 Aggregation Passes)",
        "baseline": f"Without Cache ({no_cache_duration}s)",
        "optimized": f"With .cache() in RAM ({cache_duration}s)",
        "speedup": f"{speedup}x",
        "explanation": "Caching persists intermediate filtered DataFrame in JVM memory, avoiding repeated disk/network reads and redundant lineage execution."
    }


def save_benchmark_artifacts(results):
    """Exports benchmark findings to Markdown and CSV."""
    docs_dir = "/opt/spark/work-dir/docs"
    os.makedirs(docs_dir, exist_ok=True)

    md_path = os.path.join(docs_dir, "benchmark_results.md")
    csv_path = os.path.join(docs_dir, "benchmark_results.csv")

    # Write CSV
    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["test_name", "baseline", "optimized", "speedup", "explanation"])
        writer.writeheader()
        writer.writerows(results)

    # Write Markdown
    md_content = """# Spark Performance Tuning & Optimization Benchmark

Empirical benchmark execution conducted on the **nyc-taxi-lakehouse** dataset running PySpark 3.5.1 on Docker.

## Empirical Benchmark Results

| Optimization Tested | Baseline Execution | Optimized Execution | Speedup | Technical Rationale |
| :--- | :--- | :--- | :--- | :--- |
"""
    for r in results:
        md_content += f"| **{r['test_name']}** | {r['baseline']} | {r['optimized']} | **{r['speedup']}** | {r['explanation']} |\n"

    md_content += """
## Key Interview Talking Points

### 1. Parquet / Delta vs. CSV
- **Columnar Projection:** Parquet stores data in vertical columns with metadata footers. Spark reads *only* the specific columns requested in the query, reducing disk/network I/O by up to 80%. CSV requires full row scanning and string parsing.
- **Type Serialization:** In Parquet, integers and floats are stored in native binary format. In CSV, every value is a string that must be tokenized, regex-parsed, and cast at runtime.

### 2. Partition Pruning vs. Full Table Scan
- **Directory Layout:** Partitioning by `pickup_date` organizes the lakehouse into subdirectories (`pickup_date=2024-01-15/`).
- **Catalyst Optimization:** When a query filters on `pickup_date`, Catalyst's `PartitionPruning` rule drops non-matching directories from the file listing step before tasks are scheduled, skipping ~95% of data.

### 3. In-Memory Caching (`.cache()` / `.persist()`)
- **Execution DAG:** Spark uses lazy evaluation. If a filtered DataFrame is passed to three downstream aggregations without caching, Spark re-reads the source files and re-applies filters three separate times.
- **Memory Storage:** Calling `.cache()` buffers the partitioned dataset in memory (or off-heap memory), allowing subsequent actions to execute in milliseconds without storage I/O.
"""
    with open(md_path, mode="w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"\n[BENCHMARK] Artifacts successfully written:")
    print(f"  - Markdown: {md_path}")
    print(f"  - CSV: {csv_path}")


def main():
    print("=" * 70)
    print("NYC TAXI LAKEHOUSE: SPARK PERFORMANCE BENCHMARK SUITE")
    print("=" * 70)
    start_total = time.perf_counter()

    spark = get_spark_session(app_name="nyc-taxi-benchmark")

    try:
        prepare_benchmark_datasets(spark)

        results = []
        results.append(run_format_benchmark(spark))
        results.append(run_partition_benchmark(spark))
        results.append(run_caching_benchmark(spark))

        save_benchmark_artifacts(results)

        total_sec = round(time.perf_counter() - start_total, 2)
        write_audit_record(
            spark=spark,
            layer="benchmark",
            target_table="performance_metrics",
            rows_in=5443370,
            rows_out=len(results),
            rows_rejected=0,
            status="SUCCESS",
        )

        print("\n" + "=" * 70)
        print(f"ALL BENCHMARKS COMPLETED SUCCESSFULLY IN {total_sec} s!")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
