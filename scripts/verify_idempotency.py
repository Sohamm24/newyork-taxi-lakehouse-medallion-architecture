import sys
from spark_jobs.common import get_spark_session, get_s3_path
from spark_jobs.bronze import run_bronze_ingestion
from spark_jobs.silver import run_silver_pipeline
from spark_jobs.gold import run_gold_pipeline

def get_table_counts(spark):
    tables = {
        "bronze_trips": get_s3_path("bronze", "yellow_taxi_raw"),
        "bronze_zones": get_s3_path("bronze", "taxi_zone_lookup"),
        "silver_trips": get_s3_path("silver", "yellow_taxi"),
        "gold_daily": get_s3_path("gold", "daily_trips_revenue"),
        "gold_zones": get_s3_path("gold", "pickup_zone_metrics"),
        "gold_hourly": get_s3_path("gold", "hourly_fare_tip_metrics"),
    }
    counts = {}
    for name, path in tables.items():
        counts[name] = spark.read.format("delta").load(path).count()
    return counts

def main():
    print("=== IDEMPOTENCY PROOF TEST ===")
    spark = get_spark_session("idempotency-check")
    
    print("\n[Step 1] Recording Baseline Row Counts...")
    baseline_counts = get_table_counts(spark)
    for name, count in baseline_counts.items():
        print(f"  {name:15}: {count:,} rows")
    spark.stop()

    print("\n[Step 2] Executing Full Pipeline Re-run (Bronze -> Silver -> Gold)...")
    run_bronze_ingestion()
    run_silver_pipeline()
    run_gold_pipeline()

    print("\n[Step 3] Recording Post-Rerun Row Counts & Validating Equality...")
    spark2 = get_spark_session("idempotency-verify")
    post_counts = get_table_counts(spark2)
    spark2.stop()

    all_matched = True
    print("\n=== IDEMPOTENCY VERIFICATION RESULTS ===")
    print(f"{'Table':18} | {'Run 1 Count':12} | {'Run 2 Count':12} | {'Diff':6} | {'Status'}")
    print("-" * 65)

    for name in baseline_counts:
        c1 = baseline_counts[name]
        c2 = post_counts[name]
        diff = c2 - c1
        status = "PASSED (IDEMPOTENT)" if diff == 0 else "FAILED (DUPLICATES)"
        if diff != 0:
            all_matched = False
        print(f"{name:18} | {c1:12,d} | {c2:12,d} | {diff:6d} | {status}")

    if all_matched:
        print("\nALL IDEMPOTENCY CHECKS PASSED: ZERO DUPLICATES DETECTED!")
    else:
        print("\nIDEMPOTENCY FAILURE: Row counts differ between runs!", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
