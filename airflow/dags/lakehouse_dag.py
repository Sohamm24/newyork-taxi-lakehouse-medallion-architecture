import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

logger = logging.getLogger("airflow.task")

def task_failure_callback(context):
    """
    Failure callback logging explicit diagnostics when an orchestration task fails.
    Captures task_id, dag_id, execution date, try number, and exception details.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    try_number = task_instance.try_number if task_instance else "unknown"
    exception = context.get("exception")

    error_msg = f"""
    ================================================================================
    [PIPELINE ALERT] Task Failure in DAG: '{dag_id}'
    Task Name:    {task_id}
    Try Number:   {try_number}
    Execution Ts: {context.get('execution_date')}
    Exception:    {exception}
    ================================================================================
    """
    logger.error(error_msg)
    print(error_msg)

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    dag_id="lakehouse_dag",
    default_args=default_args,
    description="End-to-end Lakehouse batch pipeline: Ingestion -> Medallion (Bronze/Silver/Gold) -> dbt tests",
    schedule_interval=None,  # Manual / triggered execution
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "nyc-taxi", "delta-lake", "dbt"],
) as dag:

    # 1. Download raw NYC Yellow taxi parquet files and taxi zone lookup CSV locally
    download_task = BashOperator(
        task_id="download_raw_data",
        bash_command="docker exec spark python /opt/spark/work-dir/scripts/download_data.py --months 2024-01,2024-02",
    )

    # 2. Upload raw files to MinIO bronze bucket landing area
    upload_task = BashOperator(
        task_id="upload_to_minio",
        bash_command="docker exec spark python /opt/spark/work-dir/scripts/upload_to_minio.py",
    )

    # 3. Bronze Layer: Raw ingestion to Delta Lake with metadata lineage columns
    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command="docker exec spark spark-submit /opt/spark/work-dir/spark_jobs/bronze.py",
    )

    # 4. Silver Layer: Schema enforcement, validation rules, quarantine, and deduplication
    silver_task = BashOperator(
        task_id="silver_cleaning",
        bash_command="docker exec spark spark-submit /opt/spark/work-dir/spark_jobs/silver.py",
    )

    # 5. Gold Layer: Analytical aggregations and clean Parquet export
    gold_task = BashOperator(
        task_id="gold_aggregation",
        bash_command="docker exec spark spark-submit /opt/spark/work-dir/spark_jobs/gold.py",
    )

    # 6. dbt Data Quality Tests: Unique, not_null, accepted ranges, and custom positive trip assertions
    dbt_test_task = BashOperator(
        task_id="dbt_data_quality_tests",
        bash_command="docker exec dbt dbt build --profiles-dir .",
    )

    # Pipeline task dependency DAG definition
    download_task >> upload_task >> bronze_task >> silver_task >> gold_task >> dbt_test_task
