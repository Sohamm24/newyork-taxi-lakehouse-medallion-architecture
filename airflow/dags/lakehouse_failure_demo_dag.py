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
    "retry_delay": timedelta(seconds=5),  # Short retry delay for fast test validation
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
}

with DAG(
    dag_id="lakehouse_failure_demo_dag",
    default_args=default_args,
    description="Deliberate failure demonstration to prove retries (2) and on_failure_callback diagnostics",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["demo", "failure-test", "retries"],
) as dag:

    # Deliberately attempts to read from a non-existent bucket in MinIO
    failing_task = BashOperator(
        task_id="ingest_invalid_bucket",
        bash_command="docker exec spark python -c \"import boto3; s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin'); s3.list_objects_v2(Bucket='non_existent_bucket_404')\"",
    )
