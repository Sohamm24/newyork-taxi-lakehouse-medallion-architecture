param(
    [Parameter(Position=0)]
    [string]$Target = "help"
)

switch ($Target.ToLower()) {
    "up" {
        Write-Host "Starting core services..." -ForegroundColor Cyan
        docker compose --profile core up -d --build
    }
    "up-orchestration" {
        Write-Host "Starting full orchestration services (Airflow, Postgres, Spark, MinIO, dbt)..." -ForegroundColor Cyan
        docker compose --profile orchestration up -d --build
    }
    "down" {
        Write-Host "Stopping all services..." -ForegroundColor Yellow
        docker compose --profile core --profile orchestration --profile bi down
    }
    "logs" {
        docker compose --profile orchestration logs -f
    }
    "clean" {
        Write-Host "Cleaning containers, volumes, and raw data..." -ForegroundColor Red
        docker compose --profile core --profile orchestration --profile bi down -v
        Remove-Item -Path "data\raw\*.parquet", "data\raw\*.csv", "dbt_project\*.duckdb*" -Force -ErrorAction SilentlyContinue
    }
    "test" {
        docker compose exec spark pytest -v /opt/spark/work-dir/tests
    }
    "run-pipeline" {
        docker compose exec spark python /opt/spark/work-dir/scripts/download_data.py
        docker compose exec spark python /opt/spark/work-dir/scripts/upload_to_minio.py
        docker compose exec spark spark-submit /opt/spark/work-dir/spark_jobs/bronze.py
        docker compose exec spark spark-submit /opt/spark/work-dir/spark_jobs/silver.py
        docker compose exec spark spark-submit /opt/spark/work-dir/spark_jobs/gold.py
    }
    "dbt-build" {
        docker compose exec dbt dbt build --profiles-dir .
    }
    "dbt-test" {
        docker compose exec dbt dbt test --profiles-dir .
    }
    "trigger-dag" {
        docker compose exec airflow airflow dags trigger lakehouse_dag
    }
    "benchmark" {
        Write-Host "Running empirical Spark performance benchmark..." -ForegroundColor Cyan
        docker compose exec spark python /opt/spark/work-dir/scripts/spark_benchmark.py
    }
    "sync-bi" {
        Write-Host "Syncing gold analytics tables and audit logs to Postgres..." -ForegroundColor Cyan
        docker compose exec dbt python /usr/app/scripts/load_gold_to_postgres.py
    }
    "bi" {
        Write-Host "Starting Metabase BI service and provisioning dashboard..." -ForegroundColor Cyan
        docker compose --profile bi up -d
        docker compose exec dbt python /usr/app/scripts/load_gold_to_postgres.py
        docker compose exec spark python /opt/spark/work-dir/scripts/setup_metabase.py
    }
    default {
        Write-Host "Usage: .\make.ps1 [up | up-orchestration | down | logs | clean | test | run-pipeline | dbt-build | dbt-test | trigger-dag | benchmark | sync-bi | bi]" -ForegroundColor Green
    }
}
