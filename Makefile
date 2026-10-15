.PHONY: help up up-orchestration down logs clean test run-pipeline dbt-build dbt-test trigger-dag

help:
	@echo "Available commands:"
	@echo "  make up               - Build and start core lakehouse services (MinIO, Spark, dbt)"
	@echo "  make up-orchestration - Start complete orchestration profile (Airflow, Postgres, MinIO, Spark, dbt)"
	@echo "  make down             - Stop all running containers across profiles"
	@echo "  make logs             - View live logs of core services"
	@echo "  make clean            - Stop containers and purge volumes and local raw files"
	@echo "  make test             - Run pytest unit tests inside the Spark container"
	@echo "  make run-pipeline     - Run manual batch pipeline CLI (download -> upload -> bronze -> silver -> gold)"
	@echo "  make dbt-build        - Build dbt models and execute all 16 data quality tests"
	@echo "  make trigger-dag      - Trigger the end-to-end lakehouse_dag in Airflow"
	@echo "  make benchmark        - Run empirical Spark performance benchmark suite"
	@echo "  make bi               - Launch Metabase BI container, sync gold data, and provision dashboard"
	@echo "  make sync-bi          - Sync gold tables and pipeline audit logs from MinIO to Postgres"

up:
	docker compose --profile core up -d --build

up-orchestration:
	docker compose --profile orchestration up -d --build

down:
	docker compose --profile core --profile orchestration --profile bi down

logs:
	docker compose --profile orchestration logs -f

clean:
	docker compose --profile core --profile orchestration --profile bi down -v
	rm -rf data/raw/*.parquet data/raw/*.csv dbt_project/target/ dbt_project/*.duckdb*

test:
	docker compose exec spark pytest -v /opt/spark/work-dir/tests

run-pipeline:
	docker compose exec spark python /opt/spark/work-dir/scripts/download_data.py
	docker compose exec spark python /opt/spark/work-dir/scripts/upload_to_minio.py
	docker compose exec spark spark-submit /opt/spark/work-dir/spark_jobs/bronze.py
	docker compose exec spark spark-submit /opt/spark/work-dir/spark_jobs/silver.py
	docker compose exec spark spark-submit /opt/spark/work-dir/spark_jobs/gold.py

dbt-build:
	docker compose exec dbt dbt build --profiles-dir .

dbt-test:
	docker compose exec dbt dbt test --profiles-dir .

trigger-dag:
	docker compose exec airflow airflow dags trigger lakehouse_dag

benchmark:
	docker compose exec spark python /opt/spark/work-dir/scripts/spark_benchmark.py

sync-bi:
	docker compose exec dbt python /usr/app/scripts/load_gold_to_postgres.py

bi:
	docker compose --profile bi up -d
	docker compose exec dbt python /usr/app/scripts/load_gold_to_postgres.py
	docker compose exec spark python /opt/spark/work-dir/scripts/setup_metabase.py
