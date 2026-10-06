FROM python:3.11-slim

RUN pip install --no-cache-dir \
    dbt-core==1.8.2 \
    dbt-duckdb==1.8.2 \
    duckdb==1.0.0

# Pre-install httpfs extension during image build so dbt runs 100% offline at runtime
RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs;')"

WORKDIR /usr/app/dbt_project
