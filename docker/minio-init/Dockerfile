FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir boto3==1.34.131

COPY create_buckets.py /app/create_buckets.py

ENTRYPOINT ["python", "/app/create_buckets.py"]
