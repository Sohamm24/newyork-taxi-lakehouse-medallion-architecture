import os
import sys
import time
import boto3
from botocore.client import Config

# Retrieve MinIO parameters from environment variables
endpoint_url = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
buckets_csv = os.getenv("MINIO_BUCKETS", "bronze,silver,gold,audit")
buckets = [b.strip() for b in buckets_csv.split(",") if b.strip()]

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

print(f"Connecting to MinIO at {endpoint_url}...")
max_retries = 30
for attempt in range(1, max_retries + 1):
    try:
        s3.list_buckets()
        print("Successfully connected to MinIO.")
        break
    except Exception as e:
        print(f"Waiting for MinIO readiness (attempt {attempt}/{max_retries}): {e}")
        time.sleep(2)
else:
    print("Error: Could not connect to MinIO within timeout period.")
    sys.exit(1)

# Inspect existing buckets and create missing ones
response = s3.list_buckets()
existing_buckets = [b["Name"] for b in response.get("Buckets", [])]

for bucket in buckets:
    if bucket in existing_buckets:
        print(f"Bucket '{bucket}' already exists.")
    else:
        print(f"Creating bucket '{bucket}'...")
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' created successfully.")

print("All required buckets (bronze, silver, gold, audit) are ready!")
