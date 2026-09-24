import os
import sys
import argparse
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from pathlib import Path

def get_s3_client(endpoint_url: str, access_key: str, secret_key: str):
    """Initializes and returns a boto3 S3 client configured for local MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def is_already_uploaded(s3_client, bucket: str, s3_key: str, local_size: int) -> bool:
    """Checks whether the object already exists in MinIO with the same byte size."""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        remote_size = response.get("ContentLength", 0)
        return remote_size == local_size
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        # If head_object throws other errors, assume not uploaded
        return False

def upload_file(s3_client, local_path: Path, bucket: str, s3_key: str, force: bool = False) -> None:
    """Uploads a local file to MinIO with idempotency check."""
    local_size = local_path.stat().st_size
    size_mb = local_size / (1024 * 1024)

    if not force and is_already_uploaded(s3_client, bucket, s3_key, local_size):
        print(f"Skipping {s3_key}: already present in bucket '{bucket}' ({size_mb:.2f} MB).")
        return

    print(f"Uploading {local_path.name} ({size_mb:.2f} MB) -> s3a://{bucket}/{s3_key}...")
    s3_client.upload_file(str(local_path), bucket, s3_key)
    print(f"Successfully uploaded {s3_key} to bucket '{bucket}'.")

def main() -> None:
    parser = argparse.ArgumentParser(description="Upload raw data files to MinIO bronze bucket")
    parser.add_argument(
        "--source-dir",
        type=str,
        default=os.getenv("RAW_DATA_DIR", "data/raw"),
        help="Local directory containing raw data files (default: data/raw)",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.getenv("BUCKET_BRONZE", "bronze"),
        help="MinIO target bucket (default: bronze)",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="raw",
        help="Target S3 key prefix (default: raw)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-upload even if object exists with matching size",
    )
    args = parser.parse_args()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    user = os.getenv("MINIO_ROOT_USER", "minioadmin")
    password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    s3 = get_s3_client(endpoint, user, password)
    source_dir = Path(args.source_dir).resolve()

    if not source_dir.exists():
        print(f"Error: source directory '{source_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)

    files_to_upload = sorted(list(source_dir.glob("*.parquet")) + list(source_dir.glob("*.csv")))
    if not files_to_upload:
        print(f"No parquet or csv files found in {source_dir}.", file=sys.stderr)
        sys.exit(1)

    print(f"=== MinIO Raw Data Uploader ===")
    print(f"Source Directory: {source_dir}")
    print(f"MinIO Endpoint:   {endpoint}")
    print(f"Target Bucket:    {args.bucket}")
    print(f"Found {len(files_to_upload)} file(s) to upload.\n")

    for file_path in files_to_upload:
        s3_key = f"{args.prefix}/{file_path.name}" if args.prefix else file_path.name
        upload_file(s3, file_path, args.bucket, s3_key, force=args.force)

    print("\nAll files uploaded to MinIO successfully!")

if __name__ == "__main__":
    main()
