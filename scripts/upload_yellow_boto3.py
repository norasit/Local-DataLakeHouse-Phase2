#!/usr/bin/env python3
"""
Upload NYC Yellow Parquet (host machine → MinIO S3)

Source (host):
  ./datasets/nyc_tlc/yellow/2024/yellow_tripdata_2024-01.parquet
  ./datasets/nyc_tlc/yellow/2024/yellow_tripdata_2024-02.parquet
  ...
  ./datasets/nyc_tlc/yellow/2025/yellow_tripdata_2025-06.parquet

Destination (MinIO):
  s3://lakehouse-data/raw/nyc/yellow/<year>/<month>/yellow_tripdata_YYYY-MM.parquet

Run on the EC2 host that also runs dockerized MinIO.
Defaults can be overridden via env vars:
  MINIO_ENDPOINT (default: http://localhost:9000)
  MINIO_ACCESS_KEY (default: test)
  MINIO_SECRET_KEY (default: test12334567)
  MINIO_REGION (default: us-east-1)
  MINIO_BUCKET (default: lakehouse-data)
  YELLOW_SRC (default: ./datasets/nyc_tlc/yellow)
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Tuple

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

# ----- Config (with sensible defaults for your compose) -----
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "test")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "test12334567")
REGION     = os.environ.get("MINIO_REGION", "us-east-1")
BUCKET     = os.environ.get("MINIO_BUCKET", "lakehouse-data")
SRC_ROOT   = Path(os.environ.get("YELLOW_SRC", "./datasets/nyc_tlc/yellow")).resolve()

# Files we accept: yellow_tripdata_YYYY-MM.parquet (case-insensitive)
FILE_RE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$", re.IGNORECASE)


def connect_s3():
    return boto3.resource(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(signature_version="s3v4",
                      s3={"addressing_style": "path"}),  # important for MinIO
    )


def ensure_bucket(s3):
    client = s3.meta.client
    try:
        client.head_bucket(Bucket=BUCKET)
        print(f"• Bucket '{BUCKET}' exists")
    except ClientError:
        print(f"• Creating bucket '{BUCKET}' ...")
        s3.create_bucket(Bucket=BUCKET)


def find_parquet_files(root: Path) -> List[Tuple[Path, str]]:
    """
    Walk SRC_ROOT recursively, return list of (local_path, s3_key)
    """
    if not root.exists():
        print(f"✖ Source root not found: {root}", file=sys.stderr)
        return []

    results: List[Tuple[Path, str]] = []
    for p in root.rglob("*.parquet"):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        year, month = m.group(1), m.group(2)
        key = f"raw/nyc/yellow/{year}/{month}/{p.name}"
        results.append((p, key))
    return results


def main() -> int:
    print("=== NYC Yellow → MinIO uploader ===")
    print(f"Endpoint : {ENDPOINT}")
    print(f"Bucket   : {BUCKET}")
    print(f"Source   : {SRC_ROOT}")

    # Connect & sanity check endpoint
    try:
        s3 = connect_s3()
        buckets = [b.name for b in s3.buckets.all()]
        print(f"Buckets  : {buckets}")
    except EndpointConnectionError as e:
        print(f"✖ Cannot reach MinIO endpoint {ENDPOINT}\n  -> {e}", file=sys.stderr)
        return 2

    # Ensure bucket exists
    ensure_bucket(s3)
    bucket = s3.Bucket(BUCKET)

    # Discover files
    pairs = find_parquet_files(SRC_ROOT)
    if not pairs:
        print("✖ No matching Parquet files found. Expected pattern: yellow_tripdata_YYYY-MM.parquet")
        return 1

    # Upload
    uploaded = 0
    for local_path, key in pairs:
        # Extra safety: ensure parent path exists locally
        if not local_path.is_file():
            print(f"  ! Skip (not a file): {local_path}")
            continue
        print(f"→ {local_path}  ->  s3://{BUCKET}/{key}")
        bucket.upload_file(str(local_path), key)
        uploaded += 1

    print(f"• Uploaded {uploaded} file(s)")

    # Show a sample listing under the prefix
    print("\nSample objects under s3://{}/raw/nyc/yellow/ :".format(BUCKET))
    count = 0
    for obj in bucket.objects.filter(Prefix="raw/nyc/yellow/"):
        print("  -", obj.key)
        count += 1
        if count >= 20:
            break
    if count == 0:
        print("  (none)")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
