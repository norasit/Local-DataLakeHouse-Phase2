#!/usr/bin/env python3
"""
Upload NYC Yellow Parquet (host machine → MinIO S3) in Hive-style partitions.

Source (host):
  ./datasets/nyc_tlc/yellow/2024/yellow_tripdata_2024-01.parquet
  ...
  ./datasets/nyc_tlc/yellow/2025/yellow_tripdata_2025-06.parquet

Destination (MinIO, partitioned):
  s3://lakehouse-data/raw/nyc/yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet

Env overrides (optional):
  MINIO_ENDPOINT (default: http://localhost:9000)
  MINIO_ACCESS_KEY (default: test)
  MINIO_SECRET_KEY (default: test12334567)
  MINIO_REGION     (default: us-east-1)
  MINIO_BUCKET     (default: lakehouse-data)
  YELLOW_SRC       (default: ./datasets/nyc_tlc/yellow)
  SKIP_IF_EXISTS   (default: true)   # ถ้ามีไฟล์อยู่แล้วจะข้าม
"""

import os
import re
import sys
from pathlib import Path
from typing import List, Tuple

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

# ----- Config -----
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "test")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "test12334567")
REGION     = os.environ.get("MINIO_REGION", "us-east-1")
BUCKET     = os.environ.get("MINIO_BUCKET", "lakehouse-data")
SRC_ROOT   = Path(os.environ.get("YELLOW_SRC", "./datasets/nyc_tlc/yellow")).resolve()
SKIP_IF_EXISTS = os.environ.get("SKIP_IF_EXISTS", "true").lower() in {"1","true","yes"}

# accept: yellow_tripdata_YYYY-MM.parquet
FILE_RE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$", re.IGNORECASE)


def connect_s3():
    return boto3.resource(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket(s3):
    client = s3.meta.client
    try:
        client.head_bucket(Bucket=BUCKET)
        print(f"• Bucket '{BUCKET}' exists")
    except ClientError:
        print(f"• Creating bucket '{BUCKET}' ...")
        s3.create_bucket(Bucket=BUCKET)


def dest_key_for(local_name: str, year: str, month: str) -> str:
    # >>> สำคัญ: ใช้ layout แบบ Hive partition
    return f"raw/nyc/yellow/year={year}/month={month}/{local_name}"


def find_parquet_files(root: Path) -> List[Tuple[Path, str]]:
    if not root.exists():
        print(f"✖ Source root not found: {root}", file=sys.stderr)
        return []
    pairs: List[Tuple[Path, str]] = []
    for p in root.rglob("*.parquet"):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        year, month = m.group(1), m.group(2)
        key = dest_key_for(p.name, year, month)
        pairs.append((p, key))
    return pairs


def object_exists(bucket, key: str) -> bool:
    try:
        bucket.Object(key).load()
        return True
    except ClientError as e:
        if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
            return False
        raise


def main() -> int:
    print("=== NYC Yellow → MinIO uploader (Hive partitions) ===")
    print(f"Endpoint : {ENDPOINT}")
    print(f"Bucket   : {BUCKET}")
    print(f"Source   : {SRC_ROOT}")
    print(f"Skip if exists: {SKIP_IF_EXISTS}")

    try:
        s3 = connect_s3()
        _ = [b.name for b in s3.buckets.all()]  # touch endpoint
    except EndpointConnectionError as e:
        print(f"✖ Cannot reach MinIO endpoint {ENDPOINT}\n  -> {e}", file=sys.stderr)
        return 2

    ensure_bucket(s3)
    bucket = s3.Bucket(BUCKET)

    pairs = find_parquet_files(SRC_ROOT)
    if not pairs:
        print("✖ No matching Parquet files found (yellow_tripdata_YYYY-MM.parquet)")
        return 1

    uploaded = 0
    for local_path, key in sorted(pairs):
        if SKIP_IF_EXISTS and object_exists(bucket, key):
            print(f"↷ Skip exists: s3://{BUCKET}/{key}")
            continue
        print(f"→ {local_path}  ->  s3://{BUCKET}/{key}")
        bucket.upload_file(
            str(local_path),
            key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
        uploaded += 1

    print(f"• Uploaded {uploaded} file(s)")

    print("\nSample under s3://{}/raw/nyc/yellow/ :".format(BUCKET))
    for i, obj in enumerate(bucket.objects.filter(Prefix="raw/nyc/yellow/")):
        print("  -", obj.key)
        if i >= 19:
            break
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
