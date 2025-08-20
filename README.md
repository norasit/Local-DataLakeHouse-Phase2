# Local Data Lakehouse (Phase 1)

This project sets up a **Local Data Lakehouse** environment using:
- **MinIO** (S3-compatible object storage)
- **Trino** 476 (SQL query engine)
- **Apache Iceberg** (table format)
- **Hive Metastore** (metadata service for Hive & Iceberg in Trino)
Target use case: learn the end-to-end flow from raw Parquet files → Hive external table → managed Iceberg table.

---

## Dataset
**NYC Taxi Trip Records – Yellow Taxi**  
**Period:** January 2024 – June 2025  
**Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Raw Parquet files will live in s3://lakehouse-data/raw/nyc/yellow/year=<YYYY>/month=<MM>/….

---

## Prerequisites
- Docker & Docker Compose
- Bash / curl
- (Optional) Python 3.10+ if you want to use the upload helper script

---

## 1) Download the dataset (to your host)
This pulls Parquet files into ./datasets/nyc_tlc/yellow/<year>/yellow_tripdata_YYYY-MM.parquet.

```bash
chmod +x scripts/download_tlc_yellow.sh
scripts/download_tlc_yellow.sh
```

The files can be large—make sure you have disk space.
Dataset files are not checked into Git.

---

## 2) Get the Postgres JDBC driver (for Hive Metastore)
```bash
chmod +x scripts/get_postgres_jdbc.sh
scripts/get_postgres_jdbc.sh
```

This downloads postgresql-<version>.jar into ./jars/ (not committed to Git).

---

## 3) Get Hadoop/AWS jars for S3A (for Hive in the metastore container)
```bash
chmod +x scripts/get_hms_s3a_jars.sh
scripts/get_hms_s3a_jars.sh
```

This downloads the versioned jars and also writes stable filenames in ./jars/
(hadoop-aws.jar, aws-sdk-bundle.jar) so the Docker volumes do not change when you
bump versions.

---

## 4) Start the stack
```bash
docker compose up -d
docker compose d
```

The compose file:
Starts **Postgres** (for Hive Metastore), **Hive Metastore**, **MinIO**, **Trino**
Creates the MinIO bucket ```lakehouse-data``` and the prefixes ```raw/``` and ```warehouse/``` (via a short-lived ```minio-mc``` init container)

MinIO console: http://localhost:9001
Access Key: ```test```
Secret Key: ```test12334567```

---

## 5) Put the raw Parquet files into MinIO
### Option A — Manual (via browser)
Open http://localhost:9001 → bucket lakehouse-data → upload files under:
```bash
raw/nyc/yellow/year=<YYYY>/month=<MM>/
```

Example:
```bash
raw/nyc/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet
raw/nyc/yellow/year=2024/month=02/yellow_tripdata_2024-02.parquet
...
```

### Option B — Python helper (from the host)
Installs ```boto3``` and uploads from ```./datasets/nyc_tlc/yellow/**.parquet```
to the correct S3 paths.

```bash
# (optional but recommended)
python3 -m venv .venv
source .venv/bin/activate
pip install boto3

# run after MinIO is up
python3 scripts/upload_yellow_boto3.py
```

Defaults used by the script (override via env vars if needed):
- MINIO_ENDPOINT=http://localhost:9000
- MINIO_ACCESS_KEY=test
- MINIO_SECRET_KEY=test12334567
- MINIO_REGION=us-east-1
- MINIO_BUCKET=lakehouse-data

---

## 6) Query with Trino
Trino web: http://localhost:8080
Trino CLI inside the container:
```bash
-- Change 'local-datalakehouse-phase1-trino-1' to your trino container name or container id.
docker exec -it local-datalakehouse-phase1-trino-1 bash
trino
```

**Quick start SQL**
Full, step-by-step guide is in ```SQL_QUERIES.md``` (written for Trino 476).
Below is the minimal happy path.

---

## 7) Common operations
Add new raw files later:
```bash
-- Refresh Hive after adding a new month in raw/nyc/yellow/year=YYYY/month=MM/...
CALL hive.system.sync_partition_metadata('raw', 'nyc_yellow', 'INCREMENTAL');

-- Ingest only the new month into Iceberg
INSERT INTO iceberg.lh.nyc_yellow
SELECT *
FROM hive.raw.nyc_yellow
WHERE year = 2025 AND month = 7;
```

Iceberg maintenance:
```bash
-- Remove old snapshots
CALL iceberg.system.expire_snapshots('lh', 'nyc_yellow', TIMESTAMP '2025-08-15 00:00:00');

-- Compact small files (optional, improves read performance)
ALTER TABLE iceberg.lh.nyc_yellow EXECUTE optimize;
```

Reset / clean up:
```bash
DROP TABLE IF EXISTS iceberg.lh.nyc_yellow;   -- removes Iceberg data+metadata under warehouse
DROP TABLE IF EXISTS hive.raw.nyc_yellow;     -- removes only metadata; raw files remain
```

---

**Troubleshooting**
- **Can’t see the bucket/prefixes in MinIO?**
The minio-mc init container should create them and then exit with code 0.
If needed, create the bucket lakehouse-data and folders raw/, warehouse/ in the UI.
- **Partition discovery fails:**
Use CALL hive.system.sync_partition_metadata(...) (note the hive. qualifier).
CALL system.sync_partition_metadata(...) is not valid for the Hive connector.
- **Quoting Iceberg metadata tables:**
Use double quotes for names with $, e.g. iceberg."lh"."nyc_yellow$files".
- **CTAS (Hive → Iceberg) uses a lot of RAM/CPU:**
That’s expected—it rewrites data and builds Iceberg metadata. Run per-month and INSERT if needed.

---

## License
This project is for educational purposes only.
