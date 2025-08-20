* Trino 476 – Lakehouse quick guide (Hive → Iceberg)
Assumptions
- Object store (MinIO/S3) bucket: lakehouse-data
- Raw files live under s3://lakehouse-data/raw/...
- Iceberg tables live under s3://lakehouse-data/warehouse/...
- Trino catalogs: hive and iceberg are configured and working

## 1) Check catalogs are available
```sql
SHOW CATALOGS;
-- Expect to see: hive, iceberg, system, ...
```

---

## 2) Create base schemas (idempotent)
```sql
-- Hive: points to physical raw files (external data)
CREATE SCHEMA IF NOT EXISTS hive.raw
WITH (location = 's3://lakehouse-data/raw/');

-- Iceberg: where managed Iceberg tables will be written
CREATE SCHEMA IF NOT EXISTS iceberg.lh
WITH (location = 's3://lakehouse-data/warehouse/lh/');
```
Optional verification:
```bash
SHOW SCHEMAS FROM hive;
SHOW SCHEMAS FROM iceberg;
```

---

## 3) Register a Hive external table over the raw files
```sql
CREATE TABLE IF NOT EXISTS hive.raw.nyc_yellow (
  vendorid                 INTEGER,
  tpep_pickup_datetime     TIMESTAMP,
  tpep_dropoff_datetime    TIMESTAMP,
  passenger_count          BIGINT,
  trip_distance            DOUBLE,
  ratecodeid               BIGINT,
  store_and_fwd_flag       VARCHAR,
  pulocationid             INTEGER,
  dolocationid             INTEGER,
  payment_type             INTEGER,
  fare_amount              DOUBLE,
  extra                    DOUBLE,
  mta_tax                  DOUBLE,
  tip_amount               DOUBLE,
  tolls_amount             DOUBLE,
  improvement_surcharge    DOUBLE,
  total_amount             DOUBLE,
  congestion_surcharge     DOUBLE,
  airport_fee              DOUBLE,
  year                     INTEGER,
  month                    INTEGER
)
WITH (
  external_location = 's3://lakehouse-data/raw/nyc/yellow/',
  format           = 'PARQUET',
  partitioned_by   = ARRAY['year','month']
);
```
**Why**: this tells Hive where to read the Parquet files (it does not move or rewrite them).

Load partitions from the folder layout (year=YYYY/month=MM/...):
```bash
CALL hive.system.sync_partition_metadata('raw', 'nyc_yellow', 'FULL');
```

---

## 4) Inspect the Hive table
Prefer the Hive “SHOW PARTITIONS” statement (works across Trino versions):
```sql
SHOW PARTITIONS FROM hive.raw.nyc_yellow ORDER BY year, month;

-- sanity checks
SELECT COUNT(*) FROM hive.raw.nyc_yellow;

SELECT year, month, COUNT(*) AS rows
FROM hive.raw.nyc_yellow
GROUP BY 1,2
ORDER BY 1,2;
```

---

## 5) Create an Iceberg table from Hive (CTAS)
```sql
CREATE TABLE IF NOT EXISTS iceberg.lh.nyc_yellow
WITH (
  format       = 'PARQUET',
  partitioning = ARRAY['year','month']
) AS
SELECT *
FROM hive.raw.nyc_yellow;
```
**What happens**: Trino reads the raw files through Hive and rewrites them as a managed Iceberg table under s3://lakehouse-data/warehouse/lh.db/nyc_yellow/... (data + metadata).
This step is compute-heavy (RAM/CPU/IO): files are rewritten and Iceberg metadata is built.

---

## 6) Inspect the Iceberg table & metadata
```sql
-- row count
SELECT count(*) FROM iceberg.lh.nyc_yellow;

-- Iceberg metadata tables (note the quoting of identifiers with $)
SELECT * FROM iceberg."lh"."nyc_yellow$snapshots"  ORDER BY committed_at DESC LIMIT 10;
SELECT * FROM iceberg."lh"."nyc_yellow$history"    ORDER BY made_current_at DESC LIMIT 10;
SELECT * FROM iceberg."lh"."nyc_yellow$manifests"  LIMIT 10;
SELECT * FROM iceberg."lh"."nyc_yellow$files"      LIMIT 10;

-- partitions is a ROW containing your partition fields (year, month)
SELECT
  partition.year  AS year,
  partition.month AS month,
  file_count,
  record_count,
  total_size
FROM "iceberg"."lh"."nyc_yellow$partitions"
ORDER BY partition.year, partition.month;
```
**Glossary**
- $snapshots: versioned snapshots of the table
- $history: snapshot switch history
- $manifests, $files: Iceberg’s file-level metadata
- $partitions: partition summary (fields are nested under partition)

---

## 7) Query examples on Iceberg
```sql
-- monthly stats
SELECT year, month, avg(fare_amount) AS avg_fare, count(*) AS rows
FROM iceberg.lh.nyc_yellow
GROUP BY 1,2
ORDER BY 1,2;

-- month filter
SELECT avg(fare_amount)
FROM iceberg.lh.nyc_yellow
WHERE year = 2024 AND month = 8;
```

---

## 8) (Optional) Iceberg maintenance
```sql
-- remove snapshots older than a timestamp
CALL iceberg.system.expire_snapshots('lh', 'nyc_yellow', TIMESTAMP '2025-08-15 00:00:00');

-- compact small data files into larger ones (improves read performance)
ALTER TABLE iceberg.lh.nyc_yellow EXECUTE optimize;

-- (optional) analyze statistics to help the optimizer
ANALYZE iceberg.lh.nyc_yellow;
```
Note: Exact maintenance options available can vary slightly by Trino version; the above works on 476.

---

## 9) When you add new raw files later
After uploading additional monthly Parquet under raw/nyc/yellow/year=YYYY/month=MM/..., refresh Hive’s external table partitions:
```sql
CALL system.sync_partition_metadata('raw', 'nyc_yellow', 'INCREMENTAL');
```
Then ingest into Iceberg either:
```bash
-- append only the new month(s)
INSERT INTO iceberg.lh.nyc_yellow
SELECT *
FROM hive.raw.nyc_yellow
WHERE year = 2025 AND month = 7;  -- example
```

---

## 10) Cleanup / reset
```sql
-- 1) Drop the Iceberg table (removes metadata and data files of this table)
DROP TABLE IF EXISTS iceberg.lh.nyc_yellow;

-- 2) Drop the Iceberg schema (if empty)
DROP SCHEMA IF EXISTS iceberg.lh CASCADE;

-- 3) Drop the Hive table (removes only metadata; raw files remain)
DROP TABLE IF EXISTS hive.raw.nyc_yellow;

-- 4) Drop the Hive schema (if you truly want a fresh start)
DROP SCHEMA IF EXISTS hive.raw CASCADE;
```
**Notes**
- Dropping the Iceberg table removes the table’s managed data/metadata under warehouse/lh.db/....
- Dropping the Hive external table removes only the metastore entry; files under raw/... remain untouched.

---

**Small tips**
- Quoting for metadata tables: use double-quotes around names with $ (e.g., "nyc_yellow$files").
- Iceberg $partitions: partition fields are nested under the partition column (a ROW), so use partition.year, partition.month.
- Hive partitions: prefer SHOW PARTITIONS FROM hive.raw.nyc_yellow over selecting from a $partitions table to avoid version differences.
- Qualify calls: for Hive metadata sync, always call hive.system.sync_partition_metadata(...) (not system.sync_partition_metadata(...)).
- Performance: CTAS (Hive → Iceberg) rewrites data and can be memory/IO intensive. Run it in chunks if needed (e.g., per month) and then INSERT into Iceberg.