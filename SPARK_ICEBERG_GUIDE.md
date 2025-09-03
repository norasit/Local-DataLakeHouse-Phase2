# Spark + Iceberg Guide

This document provides a **hands-on runbook** for experimenting with [Apache Iceberg](https://iceberg.apache.org/) tables using **Spark SQL** on top of a local data lakehouse environment (MinIO + Nessie + Trino + Spark).  

While the main [README.md](./README.md) explains how to **set up and run the environment**, this guide focuses on **using Spark SQL to interact with Iceberg tables**, especially around *maintenance operations* such as compaction, manifest rewriting, snapshot management, and orphan file cleanup.

---

## Scope of this guide

✅ Explore catalogs, namespaces, and Iceberg tables with Spark SQL  
✅ Insert and generate data to create **many small files** (the “small-file problem”)  
✅ Inspect Iceberg metadata (snapshots, manifests, file listings)  
✅ Perform **maintenance procedures** with Spark SQL, including:  
- Data compaction (`rewrite_data_files`)  
- Manifest compaction (`rewrite_manifests`)  
- Delete file compaction (`rewrite_position_deletes`, `rewrite_equality_deletes`)  
- Snapshot expiration (`expire_snapshots`)  
- Orphan file removal (`remove_orphan_files`)  
- Table diagnostics, validation, and rollback  

---

## Who is this for?

This guide is intended for:  
- **Learners** who want to understand how Iceberg organizes data and metadata.  
- **Data engineers** who want a repeatable local environment to practice Spark + Iceberg maintenance.  
- **Project contributors** who need a structured reference for testing Iceberg features.  

---

## How to use

1. **Start from a clean environment** — follow setup in [README.md](./README.md).  
2. Open a `spark-sql` shell inside the Spark container.  
3. Run the steps in order (sections 0 → 8) to create tables, generate data, observe metadata/files in MinIO, and then apply maintenance jobs.  
4. Compare pre- and post-maintenance states to see how Iceberg optimizes file layout.  

---

⚠️ **Timezone Note**:  
All timestamps in Iceberg procedures (e.g., `expire_snapshots`, `remove_orphan_files`) are interpreted in **UTC**.  
Your Docker containers are already configured with `TZ=UTC`, so you don’t need to manually adjust timezones in SQL. If you plan to run in another environment (e.g., EC2 in Singapore), adjust timestamps accordingly.

---

## Spark + Iceberg Maintenance Runbook (with MinIO + Nessie)
## 0) Reset Environment
```bash
rm -rf minio-data/lakehouse-data
rm -rf nessie-data/*
```

- Wipes MinIO data (bucket ```lakehouse-data```) and Nessie catalog metadata.
- Ensures no old namespaces, tables, or files remain.
- Airflow/Trino/Spark configs are untouched.
**Result in MinIO**: bucket exists but is empty (re-created at startup).

---

## 1) Start Containers
```bash
docker compose up -d --build
```
- Starts MinIO, Nessie, Spark, Trino, and Airflow.
- MinIO client (```minio-mc```) auto-creates bucket ```lakehouse-data/``` with prefixes ```warehouse/``` and ```raw/```.
Verify:
```bash
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
```

---

## 2) Open Spark SQL Shell
```bash
docker exec -it local-datalakehouse-phase2-spark-1 spark-sql
```
Check catalogs:
```sql
SHOW CATALOGS;
-- Expect: nessie, spark_catalog
```

---

## 3) Create & Materialize Namespace
```sql
CREATE NAMESPACE IF NOT EXISTS nessie.lab
WITH PROPERTIES ('comment'='lab namespace');

SHOW NAMESPACES IN nessie;
DESCRIBE NAMESPACE EXTENDED nessie.lab;
```
- **Nessie change**: new namespace appears.
- **MinIO**: no changes yet (namespace is metadata only).

---

## 4) Create Iceberg Table (tuned for small files)
```sql
CREATE TABLE IF NOT EXISTS nessie.lab.small_files (
  id BIGINT,
  name STRING
) USING iceberg
TBLPROPERTIES (
  'format-version'='2',
  'write.target-file-size-bytes'='65536',  -- ~64KB target
  'write.distribution-mode'='none'         -- minimal shuffling
);
```
Verify:
```sql
SHOW TABLES IN nessie.lab;
DESCRIBE TABLE EXTENDED nessie.lab.small_files;
```
**MinIO change**:
New directory appears under ```warehouse/lab/small_files_<uuid>/metadata/```.

---

## 5) Insert Initial Rows (creates first snapshot)
```sql
INSERT INTO nessie.lab.small_files VALUES
  (1,'alice'),
  (2,'bob'),
  (3,'charlie');

SELECT * FROM nessie.lab.small_files ORDER BY id;

-- Inspect snapshots
SELECT snapshot_id, committed_at, operation
FROM nessie.lab.small_files.snapshots
ORDER BY committed_at DESC;
```
**MinIO change**:
- ```data/``` contains small Parquet files.
- ```metadata/``` updated with ```v1.metadata.json``` and snapshot Avro files.

---

## 6) Generate Many Small Files (learning target)
### **Option A: Small appends**
```sql
INSERT INTO nessie.lab.small_files VALUES (4,'d'),(5,'e'),(6,'f');
INSERT INTO nessie.lab.small_files VALUES (7,'g'),(8,'h'),(9,'i');
INSERT INTO nessie.lab.small_files VALUES (10,'j'),(11,'k'),(12,'l');
```
## **Option B: Generator**
```sql
INSERT INTO nessie.lab.small_files
SELECT id, CONCAT('name_', CAST(id AS STRING))
FROM RANGE(0, 200);
```
## **Option C: Force tiny partitions**
```sql
SET spark.sql.shuffle.partitions=4;

INSERT INTO nessie.lab.small_files
SELECT id, CONCAT('bulk_', CAST(id AS STRING))
FROM RANGE(200, 1000);
```
**Goal**: produce many tiny Parquet files.
**Verify in Spark** (optional):
```sql
SELECT COUNT(*) FROM nessie.lab.small_files;
```

---

## 7) Verify “Small-File Explosion”
Check current state before maintenance:
```sql
-- Count data files + size
SELECT COUNT(*) AS data_files,
       CAST(SUM(file_size_in_bytes) AS BIGINT) AS total_bytes
FROM nessie.lab.small_files.files
WHERE content = 0;

-- Optional: list tiniest files
SELECT file_path, file_size_in_bytes
FROM nessie.lab.small_files.files
WHERE content = 0
ORDER BY file_size_in_bytes ASC
LIMIT 10;

-- Optional: snapshot count
SELECT COUNT(*) AS snapshot_count
FROM nessie.lab.small_files.snapshots;
```
**Result**: Many small .parquet files confirmed.

---

## 8) Iceberg Maintenance (Spark) — Full Runbook
### 8.1 Pre-flight: inspect current state
- Count data files:
```sql
SELECT COUNT(*) AS data_files
FROM nessie.lab.small_files.files
WHERE content = 0;
```
- Average file size (bytes):
```sql
SELECT AVG(file_size_in_bytes) AS avg_bytes
FROM nessie.lab.small_files.files
WHERE content = 0;
```
- Snapshot history:
```sql
SELECT snapshot_id, committed_at, operation
FROM nessie.lab.small_files.snapshots
ORDER BY committed_at DESC;
```

---

### 8.2 Enable deletes/GC (for snapshot expiry/orphan removal)
```sql
ALTER TABLE nessie.lab.small_files
SET TBLPROPERTIES (
  'gc.enabled'='true'
);
```

---

### 8.3 Data compaction — rewrite_data_files (Bin Pack)
- Merge many tiny Parquet files into fewer, larger files.
```sql
CALL nessie.system.rewrite_data_files(
  table => 'lab.small_files',
  options => map(
    'target-file-size-bytes','134217728',
    'min-input-files','1',
    'max-concurrent-file-group-rewrites','4',
    'partial-progress.enabled','true'
  )
);
```
---

### 8.4 Manifest compaction — rewrite_manifests
- Reduce many tiny manifest files into fewer/larger manifests.
```sql
CALL nessie.system.rewrite_manifests(
  table => 'lab.small_files'
);
```

---

### 8.5 Expire old snapshots — ```expire_snapshots```
- Drop old snapshots and unreferenced manifests.
**Keep last 5 snapshots**:
```sql
CALL nessie.system.expire_snapshots(
  table => 'lab.small_files',
  retain_last => 5
);
```
***Or by UTC timestamp***:
```sql
CALL nessie.system.expire_snapshots(
  table => 'lab.small_files',
  older_than => TIMESTAMP '2025-08-30 00:00:00'
);
```

---

### 8.6 Remove orphan files — remove_orphan_files
- Delete unreferenced files left behind by failed/aborted writes or snapshot expiry.
- Spark requires older_than >= 24h for safety.
```sql
CALL nessie.system.remove_orphan_files(
  table => 'lab.small_files',
  older_than => TIMESTAMP '2025-08-30 00:00:00'
);
```

---

### 8.7 Table diagnostics & validation
- File counts by content type:
```sql
SELECT content, COUNT(*) AS files
FROM nessie.lab.small_files.files
GROUP BY content;
```
- Total table size:
```sql
SELECT SUM(file_size_in_bytes) AS total_bytes
FROM nessie.lab.small_files.files
WHERE content = 0;
```
- Properties:
```sql
DESCRIBE TABLE EXTENDED nessie.lab.small_files;
```

---

### 8.8 Rollback / Time travel
- Find previous snapshot_id:
```sql
SELECT snapshot_id, committed_at, operation
FROM nessie.lab.small_files.snapshots
ORDER BY committed_at DESC;
```
- Rollback to snapshot:
```sql
CALL nessie.system.rollback_to_snapshot(
  table => 'lab.small_files',
  snapshot_id => <SNAPSHOT_ID>
);
```

---

### 8.9 Recommended properties (set once)
Keep compaction sensible by default:
```sql
ALTER TABLE nessie.lab.small_files SET TBLPROPERTIES (
  'write.target-file-size-bytes'='134217728',
  'write.distribution-mode'='hash',
  'gc.enabled'='true'
);
```

---

### 8.10 Post-maintenance sanity
- Verify fewer/larger data files and leaner metadata:
```sql
SELECT 
  SUM(CASE WHEN content=0 THEN 1 ELSE 0 END) AS data_files,
  SUM(CASE WHEN content=1 THEN 1 ELSE 0 END) AS pos_delete_files,
  SUM(CASE WHEN content=2 THEN 1 ELSE 0 END) AS eq_delete_files
FROM nessie.lab.small_files.files;
```