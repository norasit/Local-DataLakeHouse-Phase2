# Local Data Lakehouse (Phase 2)

## Overview
Phase 2 is a **simplified version** of Phase 1:
- Removed **Hive Metastore** → use **Nessie** as the Iceberg catalog
- Removed **Airflow** → focus on Spark + Trino usage manually
- Removed the **NYC Taxi dataset** → no dataset download/upload steps
- Kept only the **core components** needed to learn Iceberg

### Components
- **MinIO** (S3-compatible object storage)  
- **Project Nessie** (Iceberg catalog & metadata store)  
- **Apache Trino** (SQL query engine)  
- **Apache Spark** (data processing + Iceberg maintenance)  

The goal of Phase 2 is to provide an **introductory environment** for practicing Iceberg basics,  
before moving on to Phase 3 (ETL, orchestration, and production-like workflows).

---

## Prerequisites
- Docker & Docker Compose  
- Bash / curl  

---

## Setup
1. Download the required JARs (for Spark + Iceberg + S3A):
    ```bash
    chmod +x scripts/get_hms_s3a_jars.sh
    scripts/get_hms_s3a_jars.sh

    chmod +x scripts/get_iceberg_spark_jar.sh
    scripts/get_iceberg_spark_jar.sh
    ```

JARs will be stored under ```jars/``` and automatically mounted into the Spark container.

---

2. Start the stack:
    ```bash
    docker compose up -d
    ```

Services started:
- MinIO → http://localhost:9001 (user/pass: test / test12334567)
- Nessie → http://localhost:19120 (UI + API)
- Trino → http://localhost:8080
- Spark (master + worker)

---

## Usage
- Open Trino Web UI → query catalogs (Nessie + Iceberg)
- Start a Spark SQL shell:
    ```bash
    docker exec -it local-datalakehouse-phase2-spark-1 spark-sql
    ```

For a **step-by-step Spark + Iceberg guide** (maintenance, compaction, snapshot management, orphan cleanup),
see **SPARK_ICEBERG_GUIDE.md**.

---

## Notes
- Phase 2 does **not include a sample dataset** → users can create tables and insert data manually in Spark SQL.
- Data is persisted in:
  - minio-data/ (object storage)
  - nessie-data/ (Iceberg catalog metadata)

## License
Educational purpose only.