# Local Data Lakehouse (Phase 1)

This project sets up a **Local Data Lakehouse** environment using:
- **Apache Iceberg** as the table format
- **MinIO** as the object storage
- **Trino** as the query engine

---

## Dataset
For Phase 1, the dataset used is **NYC Taxi Trip Records – Yellow Taxi**  
**Period:** January 2024 – June 2025  
**Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/assets/tlc/pages/about/tlc_trip_record_data.html)

---

## 1. Prepare the Dataset
After cloning this repository, run the following commands to download the dataset files into the `datasets/nyc_tlc` folder:

```bash
chmod +x scripts/download_tlc_yellow.sh
scripts/download_tlc_yellow.sh
```

**Notes:**
- The script will download large `.parquet` files — ensure you have sufficient disk space.
- Dataset files are **not** stored in the Git repository.

---

## 2. Run the Local Data Lakehouse
Start all services using Docker Compose:

```bash
docker compose up -d
```

---

## 3. Access MinIO and Upload Files
- **URL:** [http://localhost:9001](http://localhost:9001)  
- **Access Key:** `test`  
- **Secret Key:** `test12334567`  

After logging in, upload dataset files into the bucket named **`lakehouse-data`**.

### Folder Structure Pattern
Inside the `lakehouse-data` bucket, dataset files should be organized using the following path pattern:

```
raw/nyc_tlc/<car_type>/year=<YYYY>/month=<MM>/
```

Where:
- `<car_type>` = `yellow`, `green`, `for_hire_vehicle`, `high_volume`
- `<YYYY>` = Year of the trip data (e.g., `2024`, `2025`)
- `<MM>` = Month of the trip data, zero-padded (e.g., `01`, `02`, ..., `12`)

**Example:**
```
raw/nyc_tlc/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet
raw/nyc_tlc/yellow/year=2024/month=02/yellow_tripdata_2024-02.parquet
raw/nyc_tlc/green/year=2025/month=03/green_tripdata_2025-03.parquet
```

**Why use this pattern?**
- **Scalable:** Easily add new vehicle types without changing the existing structure.
- **Efficient Queries:** Table engines like Iceberg & Trino can use `car_type`, `year`, and `month` as partitions for faster queries.
- **Organized Storage:** Keeps datasets well-structured for ingestion and maintenance.

---

## 4. Access Trino
- **URL:** [http://localhost:8080](http://localhost:8080)  
You can query data using the Trino CLI or connect through BI tools such as Apache Superset.

---

## License
This project is for educational purposes only.
