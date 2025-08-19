# run บน host
import pyarrow.parquet as pq
pf = pq.ParquetFile('datasets/nyc_tlc/yellow/2024/yellow_tripdata_2024-01.parquet')
print(pf.schema)   # ดูชื่อคอลัมน์/ชนิดข้อมูลจริงจากไฟล์
