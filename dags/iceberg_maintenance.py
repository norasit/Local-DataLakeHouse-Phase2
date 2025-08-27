from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG("iceberg_compact_daily", start_date=days_ago(1), schedule="@daily", catchup=False) as dag:
    compact = SparkSubmitOperator(
        task_id="rewrite_data_files",
        application="/opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.6.jar",  # แทนที่ด้วยสคริปต์จริงของคุณ
        conn_id=None,
        jars="/opt/bitnami/spark/jars/iceberg-spark-runtime.jar",
        java_class=None,
        application_args=[],
        name="iceberg-maintenance",
        conf={
            "spark.sql.catalog.lh": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.lh.type": "hive",
            "spark.sql.catalog.lh.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.lh.warehouse": "s3a://lakehouse-data/warehouse/",
        },
        packages=None,
        total_executor_cores=1,
        executor_memory="1g",
        driver_memory="1g",
        master="spark://spark:7077",
    )
