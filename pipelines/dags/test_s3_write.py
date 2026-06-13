from pyspark.sql import SparkSession
from datetime import datetime

spark = (
    SparkSession.builder
    .appName("s3_write_test")
    .getOrCreate()
)

data = [
    (
        datetime.now().isoformat(),
        "SUCCESS"
    )
]

df = spark.createDataFrame(
    data,
    ["execution_time", "status"]
)

df.write.mode("overwrite").parquet(
    "s3a://nebula2-airflow-logs/e2e_test/"
)

spark.stop()