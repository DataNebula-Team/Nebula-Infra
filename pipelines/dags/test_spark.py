from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("spark_cluster_test")
    .getOrCreate()
)

data = [
    (1, "Airflow"),
    (2, "RabbitMQ"),
    (3, "Spark")
]

df = spark.createDataFrame(
    data,
    ["id", "component"]
)

df.show()

print(f"Rows: {df.count()}")

spark.stop()