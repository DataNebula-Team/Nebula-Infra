from airflow.sdk import dag, task
from datetime import datetime
import subprocess
import logging

SPARK_MASTER = "spark://11.0.2.183:7077"

@dag(
    dag_id="test_spark_to_s3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["e2e", "s3"]
)
def test_spark_to_s3():

    @task
    def submit_job():

        cmd = [
            "spark-submit",
            "--master",
            SPARK_MASTER,
            "/opt/airflow/dags/test_s3_write.py"
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        logging.info(result.stdout)

        return "S3_OK"

    submit_job()

dag_instance = test_spark_to_s3()