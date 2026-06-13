from airflow.sdk import dag, task
from datetime import datetime
import subprocess
import logging

SPARK_MASTER = "spark://11.0.2.183:7077"

@dag(
    dag_id="test_spark_submit",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["e2e", "spark"]
)
def test_spark_submit():

    @task
    def submit_spark_job():

        cmd = [
            "spark-submit",
            "--master",
            SPARK_MASTER,
            "/opt/airflow/dags/test_spark.py"
        ]

        logging.info(f"Ejecutando: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        logging.info(result.stdout)

        return "SPARK_OK"

    submit_spark_job()

dag_instance = test_spark_submit()