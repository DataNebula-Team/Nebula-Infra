from airflow.sdk import dag, task
from datetime import datetime
import socket
import logging

@dag(
    dag_id="test_celery_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["e2e", "celery"]
)
def test_celery_pipeline():

    @task
    def validate_worker():

        hostname = socket.gethostname()

        logging.info("=" * 50)
        logging.info("TASK EJECUTADA EN WORKER")
        logging.info(f"HOSTNAME: {hostname}")
        logging.info("=" * 50)

        return hostname

    validate_worker()

dag_instance = test_celery_pipeline()