"""
DAG B5 - dag_dbt_marts

Lee int_modelo_clinico desde intermediate/batch/ y tablas adicionales desde
staging/batch/, calcula los 4 marts con sus KPIs y los escribe en marts/batch/
con Delta Lake via job Spark en SRV-CLUSTER-SPARK.

Flujo:
    verificar_intermediate -> spark_submit_marts -> verificar_marts

Conexiones requeridas:
    - AWS: my_s3_conn (tipo aws)

Variables de entorno requeridas:
    - SPARK_MASTER_URL
    - S3_BUCKET

Bucket: data-nebula-clinical-lake
Lee:    intermediate/batch/ + staging/batch/
Escribe: marts/batch/mart=clinico|farmacia|financiero|operacional/
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

import requests

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

AWS_CONN_ID      = "my_s3_conn"
BUCKET           = os.getenv("S3_BUCKET", "data-nebula-clinical-lake")
SPARK_REST_URL   = os.getenv("SPARK_MASTER_URL", "http://11.0.2.183:6066")
SPARK_JOB_PATH   = "/opt/spark-jobs/b5_marts.py"
JOB_TIMEOUT_SECONDS = 7200  # 2 horas máximo


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def poll_job_status(submission_id: str) -> str:
    url = f"{SPARK_REST_URL}/v1/submissions/status/{submission_id}"
    elapsed = 0
    interval = 30

    while elapsed < JOB_TIMEOUT_SECONDS:
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            state = data.get("driverState", "UNKNOWN")
            logger.info("[B5] Estado del job: %s (elapsed: %ds)", state, elapsed)

            if state == "FINISHED":
                return state
            elif state in ("FAILED", "KILLED", "ERROR"):
                detail = data.get("message", "Sin detalle")
                raise RuntimeError(f"Job B5 terminó con estado {state}: {detail}")

        except RuntimeError:
            raise
        except Exception as e:
            logger.warning("[B5] Error consultando estado: %s", e)

        import time
        time.sleep(interval)
        elapsed += interval

    raise TimeoutError(
        f"Job B5 no completó en {JOB_TIMEOUT_SECONDS}s. "
        f"submission_id: {submission_id}"
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag_dbt_marts",
    schedule="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["B5", "marts", "kpis", "lab"],
    default_args={"retries": 1},
)
def dag_dbt_marts():

    @task
    def verificar_intermediate(**context) -> dict:
        """Verifica que intermediate/batch/ tiene datos para la fecha del run."""
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)
        date_str = context["ds"]

        prefix = f"intermediate/batch/"
        keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []
        parquet_keys = [k for k in keys if k.endswith(".parquet")]

        if not parquet_keys:
            raise FileNotFoundError(
                f"No hay datos en intermediate/batch/ para procesar. "
                f"¿Se ejecutó B4 (dag_dbt_intermediate) primero?"
            )

        logger.info(
            "✓ intermediate/batch/ disponible: %d archivos parquet",
            len(parquet_keys)
        )
        return {
            "parquet_count": len(parquet_keys),
            "date": date_str,
        }

    @task
    def spark_submit_marts(verificacion: dict, **context) -> str:
        """Envía el job B5 al clúster Spark via REST API y espera que termine."""
        date_str = context["ds"]

        logger.info("Lanzando B5 Marts para fecha: %s", date_str)
        logger.info("Enviando job B5 a Spark REST API: %s", f"{SPARK_REST_URL}/v1/submissions/create")

        payload = {
            "action": "CreateSubmissionRequest",
            "appResource": f"file://{SPARK_JOB_PATH}",
            "clientSparkVersion": "3.5.3",
            "mainClass": "org.apache.spark.deploy.SparkSubmit",
            "environmentVariables": {
                "SPARK_ENV_LOADED": "1",
            },
            "appArgs": [
                f"file://{SPARK_JOB_PATH}",
                "--date",   date_str,
                "--bucket", BUCKET,
            ],
            "sparkProperties": {
                "spark.master":                   "spark://11.0.2.183:7077",
                "spark.driver.extraJavaOptions":  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",
                "spark.executor.extraJavaOptions":"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",
                "spark.app.name":                 f"B5_Marts_{date_str}",
                "spark.executor.memory":          "2g",
                "spark.executor.cores":           "1",
                "spark.executor.instances":       "1",
                "spark.driver.memory":            "1536m",
                "spark.sql.extensions":           "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.eventLog.enabled":         "false",
            },
        }

        url = f"{SPARK_REST_URL}/v1/submissions/create"
        try:
            resp = requests.post(url, json=payload, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            raise RuntimeError(
                f"No se puede conectar al clúster Spark en {SPARK_REST_URL}. "
                f"Verificar que SRV-CLUSTER-SPARK esté encendido y el puerto 6066 abierto. "
                f"Error: {e}"
            )

        result = resp.json()
        submission_id = result.get("submissionId")

        if not submission_id:
            raise RuntimeError(f"Spark REST API no retornó submissionId: {result}")

        logger.info("Job B5 enviado. submission_id: %s", submission_id)

        final_state = poll_job_status(submission_id)
        logger.info("Job B5 completado con estado: %s", final_state)

        return submission_id

    @task
    def verificar_marts(**context) -> dict:
        """Verifica que los 4 marts fueron escritos en S3 marts/batch/."""
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)
        date_str = context["ds"]

        marts_esperados = ["clinico", "farmacia", "financiero", "operacional"]
        marts_encontrados = []
        marts_faltantes   = []

        for mart in marts_esperados:
            prefix = f"marts/batch/mart={mart}/"
            keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []
            parquet_keys = [k for k in keys if k.endswith(".parquet")]
            if parquet_keys:
                marts_encontrados.append(mart)
                logger.info("✓ mart_%s: %d archivos", mart, len(parquet_keys))
            else:
                marts_faltantes.append(mart)
                logger.warning("✗ mart_%s: sin archivos", mart)

        if marts_faltantes:
            logger.warning("Marts no escritos: %s", marts_faltantes)
        else:
            logger.info("✓ Los 4 marts escritos correctamente en marts/batch/")

        return {
            "marts_encontrados": marts_encontrados,
            "marts_faltantes":   marts_faltantes,
            "date": date_str,
        }

    # ---- Flujo del DAG ----
    verificacion  = verificar_intermediate()
    submission_id = spark_submit_marts(verificacion)
    verificar_marts()


dag_dbt_marts()
