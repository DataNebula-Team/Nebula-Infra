"""
DAG B3 — dag_dbt_staging

Primera etapa de transformación con Spark. Los 19 archivos de raw/batch/
se convierten en 19 tablas staging limpias y normalizadas (mapeo 1:1).
Se normalizan llaves, se estandarizan códigos clínicos usando los diccionarios
de referencia de PostgreSQL 5433, y se escribe en S3 staging/batch/.

El Celery Worker lanza el job via REST API de Spark (puerto 6066) hacia
SRV-CLUSTER-SPARK (11.0.2.183). El clúster ejecuta b3_staging.py que
vive en /opt/spark-jobs/ (montado desde ~/spark-cluster/jobs/).

Flujo de tasks:
    verificar_cluster → spark_submit_staging → verificar_staging

Conexiones requeridas:
    - AWS:      my_s3_conn
    - Postgres: postgres_nebula (para leer config)

Variables de Airflow requeridas (o .env en Workers):
    - ANALYTICS_PG_HOST   : IP de SRV-ANALYTICS (10.0.2.10)
    - ANALYTICS_PG_USER   : usuario PostgreSQL 5433
    - ANALYTICS_PG_PASS   : password PostgreSQL 5433
    - S3_BUCKET           : data-nebula-clinical-lake
    - SPARK_MASTER_URL    : http://11.0.2.183:6066

Bucket: data-nebula-clinical-lake
Lee:    S3 raw/batch/
Escribe: S3 staging/batch/
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime

import requests

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

SPARK_REST_URL    = os.getenv("SPARK_MASTER_URL", "http://11.0.2.183:6066")
AWS_CONN_ID       = "my_s3_conn"
BUCKET            = os.getenv("S3_BUCKET", "data-nebula-clinical-lake")
ANALYTICS_PG_HOST = os.getenv("ANALYTICS_PG_HOST", "10.0.2.10")
ANALYTICS_PG_USER = os.getenv("ANALYTICS_PG_USER", "analytics")
ANALYTICS_PG_PASS = os.getenv("ANALYTICS_PG_PASS", "")

# Script que corre en el clúster (montado como volumen)
SPARK_JOB_PATH = "/opt/spark-jobs/b3_staging.py"

# Polling: cuánto esperar y cada cuánto revisar estado del job
JOB_TIMEOUT_SECONDS  = 7200   # 2 horas máximo
JOB_POLL_INTERVAL    = 30     # revisar cada 30 segundos


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def submit_spark_job(date_str: str) -> str:
    """
    Envía el job b3_staging.py al clúster via REST API (puerto 6066).
    Retorna el submission_id para hacer polling de estado.
    """
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
            "--date",    date_str,
            "--pg_host", ANALYTICS_PG_HOST,
            "--pg_user", ANALYTICS_PG_USER,
            "--pg_pass", ANALYTICS_PG_PASS,
            "--bucket",  BUCKET,
        ],
        "sparkProperties": {
            "spark.master":                "spark://11.0.2.183:7077",
            "spark.driver.extraJavaOptions": "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",
            "spark.executor.extraJavaOptions": "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",
            "spark.app.name":             f"B3_Staging_{date_str}",
            "spark.executor.memory":      "2g",
            "spark.executor.cores":       "1",
            "spark.executor.instances":   "1",
            "spark.driver.memory":        "1536m",
            "spark.sql.extensions":       "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
    }

    url = f"{SPARK_REST_URL}/v1/submissions/create"
    logger.info("Enviando job B3 a Spark REST API: %s", url)

    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()

    result = resp.json()
    submission_id = result.get("submissionId")

    if not submission_id:
        raise RuntimeError(f"Spark REST API no retornó submissionId: {result}")

    logger.info("Job B3 enviado. submission_id: %s", submission_id)
    return submission_id


def poll_job_status(submission_id: str) -> str:
    """
    Hace polling del estado del job hasta que termine o se agote el timeout.
    Retorna el estado final: FINISHED, FAILED, KILLED.
    """
    url = f"{SPARK_REST_URL}/v1/submissions/status/{submission_id}"
    elapsed = 0

    while elapsed < JOB_TIMEOUT_SECONDS:
        time.sleep(JOB_POLL_INTERVAL)
        elapsed += JOB_POLL_INTERVAL

        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        status_data = resp.json()

        run_state = status_data.get("driverState", "UNKNOWN")
        logger.info(
            "[B3] Estado del job: %s (elapsed: %ds)", run_state, elapsed
        )

        if run_state == "FINISHED":
            logger.info("✓ Job B3 completado exitosamente")
            return "FINISHED"

        if run_state in ("FAILED", "KILLED", "ERROR"):
            msg = status_data.get("message", "Sin detalle")
            raise RuntimeError(
                f"Job B3 terminó con estado {run_state}: {msg}"
            )

    raise TimeoutError(
        f"Job B3 no completó en {JOB_TIMEOUT_SECONDS}s. "
        f"submission_id: {submission_id}"
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag_dbt_staging",
    schedule=None,          # Triggereado por dag_eda_post_ingest via Quality Gate
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["B3", "staging", "spark", "delta", "lab"],
    default_args={"retries": 1},
    doc_md=__doc__,
)
def dag_dbt_staging():

    @task
    def verificar_cluster() -> bool:
        """
        Verifica que el clúster Spark esté disponible antes de enviar el job.
        Consulta el endpoint de estado de la REST API.
        """
        url = f"{SPARK_REST_URL}/v1/submissions/status/test"
        try:
            # Un 404 o 400 significa que la API está activa pero no hay job con ese ID
            # Un ConnectionError significa que el clúster no está disponible
            resp = requests.get(url, timeout=10)
            logger.info("Clúster Spark disponible. HTTP %d", resp.status_code)
            return True
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                f"No se puede conectar al clúster Spark en {SPARK_REST_URL}. "
                "Verificar que SRV-CLUSTER-SPARK esté encendido y el puerto 6066 abierto."
            )

    @task
    def spark_submit_staging(cluster_ok: bool, **context) -> str:
        """
        Envía b3_staging.py al clúster Spark via REST API y espera resultado.
        El Worker no procesa datos — solo lanza el job y espera.
        """
        date_str = context["ds"]
        logger.info("Lanzando B3 Staging para fecha: %s", date_str)

        # Enviar job
        submission_id = submit_spark_job(date_str)

        # Esperar resultado
        estado_final = poll_job_status(submission_id)

        logger.info(
            "B3 Staging completado. submission_id=%s estado=%s",
            submission_id, estado_final
        )
        return submission_id

    @task
    def verificar_staging(submission_id: str, **context) -> dict:
        """
        Verifica que los archivos staging fueron escritos en S3.
        Comprueba que existan particiones para la fecha de ejecución.
        """
        date_str = context["ds"]
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)

        prefix = f"staging/batch/"
        keys   = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []

        # Buscar archivos con la fecha correcta
        archivos_fecha = [k for k in keys if f"date={date_str}" in k]

        if not archivos_fecha:
            raise ValueError(
                f"No se encontraron archivos en staging/batch/ para date={date_str}. "
                f"El job puede haber fallado silenciosamente. submission_id={submission_id}"
            )

        # Contar tablas distintas escritas
        tablas = set()
        for k in archivos_fecha:
            if "table_name=" in k:
                tabla = k.split("table_name=")[1].split("/")[0]
                tablas.add(tabla)

        logger.info(
            "✓ Staging verificado: %d archivos, %d tablas distintas para date=%s",
            len(archivos_fecha), len(tablas), date_str
        )
        logger.info("Tablas encontradas: %s", sorted(tablas))

        if len(tablas) < 15:
            logger.warning(
                "Se esperaban 19 tablas pero solo se encontraron %d. "
                "Revisar logs del clúster.", len(tablas)
            )

        return {
            "date":           date_str,
            "archivos":       len(archivos_fecha),
            "tablas":         len(tablas),
            "submission_id":  submission_id,
        }

    # ── Flujo ──────────────────────────────────────────────────────────────
    cluster_ok    = verificar_cluster()
    submission_id = spark_submit_staging(cluster_ok)
    verificar_staging(submission_id)


dag_dbt_staging()
