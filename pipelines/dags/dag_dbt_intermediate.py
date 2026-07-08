"""
DAG B4 — dag_dbt_intermediate

Construye el modelo clínico unificado int_modelo_clinico mediante LEFT JOINs
sobre subject_id y hadm_id. Aquí ocurre la convergencia batch + streaming.

Lee de staging/batch/ (obligatorio) y de staging/streaming/ (opcional).
Si staging/streaming/ no existe (S1 no ha corrido), B4 continúa solo con batch.
Escribe int_modelo_clinico en S3 intermediate/batch/ con Delta Lake.

El Celery Worker lanza b4_intermediate.py via REST API de Spark (puerto 6066)
hacia SRV-CLUSTER-SPARK (11.0.2.183).

Flujo de tasks:
    verificar_staging_disponible → spark_submit_intermediate → verificar_intermediate

Conexiones requeridas:
    - AWS: my_s3_conn

Variables de entorno:
    - S3_BUCKET         : data-nebula-clinical-lake
    - SPARK_MASTER_URL  : http://11.0.2.183:6066

Bucket: data-nebula-clinical-lake
Lee:    S3 staging/batch/ + S3 staging/streaming/ (opcional)
Escribe: S3 intermediate/batch/
"""

from __future__ import annotations

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

SPARK_REST_URL = os.getenv("SPARK_MASTER_URL", "http://11.0.2.183:6066")
AWS_CONN_ID    = "my_s3_conn"
BUCKET         = os.getenv("S3_BUCKET", "data-nebula-clinical-lake")

SPARK_JOB_PATH      = "/opt/spark-jobs/b4_intermediate.py"
JOB_TIMEOUT_SECONDS = 1800
JOB_POLL_INTERVAL   = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def submit_spark_job(date_str: str) -> str:
    """
    Envía b4_intermediate.py al clúster via REST API.
    Retorna submission_id para polling.
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
            "--date",   date_str,
            "--bucket", BUCKET,
        ],
        "sparkProperties": {
            "spark.master":               "spark://11.0.2.183:7077",
            "spark.driver.extraJavaOptions": "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",
            "spark.executor.extraJavaOptions": "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED",
            "spark.app.name":            f"B4_Intermediate_{date_str}",
            "spark.executor.memory":     "2g",
            "spark.executor.cores":      "1",
            "spark.executor.instances":  "2",
            "spark.driver.memory":       "1536m",
            "spark.sql.extensions":      "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
    }

    url = f"{SPARK_REST_URL}/v1/submissions/create"
    logger.info("Enviando job B4 a Spark REST API: %s", url)

    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()

    result = resp.json()
    submission_id = result.get("submissionId")

    if not submission_id:
        raise RuntimeError(f"Spark REST API no retornó submissionId: {result}")

    logger.info("Job B4 enviado. submission_id: %s", submission_id)
    return submission_id


def poll_job_status(submission_id: str) -> str:
    """Polling de estado hasta FINISHED o timeout."""
    url     = f"{SPARK_REST_URL}/v1/submissions/status/{submission_id}"
    elapsed = 0

    while elapsed < JOB_TIMEOUT_SECONDS:
        time.sleep(JOB_POLL_INTERVAL)
        elapsed += JOB_POLL_INTERVAL

        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        status_data = resp.json()

        run_state = status_data.get("driverState", "UNKNOWN")
        logger.info("[B4] Estado: %s (elapsed: %ds)", run_state, elapsed)

        if run_state == "FINISHED":
            logger.info("✓ Job B4 completado exitosamente")
            return "FINISHED"

        if run_state in ("FAILED", "KILLED", "ERROR"):
            msg = status_data.get("message", "Sin detalle")
            raise RuntimeError(
                f"Job B4 terminó con estado {run_state}: {msg}"
            )

    raise TimeoutError(
        f"Job B4 no completó en {JOB_TIMEOUT_SECONDS}s. "
        f"submission_id: {submission_id}"
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag_dbt_intermediate",
    schedule=None,          # Triggereado por dag_dbt_staging
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["B4", "intermediate", "spark", "delta", "convergencia", "lab"],
    default_args={"retries": 1},
    doc_md=__doc__,
)
def dag_dbt_intermediate():

    @task
    def verificar_staging_disponible(**context) -> dict:
        """
        Verifica que staging/batch/ tiene datos para la fecha actual.
        También detecta si staging/streaming/ está disponible (S1 ya corrió).
        B4 puede continuar sin streaming — es comportamiento esperado en el lab.
        """
        date_str = context["ds"]
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)

        # Verificar staging/batch/
        batch_keys = s3_hook.list_keys(
            bucket_name=BUCKET,
            prefix=f"staging/batch/"
        ) or []
        batch_fecha = [k for k in batch_keys if f"date={date_str}" in k]

        if not batch_fecha:
            raise ValueError(
                f"No hay datos en staging/batch/ para date={date_str}. "
                "B3 debe completarse antes de ejecutar B4."
            )

        # Verificar staging/streaming/ (opcional)
        streaming_keys = s3_hook.list_keys(
            bucket_name=BUCKET,
            prefix=f"staging/streaming/"
        ) or []
        streaming_fecha = [k for k in streaming_keys if f"date={date_str}" in k]
        streaming_disponible = len(streaming_fecha) > 0

        if streaming_disponible:
            logger.info(
                "✓ staging/streaming/ disponible: %d archivos. "
                "B4 integrará batch + streaming (convergencia completa).",
                len(streaming_fecha)
            )
        else:
            logger.info(
                "staging/streaming/ no disponible para date=%s. "
                "B4 procesará solo batch. "
                "Para activar convergencia: ejecutar S1 antes de B4.",
                date_str
            )

        logger.info(
            "✓ staging/batch/ verificado: %d archivos para date=%s",
            len(batch_fecha), date_str
        )

        return {
            "date":                  date_str,
            "batch_archivos":        len(batch_fecha),
            "streaming_disponible":  streaming_disponible,
            "streaming_archivos":    len(streaming_fecha),
        }

    @task
    def spark_submit_intermediate(staging_info: dict, **context) -> str:
        """
        Envía b4_intermediate.py al clúster Spark via REST API y espera resultado.
        El script maneja internamente si hay o no datos de streaming.
        """
        date_str = context["ds"]
        logger.info(
            "Lanzando B4 Intermediate para fecha: %s | "
            "batch=%d archivos | streaming=%s",
            date_str,
            staging_info["batch_archivos"],
            "disponible" if staging_info["streaming_disponible"] else "no disponible",
        )

        submission_id = submit_spark_job(date_str)
        estado_final  = poll_job_status(submission_id)

        logger.info(
            "B4 Intermediate completado. submission_id=%s estado=%s",
            submission_id, estado_final
        )
        return submission_id

    @task
    def verificar_intermediate(submission_id: str, **context) -> dict:
        """
        Verifica que int_modelo_clinico fue escrita en S3 intermediate/batch/.
        """
        date_str = context["ds"]
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)

        prefix = "intermediate/batch/"
        keys   = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []

        archivos_fecha = [k for k in keys if f"date={date_str}" in k]

        if not archivos_fecha:
            raise ValueError(
                f"No se encontraron archivos en intermediate/batch/ para date={date_str}. "
                f"Revisar logs del clúster. submission_id={submission_id}"
            )

        # Contar registros por sede
        sedes = set()
        for k in archivos_fecha:
            if "sede=" in k:
                sede = k.split("sede=")[1].split("/")[0]
                sedes.add(sede)

        logger.info(
            "✓ int_modelo_clinico verificada: %d archivos, sedes=%s para date=%s",
            len(archivos_fecha), sorted(sedes), date_str
        )

        if len(sedes) < 3:
            logger.warning(
                "Se esperaban 3 sedes pero solo se encontraron %d: %s",
                len(sedes), sedes
            )

        return {
            "date":          date_str,
            "archivos":      len(archivos_fecha),
            "sedes":         len(sedes),
            "submission_id": submission_id,
        }

    # ── Flujo ──────────────────────────────────────────────────────────────
    staging_info  = verificar_staging_disponible()
    submission_id = spark_submit_intermediate(staging_info)
    verificar_intermediate(submission_id)


dag_dbt_intermediate()
