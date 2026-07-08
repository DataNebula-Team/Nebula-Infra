"""
DAG de prueba — dag_test_infraestructura

Verifica que los 3 componentes críticos del stack funcionan antes de
ejecutar el pipeline real.

Flujo:
    verificar_celery → verificar_s3 → verificar_spark_api

Pruebas:
    1. verificar_celery    — confirma que el Worker recibe y ejecuta tasks
    2. verificar_s3        — confirma acceso de lectura/escritura a S3
    3. verificar_spark_api — confirma conectividad HTTP al clúster Spark

Si los 3 pasan, el pipeline B1→B4 puede ejecutarse.
"""

from __future__ import annotations

import logging
import os
import socket
from datetime import datetime

import requests

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

SPARK_REST_URL = os.getenv("SPARK_MASTER_URL", "http://11.0.2.183:6066")
AWS_CONN_ID    = "my_s3_conn"
BUCKET         = os.getenv("S3_BUCKET", "data-nebula-clinical-lake")


@dag(
    dag_id="dag_test_infraestructura",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test", "infraestructura", "e2e"],
    default_args={"retries": 0},
)
def dag_test_infraestructura():

    @task
    def verificar_celery() -> dict:
        """
        Prueba 1 — Celery Worker.
        Confirma que un Worker recibe la task y puede ejecutar código Python.
        """
        hostname = socket.gethostname()
        logger.info("=" * 50)
        logger.info("✅ PRUEBA 1: Celery Worker")
        logger.info("Worker hostname: %s", hostname)
        logger.info("=" * 50)
        return {"worker": hostname, "status": "ok"}

    @task
    def verificar_s3(celery_result: dict) -> dict:
        """
        Prueba 2 — Conexión S3.
        Confirma que el Worker puede listar objetos en el bucket
        usando la conexión my_s3_conn configurada en Airflow.
        """
        logger.info("=" * 50)
        logger.info("✅ PRUEBA 2: Conexión S3")
        logger.info("Worker: %s", celery_result["worker"])

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        # Listar prefijo sources/ para confirmar acceso
        keys = s3_hook.list_keys(
            bucket_name=BUCKET,
            prefix="sources/",
            max_items=5,
        ) or []

        if not keys:
            raise ValueError(
                f"No se encontraron objetos en s3://{BUCKET}/sources/. "
                "Verificar credenciales en conexión my_s3_conn."
            )

        logger.info("Bucket: %s", BUCKET)
        logger.info("Objetos encontrados en sources/: %d", len(keys))
        for k in keys[:3]:
            logger.info("  - %s", k)
        logger.info("=" * 50)

        return {"s3_bucket": BUCKET, "objetos": len(keys), "status": "ok"}

    @task
    def verificar_spark_api(s3_result: dict) -> dict:
        """
        Prueba 3 — REST API del clúster Spark.
        Confirma que el Worker puede hacer HTTP al puerto 6066 de SRV-CLUSTER-SPARK.
        Una respuesta HTTP (cualquier código) confirma conectividad.
        """
        logger.info("=" * 50)
        logger.info("✅ PRUEBA 3: Spark REST API")
        logger.info("S3 verificado: %s objetos", s3_result["objetos"])

        url = f"{SPARK_REST_URL}/v1/submissions/status/test"

        try:
            resp = requests.get(url, timeout=10)
            logger.info("URL: %s", url)
            logger.info("HTTP Status: %d", resp.status_code)
            logger.info("Respuesta: %s", resp.text[:200])
            logger.info("=" * 50)

            # Cualquier respuesta HTTP confirma conectividad
            # 404 = API activa pero no hay job con ese ID (esperado)
            return {
                "spark_url":   SPARK_REST_URL,
                "http_status": resp.status_code,
                "status":      "ok",
            }

        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                f"No se puede conectar al clúster Spark en {SPARK_REST_URL}. "
                "Verificar que SRV-CLUSTER-SPARK esté encendido y el puerto 6066 abierto."
            )
        except requests.exceptions.Timeout:
            raise RuntimeError(
                f"Timeout conectando a {SPARK_REST_URL}. "
                "Verificar Security Groups y VPC Peering."
            )

    # ── Flujo ──────────────────────────────────────────────────────────────
    celery_result = verificar_celery()
    s3_result     = verificar_s3(celery_result)
    verificar_spark_api(s3_result)


dag_test_infraestructura()
