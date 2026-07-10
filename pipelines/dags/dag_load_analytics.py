"""
DAG B6 - dag_load_analytics

Último DAG del flujo batch. Lee los 4 marts desde S3 marts/batch/,
los carga en PostgreSQL analítico (SRV-ANALYTICS:5433) para Superset,
notifica a Grafana que el ciclo batch completó y registra el log final.

Flujo:
    leer_marts_desde_s3
        → truncar_tablas_analytics
            → cargar_postgresql_analytics
                → [refrescar_superset, notificar_grafana]
                    → log_ciclo_completado

Conexiones requeridas:
    - AWS:      my_s3_conn      (tipo aws)
    - Postgres: postgres_nebula (SRV-MASTER:5432 — metadata y logs)
    - Postgres: postgres_analytics (SRV-ANALYTICS:5433 — marts analíticos)

Bucket: data-nebula-clinical-lake
Lee:    marts/batch/
Escribe: PostgreSQL 5433 (4 tablas mart_*) + PostgreSQL 5432 (log)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

import pandas as pd
import requests

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

AWS_CONN_ID        = "my_s3_conn"
PG_ANALYTICS_CONN  = "postgres_analytics"   # SRV-ANALYTICS:5433
PG_NEBULA_CONN     = "postgres_nebula"      # SRV-MASTER:5432

BUCKET             = os.getenv("S3_BUCKET", "data-nebula-clinical-lake")
SUPERSET_URL       = os.getenv("SUPERSET_URL", "http://10.0.2.10:8088")
GRAFANA_URL        = os.getenv("GRAFANA_URL", "http://10.0.2.5:3000")

MARTS = ["clinico", "farmacia", "financiero", "operacional"]

# ---------------------------------------------------------------------------
# DDL — Tablas en PostgreSQL 5433
# ---------------------------------------------------------------------------

DDL_MART_CLINICO = """
CREATE TABLE IF NOT EXISTS mart_clinico (
    sede                            VARCHAR(50),
    avg_tiempo_espera_urgencias_min NUMERIC,
    avg_tiempo_hospitalizacion_hrs  NUMERIC,
    total_pacientes                 INTEGER,
    total_admisiones                INTEGER,
    pacientes_reingreso_30d         INTEGER,
    total_pacientes_unicos          INTEGER,
    tasa_reingreso_30d_pct          NUMERIC,
    top_diagnosticos                TEXT,
    date                            DATE,
    loaded_at                       TIMESTAMP DEFAULT NOW()
);
"""

DDL_MART_FARMACIA = """
CREATE TABLE IF NOT EXISTS mart_farmacia (
    sede                            VARCHAR(50),
    total_prescripciones            BIGINT,
    avg_prescripciones_por_paciente NUMERIC,
    medicamentos_stock_critico      INTEGER,
    total_medicamentos              INTEGER,
    avg_stock_actual                NUMERIC,
    top_medicamentos                TEXT,
    date                            DATE,
    loaded_at                       TIMESTAMP DEFAULT NOW()
);
"""

DDL_MART_FINANCIERO = """
CREATE TABLE IF NOT EXISTS mart_financiero (
    sede                            VARCHAR(50),
    ingresos_totales                NUMERIC,
    promedio_facturacion            NUMERIC,
    total_registros                 INTEGER,
    registros_billing_cero          INTEGER,
    costo_operacional_total         NUMERIC,
    avg_costo_operacional           NUMERIC,
    date                            DATE,
    loaded_at                       TIMESTAMP DEFAULT NOW()
);
"""

DDL_MART_OPERACIONAL = """
CREATE TABLE IF NOT EXISTS mart_operacional (
    sede                            VARCHAR(50),
    total_estancias_uci             BIGINT,
    avg_horas_uci                   NUMERIC,
    pacientes_con_uci               INTEGER,
    total_pacientes                 INTEGER,
    pct_ocupacion_uci               NUMERIC,
    distribucion_servicios          TEXT,
    transferencias_por_tipo         TEXT,
    date                            DATE,
    loaded_at                       TIMESTAMP DEFAULT NOW()
);
"""

DDL_PIPELINE_LOG = """
CREATE TABLE IF NOT EXISTS pipeline_batch_log (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(200),
    dag_id          VARCHAR(100),
    fecha_logica    DATE,
    inicio          TIMESTAMP,
    fin             TIMESTAMP,
    duracion_seg    INTEGER,
    marts_cargados  INTEGER,
    registros_total INTEGER,
    estado          VARCHAR(20),
    detalle         TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag_load_analytics",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["B6", "analytics", "postgresql", "superset", "grafana", "lab"],
    default_args={"retries": 1},
)
def dag_load_analytics():

    @task
    def leer_marts_desde_s3(**context) -> dict:
        """Verifica que los 4 marts existen en S3 y retorna metadata."""
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)
        date_str = context["ds"]
        inicio   = datetime.utcnow()

        marts_info = {}
        for mart in MARTS:
            prefix = f"marts/batch/mart={mart}/"
            keys   = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []
            parquet_keys = [k for k in keys if k.endswith(".parquet")]

            if parquet_keys:
                marts_info[mart] = {"archivos": len(parquet_keys), "disponible": True}
                logger.info("✓ mart_%s: %d archivos en S3", mart, len(parquet_keys))
            else:
                marts_info[mart] = {"archivos": 0, "disponible": False}
                logger.warning("✗ mart_%s: sin archivos en S3", mart)

        disponibles = sum(1 for m in marts_info.values() if m["disponible"])
        logger.info("Marts disponibles en S3: %d / %d", disponibles, len(MARTS))

        return {
            "marts_info": marts_info,
            "date": date_str,
            "inicio": inicio.isoformat(),
        }

    @task
    def truncar_tablas_analytics(marts_data: dict) -> dict:
        """Crea tablas si no existen y las trunca para carga fresca."""
        pg = PostgresHook(postgres_conn_id=PG_ANALYTICS_CONN)

        ddls = {
            "mart_clinico":     DDL_MART_CLINICO,
            "mart_farmacia":    DDL_MART_FARMACIA,
            "mart_financiero":  DDL_MART_FINANCIERO,
            "mart_operacional": DDL_MART_OPERACIONAL,
        }

        conn = pg.get_conn()
        cur  = conn.cursor()

        for tabla, ddl in ddls.items():
            cur.execute(ddl)
            logger.info("✓ Tabla %s verificada/creada", tabla)

        for tabla in ddls.keys():
            cur.execute(f"TRUNCATE TABLE {tabla};")
            logger.info("✓ Tabla %s truncada", tabla)

        conn.commit()
        cur.close()
        conn.close()

        logger.info("✓ Tablas analíticas preparadas para carga")
        return marts_data

    @task
    def cargar_postgresql_analytics(marts_data: dict) -> dict:
        """Lee los marts desde S3 y los inserta en PostgreSQL 5433."""
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)
        pg       = PostgresHook(postgres_conn_id=PG_ANALYTICS_CONN)
        date_str = marts_data["date"]

        registros_total = 0
        marts_cargados  = 0

        for mart in MARTS:
            if not marts_data["marts_info"].get(mart, {}).get("disponible"):
                logger.warning("Saltando mart_%s — no disponible en S3", mart)
                continue

            try:
                prefix = f"marts/batch/mart={mart}/"
                keys   = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []
                parquet_keys = [k for k in keys if k.endswith(".parquet")]

                dfs = []
                for key in parquet_keys:
                    obj = s3_hook.get_key(key=key, bucket_name=BUCKET)
                    import io
                    import pyarrow.parquet as pq
                    buf = io.BytesIO(obj.get()["Body"].read())
                    dfs.append(pq.read_table(buf).to_pandas())

                if not dfs:
                    continue

                df = pd.concat(dfs, ignore_index=True)

                # Serializar columnas complejas (listas/structs) a JSON string
                for col in df.columns:
                    if df[col].dtype == object:
                        try:
                            df[col] = df[col].apply(
                                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                            )
                        except Exception:
                            pass

                df["date"]      = date_str
                df["loaded_at"] = datetime.utcnow()

                tabla = f"mart_{mart}"
                engine = pg.get_sqlalchemy_engine()
                df.to_sql(
                    name=tabla,
                    con=engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=500,
                )

                registros_total += len(df)
                marts_cargados  += 1
                logger.info("✓ mart_%s cargado: %d registros en %s", mart, len(df), tabla)

            except Exception as e:
                logger.error("Error cargando mart_%s: %s", mart, e)

        logger.info(
            "Carga completada: %d marts, %d registros totales",
            marts_cargados, registros_total
        )

        return {
            **marts_data,
            "marts_cargados":  marts_cargados,
            "registros_total": registros_total,
        }

    @task
    def refrescar_superset(carga_data: dict) -> bool:
        """Refresca el caché de Superset via API."""
        try:
            # Login en Superset
            login_resp = requests.post(
                f"{SUPERSET_URL}/api/v1/security/login",
                json={
                    "username": os.getenv("SUPERSET_USER", "admin"),
                    "password": os.getenv("SUPERSET_PASSWORD", "admin"),
                    "provider": "db",
                },
                timeout=15,
            )
            if login_resp.status_code != 200:
                logger.warning("No se pudo autenticar en Superset: %s", login_resp.status_code)
                return False

            token = login_resp.json().get("access_token")
            headers = {"Authorization": f"Bearer {token}"}

            # Refrescar caché de dashboards
            dash_resp = requests.get(
                f"{SUPERSET_URL}/api/v1/dashboard/",
                headers=headers,
                timeout=15,
            )

            if dash_resp.status_code == 200:
                dashboards = dash_resp.json().get("result", [])
                for dash in dashboards:
                    dash_id = dash.get("id")
                    requests.delete(
                        f"{SUPERSET_URL}/api/v1/dashboard/{dash_id}/cache",
                        headers=headers,
                        timeout=10,
                    )
                logger.info("✓ Superset: caché refrescado para %d dashboards", len(dashboards))
            else:
                logger.warning("No se pudieron obtener dashboards de Superset")

            return True

        except Exception as e:
            logger.warning("Superset no disponible o error al refrescar: %s", e)
            return False

    @task
    def notificar_grafana(carga_data: dict) -> bool:
        """Crea una anotación en Grafana indicando que el ciclo batch completó."""
        try:
            date_str       = carga_data["date"]
            marts_cargados = carga_data.get("marts_cargados", 0)
            registros      = carga_data.get("registros_total", 0)

            payload = {
                "text": f"Ciclo batch completado — {date_str} — {marts_cargados}/4 marts — {registros:,} registros",
                "tags": ["batch", "data-nebula", "B6"],
                "time": int(datetime.utcnow().timestamp() * 1000),
            }

            resp = requests.post(
                f"{GRAFANA_URL}/api/annotations",
                json=payload,
                auth=(
                    os.getenv("GRAFANA_USER", "admin"),
                    os.getenv("GRAFANA_PASSWORD", "admin"),
                ),
                timeout=10,
            )

            if resp.status_code in (200, 201):
                logger.info("✓ Grafana: anotación creada correctamente")
                return True
            else:
                logger.warning("Grafana respondió con status %s", resp.status_code)
                return False

        except Exception as e:
            logger.warning("Grafana no disponible o error al notificar: %s", e)
            return False

    @task
    def log_ciclo_completado(carga_data: dict, superset_ok: bool,
                              grafana_ok: bool, **context) -> None:
        """Registra en PostgreSQL 5432 que el ciclo batch finalizó."""
        pg       = PostgresHook(postgres_conn_id=PG_NEBULA_CONN)
        date_str = carga_data["date"]
        fin      = datetime.utcnow()
        inicio   = datetime.fromisoformat(carga_data["inicio"])
        duracion = int((fin - inicio).total_seconds())

        conn = pg.get_conn()
        cur  = conn.cursor()

        # Crear tabla si no existe
        cur.execute(DDL_PIPELINE_LOG)

        detalle = json.dumps({
            "superset_refrescado": superset_ok,
            "grafana_notificado":  grafana_ok,
            "marts_info":          carga_data.get("marts_info", {}),
        })

        cur.execute("""
            INSERT INTO pipeline_batch_log
                (run_id, dag_id, fecha_logica, inicio, fin,
                 duracion_seg, marts_cargados, registros_total, estado, detalle)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            context["run_id"],
            "dag_load_analytics",
            date_str,
            inicio,
            fin,
            duracion,
            carga_data.get("marts_cargados", 0),
            carga_data.get("registros_total", 0),
            "SUCCESS",
            detalle,
        ))

        conn.commit()
        cur.close()
        conn.close()

        logger.info(
            "✓ Ciclo batch registrado: %d marts, %d registros, %ds",
            carga_data.get("marts_cargados", 0),
            carga_data.get("registros_total", 0),
            duracion,
        )

    # ---- Flujo del DAG ----
    marts_data   = leer_marts_desde_s3()
    truncado     = truncar_tablas_analytics(marts_data)
    carga_data   = cargar_postgresql_analytics(truncado)
    superset_ok  = refrescar_superset(carga_data)
    grafana_ok   = notificar_grafana(carga_data)
    log_ciclo_completado(carga_data, superset_ok, grafana_ok)


dag_load_analytics()
