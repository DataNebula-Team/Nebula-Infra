"""
DAG S1 - dag_simulate_streaming

Genera eventos simulados a partir de datasets MIMIC en raw/batch/ y los
deposita en S3 raw/streaming/ en el formato que Kafka produciría en producción.

Solo existe en el lab — en producción Kafka y sus productores lo reemplazarían.

Flujo:
    seleccionar_registros_mimic → transformar_a_eventos → escribir_raw_streaming → log_simulacion

Conexiones requeridas:
    - AWS:      my_s3_conn
    - Postgres: postgres_nebula

Bucket:   data-nebula-clinical-lake
Lectura:  raw/batch/source=mimic/sede=*/date=*/*.parquet
Escritura: raw/streaming/topic=<topic>/sede=<sede>/date=<YYYY-MM-DD>/hour=<HH>/<uuid>.parquet
           PostgreSQL → public.streaming_simulation_log
"""

from __future__ import annotations

import io
import json
import logging
import uuid
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

AWS_CONN_ID    = "my_s3_conn"
PG_CONN_ID     = "postgres_nebula"
BUCKET         = "data-nebula-clinical-lake"
RAW_BATCH      = "raw/batch/source=mimic/"
RAW_STREAMING  = "raw/streaming/"

# Cantidad de registros por lote (configurable)
BATCH_SIZE = 500

# Tablas fuente y su event_type mapeado
SOURCE_TABLES = {
    "chartevents":   "vitales",
    "prescriptions": "medicamentos",
    "admissions":    "urgencias",   # solo admisiones de tipo urgencia
}

SEDES = ["sede_0", "sede_1", "sede_2"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_latest_date(s3_hook: S3Hook) -> str:
    """Encuentra la última fecha disponible en raw/batch/source=mimic/."""
    keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=RAW_BATCH) or []
    dates = sorted(set(
        k.split("date=")[1].split("/")[0]
        for k in keys if "date=" in k
    ))
    return dates[-1] if dates else datetime.utcnow().strftime("%Y-%m-%d")


def read_parquet_columns(s3_hook: S3Hook, key: str, columns: list[str]) -> pd.DataFrame:
    """Lee solo las columnas necesarias de un Parquet en S3."""
    obj = s3_hook.get_key(key, bucket_name=BUCKET)
    buf = io.BytesIO(obj.get()["Body"].read())
    table = pq.read_table(buf)
    existing = [c for c in columns if c in table.schema.names]
    return table.select(existing).to_pandas() if existing else pd.DataFrame()


def ensure_simulation_log_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.streaming_simulation_log (
            id              SERIAL PRIMARY KEY,
            dag_run_id      VARCHAR(512),
            execution_date  DATE,
            topic           VARCHAR(100),
            sede            VARCHAR(50),
            n_eventos       INTEGER,
            batch_size      INTEGER,
            s3_key          VARCHAR(1024),
            logged_at       TIMESTAMP
        );
    """)
    cursor.connection.commit()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag_simulate_streaming",
    schedule="*/30 * * * *",   # cada 30 minutos durante sesiones de trabajo
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["S1", "streaming", "simulation", "lab"],
    default_args={"retries": 1},
)
def dag_simulate_streaming():

    @task
    def seleccionar_registros_mimic(**context) -> dict:
        """Task 1 — selecciona aleatoriamente BATCH_SIZE registros desde
        raw/batch/ para chartevents, prescriptions y admissions por sede."""
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)
        date_str = get_latest_date(s3_hook)
        logger.info("Usando datos de fecha: %s", date_str)

        selected: dict[str, dict[str, list]] = {}

        for sede in SEDES:
            selected[sede] = {}
            for table, event_type in SOURCE_TABLES.items():
                key = f"{RAW_BATCH}sede={sede}/date={date_str}/{table}.parquet"

                if not s3_hook.check_for_key(key, bucket_name=BUCKET):
                    logger.warning("No encontrado: %s", key)
                    continue

                # Columnas mínimas necesarias por tabla
                cols_map = {
                    "chartevents":   ["subject_id", "hadm_id", "stay_id", "charttime", "itemid", "value", "valuenum", "valueuom"],
                    "prescriptions": ["subject_id", "hadm_id", "drug", "dose_val_rx", "dose_unit_rx", "starttime", "stoptime"],
                    "admissions":    ["subject_id", "hadm_id", "admittime", "dischtime", "admission_type", "admission_location"],
                }
                cols = cols_map.get(table, [])

                try:
                    df = read_parquet_columns(s3_hook, key, cols)
                except Exception as e:
                    logger.warning("Error leyendo %s: %s", key, e)
                    continue

                if df.empty:
                    continue

                # Para admissions: filtrar solo urgencias
                if table == "admissions" and "admission_type" in df.columns:
                    df = df[df["admission_type"].str.upper().str.contains("URGENT|EMERGENCY", na=False)]

                # Muestra aleatoria hasta BATCH_SIZE
                sample_size = min(BATCH_SIZE, len(df))
                df_sample = df.sample(n=sample_size, random_state=42)

                selected[sede][table] = df_sample.to_dict(orient="records")
                logger.info("[%s][%s] %d registros seleccionados", sede, table, len(df_sample))
                del df, df_sample

        return selected

    @task
    def transformar_a_eventos(selected: dict) -> dict:
        """Task 2 — transforma los registros seleccionados en eventos de streaming:
        - Asigna event_id único (UUID)
        - Cambia timestamp a la fecha/hora actual
        - Clasifica en event_type según la tabla origen
        - Mantiene la sede del registro original
        """
        now = datetime.utcnow()
        now_str = now.isoformat()

        events: dict[str, dict[str, list]] = {}

        topic_map = {
            "chartevents":   "vitales",
            "prescriptions": "medicamentos",
            "admissions":    "urgencias",
        }

        for sede, tables in selected.items():
            events[sede] = {}
            for table, records in tables.items():
                topic = topic_map.get(table, "otros")
                transformed = []

                for record in records:
                    event = {
                        "event_id":    str(uuid.uuid4()),
                        "event_type":  topic,
                        "event_ts":    now_str,
                        "sede":        sede,
                        "source_table": table,
                        **{k: str(v) if v is not None else None for k, v in record.items()},
                    }
                    transformed.append(event)

                events[sede][topic] = transformed
                logger.info("[%s][%s] %d eventos generados", sede, topic, len(transformed))

        return events

    @task
    def escribir_raw_streaming(events: dict, **context) -> list[dict]:
        """Task 3 — escribe los eventos en S3 raw/streaming/ en formato Parquet
        con compresión Snappy, particionado por topic / sede / date / hour."""
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)
        now      = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")

        written = []

        for sede, topics in events.items():
            for topic, records in topics.items():
                if not records:
                    continue

                df = pd.DataFrame(records)
                table = pa.Table.from_pandas(df, preserve_index=False)

                buf = io.BytesIO()
                pq.write_table(table, buf, compression="snappy")
                parquet_bytes = buf.getvalue()

                file_uuid = str(uuid.uuid4())
                key = (
                    f"{RAW_STREAMING}"
                    f"topic={topic}/"
                    f"sede={sede}/"
                    f"date={date_str}/"
                    f"hour={hour_str}/"
                    f"{file_uuid}.parquet"
                )

                s3_hook.load_bytes(
                    bytes_data=parquet_bytes,
                    key=key,
                    bucket_name=BUCKET,
                    replace=True,
                )

                entry = {
                    "topic":     topic,
                    "sede":      sede,
                    "date":      date_str,
                    "hour":      hour_str,
                    "n_eventos": len(records),
                    "s3_key":    key,
                    "size_bytes": len(parquet_bytes),
                }
                written.append(entry)
                logger.info(
                    "[%s][%s] %d eventos → s3://%s/%s",
                    sede, topic, len(records), BUCKET, key,
                )
                del df, table, buf

        return written

    @task
    def log_simulacion(written: list[dict], **context) -> dict:
        """Task 4 — registra en PostgreSQL cuántos eventos se generaron,
        de qué tipos, para qué sedes y el timestamp de la generación."""
        pg_hook  = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn     = pg_hook.get_conn()
        cursor   = conn.cursor()
        ensure_simulation_log_table(cursor)

        run_id    = context["run_id"]
        date_str  = context["ds"]
        logged_at = datetime.utcnow()

        total_eventos = 0
        for item in written:
            cursor.execute("""
                INSERT INTO public.streaming_simulation_log
                    (dag_run_id, execution_date, topic, sede, n_eventos,
                     batch_size, s3_key, logged_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                run_id, date_str,
                item["topic"], item["sede"], int(item["n_eventos"]),
                BATCH_SIZE, item["s3_key"], logged_at,
            ))
            total_eventos += item["n_eventos"]

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(
            "Simulación registrada: %d archivos, %d eventos totales",
            len(written), total_eventos,
        )

        resumen = {}
        for item in written:
            key = f"{item['topic']}/{item['sede']}"
            resumen[key] = item["n_eventos"]

        return {
            "run_id":        run_id,
            "total_eventos": total_eventos,
            "archivos":      len(written),
            "resumen":       resumen,
        }

    # ── Flujo secuencial ───────────────────────────────────────────────
    registros = seleccionar_registros_mimic()
    eventos   = transformar_a_eventos(registros)
    written   = escribir_raw_streaming(eventos)
    log_simulacion(written)


dag_simulate_streaming()
