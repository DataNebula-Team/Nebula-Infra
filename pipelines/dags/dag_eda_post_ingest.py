"""
DAG B2 - dag_eda_post_ingest

Lee los Parquets de raw/batch/ en S3, ejecuta tests de calidad en paralelo
y aplica un Quality Gate para decidir si el pipeline continúa hacia B3.

Flujo:
    [test_nulos_y_duplicados, test_integridad_referencial]
        → generar_reporte_calidad
        → quality_gate (ShortCircuitOperator)

Conexiones requeridas:
    - AWS:      my_s3_conn
    - Postgres: postgres_nebula

Bucket: data-nebula-clinical-lake
Lectura:  raw/batch/source=mimic/sede=*/date=<ds>/*.parquet
Escritura: PostgreSQL → public.nebula_quality_report, public.nebula_quality_gate_log
"""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

AWS_CONN_ID     = "my_s3_conn"
PG_CONN_ID      = "postgres_nebula"
BUCKET          = "data-nebula-clinical-lake"
RAW_PREFIX      = "raw/batch/"
NULL_THRESHOLD  = 5.0  # % máximo de nulos críticos por sede

# Campos críticos por tabla
CRITICAL_FIELDS = {
    "admissions":      ["subject_id", "hadm_id", "admittime"],
    "diagnoses_icd":   ["subject_id", "hadm_id", "icd_code"],
    "procedures_icd":  ["subject_id", "hadm_id", "icd_code"],
    "chartevents":     ["subject_id", "hadm_id", "stay_id"],
    "labevents":       ["subject_id", "hadm_id"],
    "prescriptions":   ["subject_id", "hadm_id"],
    "emar":            ["subject_id", "hadm_id"],
    "pharmacy":        ["subject_id", "hadm_id"],
    "inputevents":     ["subject_id", "hadm_id", "stay_id"],
    "outputevents":    ["subject_id", "hadm_id", "stay_id"],
    "procedureevents": ["subject_id", "hadm_id", "stay_id"],
    "icustays":        ["subject_id", "hadm_id", "stay_id"],
    "transfers":       ["subject_id", "hadm_id"],
    "services":        ["subject_id", "hadm_id"],
    "drgcodes":        ["subject_id", "hadm_id"],
    "patients":        ["subject_id"],
}

# Claves primarias por tabla
PRIMARY_KEYS = {
    "admissions":      ["hadm_id"],
    "patients":        ["subject_id"],
    "transfers":       ["transfer_id"],
    "diagnoses_icd":   ["subject_id", "hadm_id", "seq_num"],
    "procedures_icd":  ["subject_id", "hadm_id", "seq_num"],
    "labevents":       ["labevent_id"],
    "prescriptions":   ["pharmacy_id"],
    "services":        ["subject_id", "hadm_id", "transfertime"],
    "drgcodes":        ["subject_id", "hadm_id", "drg_type"],
    "emar":            ["emar_id"],
    "pharmacy":        ["pharmacy_id"],
    "chartevents":     ["charttime", "subject_id", "itemid"],
    "icustays":        ["stay_id"],
    "inputevents":     ["orderid"],
    "outputevents":    ["charttime", "subject_id", "itemid"],
    "procedureevents": ["orderid"],
}

# FK checks: (tabla_hijo, fk_col, tabla_padre, pk_col)
REF_CHECKS = [
    ("diagnoses_icd", "hadm_id", "admissions", "hadm_id"),
    ("chartevents",   "stay_id", "icustays",   "stay_id"),
    ("prescriptions", "hadm_id", "admissions",  "hadm_id"),
    ("labevents",     "hadm_id", "admissions",  "hadm_id"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_latest_date(s3_hook: S3Hook, date_str: str, source: str) -> str:
    """Retorna date_str si hay datos, si no la última fecha disponible."""
    prefix = f"{RAW_PREFIX}source={source}/"
    keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []
    parquet_keys = [k for k in keys if k.endswith(".parquet")]

    if any(f"date={date_str}" in k for k in parquet_keys):
        return date_str

    dates = sorted(set(
        k.split("date=")[1].split("/")[0]
        for k in parquet_keys if "date=" in k
    ))
    if dates:
        latest = dates[-1]
        logger.warning("Sin datos para %s — usando fecha: %s", date_str, latest)
        return latest
    return date_str


def list_keys_for(s3_hook: S3Hook, source: str, sede: str, date_str: str) -> list[str]:
    prefix = f"{RAW_PREFIX}source={source}/sede={sede}/date={date_str}/"
    keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []
    return [k for k in keys if k.endswith(".parquet")]


def read_columns_from_s3(s3_hook: S3Hook, key: str, columns: list[str]) -> pd.DataFrame:
    """Lee solo las columnas necesarias de un Parquet en S3 (ahorra memoria)."""
    obj = s3_hook.get_key(key, bucket_name=BUCKET)
    buf = io.BytesIO(obj.get()["Body"].read())
    table = pq.read_table(buf)
    existing = [c for c in columns if c in table.schema.names]
    if existing:
        table = table.select(existing)
    return table.to_pandas()


def read_full_from_s3(s3_hook: S3Hook, key: str) -> pd.DataFrame:
    obj = s3_hook.get_key(key, bucket_name=BUCKET)
    buf = io.BytesIO(obj.get()["Body"].read())
    return pq.read_table(buf).to_pandas()


def table_name_from_key(key: str) -> str:
    return key.split("/")[-1].replace(".parquet", "")


def ensure_quality_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.nebula_quality_report (
            id              SERIAL PRIMARY KEY,
            dag_run_id      VARCHAR(512),
            execution_date  DATE,
            sede            VARCHAR(50),
            source          VARCHAR(50),
            tabla           VARCHAR(100),
            total_registros INTEGER,
            nulos_criticos  INTEGER,
            pct_nulos       FLOAT,
            duplicados      INTEGER,
            logged_at       TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.nebula_quality_gate_log (
            id              SERIAL PRIMARY KEY,
            dag_run_id      VARCHAR(512),
            execution_date  DATE,
            resultado       VARCHAR(10),
            motivo          TEXT,
            sedes_fallidas  TEXT,
            logged_at       TIMESTAMP
        );
    """)
    cursor.connection.commit()


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag_eda_post_ingest",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["B2", "quality", "eda", "lab"],
    default_args={"retries": 1},
)
def dag_eda_post_ingest():

    @task
    def test_nulos_y_duplicados(**context) -> list[dict]:
        """Task 1 — mide % de nulos en campos críticos y duplicados por PK,
        tabla por tabla por sede. Lee solo columnas necesarias para ahorrar RAM."""
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)
        date_str = context["ds"]
        date_str = get_latest_date(s3_hook, date_str, "mimic")
        results  = []

        for sede in ["sede_bogota", "sede_medellin", "sede_cali"]:
            keys = list_keys_for(s3_hook, "mimic", sede, date_str)

            for key in keys:
                table_name = table_name_from_key(key)
                critical   = CRITICAL_FIELDS.get(table_name, [])
                pk_cols    = PRIMARY_KEYS.get(table_name, [])
                cols_needed = list(set(critical + pk_cols))

                if not cols_needed:
                    continue

                try:
                    df = read_columns_from_s3(s3_hook, key, cols_needed)
                except Exception as e:
                    logger.warning("Error leyendo %s: %s", key, e)
                    continue

                total = len(df)
                if total == 0:
                    continue

                # Nulos en campos críticos
                existing_critical = [c for c in critical if c in df.columns]
                nulos = int(df[existing_critical].isnull().any(axis=1).sum()) if existing_critical else 0
                pct_nulos = round((nulos / total) * 100, 4)

                # Duplicados por PK
                existing_pk = [c for c in pk_cols if c in df.columns]
                duplicados  = int(df.duplicated(subset=existing_pk).sum()) if existing_pk else 0

                results.append({
                    "sede":            sede,
                    "source":          "mimic",
                    "tabla":           table_name,
                    "total_registros": int(total),
                    "nulos_criticos":  nulos,
                    "pct_nulos":       float(pct_nulos),
                    "duplicados":      duplicados,
                })
                logger.info(
                    "[%s][%s] total=%d nulos=%d (%.2f%%) dupes=%d",
                    sede, table_name, total, nulos, pct_nulos, duplicados,
                )
                del df

        return results

    @task
    def test_integridad_referencial(**context) -> list[dict]:
        """Task 2 — valida FK entre tablas MIMIC por sede.
        Lee SOLO las columnas FK/PK necesarias para cada check (ahorra RAM)."""
        s3_hook  = S3Hook(aws_conn_id=AWS_CONN_ID)
        date_str = context["ds"]
        date_str = get_latest_date(s3_hook, date_str, "mimic")
        results  = []

        for sede in ["sede_bogota", "sede_medellin", "sede_cali"]:
            keys = list_keys_for(s3_hook, "mimic", sede, date_str)
            key_map = {table_name_from_key(k): k for k in keys}

            for child_table, fk_col, parent_table, pk_col in REF_CHECKS:
                child_key  = key_map.get(child_table)
                parent_key = key_map.get(parent_table)

                if not child_key or not parent_key:
                    logger.warning("[%s] Falta %s o %s", sede, child_table, parent_table)
                    continue

                try:
                    child_df  = read_columns_from_s3(s3_hook, child_key,  [fk_col])
                    parent_df = read_columns_from_s3(s3_hook, parent_key, [pk_col])
                except Exception as e:
                    logger.warning("[%s] Error leyendo tablas FK: %s", sede, e)
                    continue

                if fk_col not in child_df.columns or pk_col not in parent_df.columns:
                    logger.warning("[%s] Columna FK/PK no encontrada", sede)
                    continue

                parent_keys   = set(parent_df[pk_col].dropna().unique())
                child_vals    = child_df[fk_col].dropna()
                huerfanos     = int((~child_vals.isin(parent_keys)).sum())
                total_child   = int(len(child_df))
                pct_huerfanos = round((huerfanos / total_child) * 100, 4) if total_child > 0 else 0.0

                results.append({
                    "sede":            sede,
                    "child_table":     child_table,
                    "parent_table":    parent_table,
                    "fk_col":          fk_col,
                    "total_registros": total_child,
                    "huerfanos":       huerfanos,
                    "pct_huerfanos":   float(pct_huerfanos),
                })
                logger.info(
                    "[%s] %s.%s → %s.%s : %d huérfanos (%.2f%%)",
                    sede, child_table, fk_col, parent_table, pk_col,
                    huerfanos, pct_huerfanos,
                )
                del child_df, parent_df

        return results

    @task
    def generar_reporte_calidad(
        nulos_results: list[dict],
        integridad_results: list[dict],
        **context,
    ) -> dict:
        """Task 3 — consolida resultados por sede y guarda en PostgreSQL."""
        pg_hook  = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn     = pg_hook.get_conn()
        cursor   = conn.cursor()
        ensure_quality_tables(cursor)

        date_str  = context["ds"]
        run_id    = context["run_id"]
        logged_at = datetime.utcnow()

        for item in nulos_results:
            cursor.execute("""
                INSERT INTO public.nebula_quality_report
                    (dag_run_id, execution_date, sede, source, tabla,
                     total_registros, nulos_criticos, pct_nulos, duplicados, logged_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                run_id, date_str,
                str(item["sede"]), str(item["source"]), str(item["tabla"]),
                int(item["total_registros"]), int(item["nulos_criticos"]),
                float(item["pct_nulos"]), int(item["duplicados"]), logged_at,
            ))
        conn.commit()

        # Resumen por sede
        sede_summary: dict[str, dict] = {}
        for item in nulos_results:
            sede = item["sede"]
            if sede not in sede_summary:
                sede_summary[sede] = {
                    "total_registros":  0,
                    "total_nulos":      0,
                    "max_pct_nulos":    0.0,
                    "tablas_fallidas":  [],
                    "duplicados_total": 0,
                    "huerfanos_total":  0,
                }
            s = sede_summary[sede]
            s["total_registros"]  += item["total_registros"]
            s["total_nulos"]      += item["nulos_criticos"]
            s["duplicados_total"] += item["duplicados"]
            if item["pct_nulos"] > s["max_pct_nulos"]:
                s["max_pct_nulos"] = item["pct_nulos"]
            if item["pct_nulos"] > NULL_THRESHOLD:
                s["tablas_fallidas"].append(f"{item['tabla']} ({item['pct_nulos']}%)")

        for item in integridad_results:
            if item["sede"] in sede_summary:
                sede_summary[item["sede"]]["huerfanos_total"] += item["huerfanos"]

        cursor.close()
        conn.close()

        for sede, s in sede_summary.items():
            logger.info(
                "[%s] max_pct_nulos=%.2f%% duplicados=%d huerfanos=%d",
                sede, s["max_pct_nulos"], s["duplicados_total"], s["huerfanos_total"],
            )

        return {
            "run_id":         run_id,
            "execution_date": date_str,
            "sede_summary":   sede_summary,
            "threshold":      NULL_THRESHOLD,
        }

    def _quality_gate_fn(**context) -> bool:
        """ShortCircuitOperator — lee PostgreSQL y decide si continúa a B3."""
        pg_hook  = PostgresHook(postgres_conn_id=PG_CONN_ID)
        conn     = pg_hook.get_conn()
        cursor   = conn.cursor()
        ensure_quality_tables(cursor)

        date_str = context["ds"]
        run_id   = context["run_id"]

        cursor.execute("""
            SELECT sede, MAX(pct_nulos) as max_pct
            FROM public.nebula_quality_report
            WHERE dag_run_id = %s
            GROUP BY sede
            ORDER BY sede
        """, (run_id,))
        rows = cursor.fetchall()

        sedes_fallidas = [
            f"{sede} ({max_pct:.2f}%)"
            for sede, max_pct in rows
            if max_pct > NULL_THRESHOLD
        ]

        passed    = len(sedes_fallidas) == 0
        resultado = "PASS" if passed else "FAIL"
        motivo    = (
            "Todas las sedes dentro del umbral"
            if passed
            else f"Sedes con nulos > {NULL_THRESHOLD}%: {', '.join(sedes_fallidas)}"
        )

        cursor.execute("""
            INSERT INTO public.nebula_quality_gate_log
                (dag_run_id, execution_date, resultado, motivo, sedes_fallidas, logged_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            run_id, date_str, resultado, motivo,
            json.dumps(sedes_fallidas), datetime.utcnow(),
        ))
        conn.commit()
        cursor.close()
        conn.close()

        if passed:
            logger.info("Quality Gate: PASS — %s", motivo)
        else:
            logger.warning("Quality Gate: FAIL — %s", motivo)

        return passed

    # ── Flujo ──────────────────────────────────────────────────────────
    nulos_results      = test_nulos_y_duplicados()
    integridad_results = test_integridad_referencial()
    reporte = generar_reporte_calidad(nulos_results, integridad_results)

    quality_gate = ShortCircuitOperator(
        task_id="quality_gate",
        python_callable=_quality_gate_fn,
    )

    reporte >> quality_gate


dag_eda_post_ingest()
