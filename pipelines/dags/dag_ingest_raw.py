"""
DAG B1 - dag_ingest_raw

Lee 19 CSVs desde S3 sources/, asigna sede simulada via hash(subject_id) % 3,
convierte a Parquet (PyArrow), calcula checksum MD5 y escribe particionado
en raw/batch/ por source / sede / date.

Flujo:
    verificar_fuentes -> [ingest_mimic, ingest_externos] -> validar_checksums -> registrar_metadata_ingesta

Conexiones requeridas:
    - AWS: my_s3_conn (tipo aws)

Bucket: data-nebula-clinical-lake
Lectura : sources/mimic/mimic_iv/*.csv (16 archivos)
          sources/financial/financial_data.csv
          sources/financial/healthcare_dataset.csv  (ajustar carpeta real si aplica)
          sources/operational/inventory_data.csv
Escritura: raw/batch/source=<source>/sede=<sede>/date=<YYYY-MM-DD>/<archivo>.parquet
           raw/_metadata/ingestion_log/date=<YYYY-MM-DD>/ingest_raw_<run_id>.json
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración global
# ---------------------------------------------------------------------------

AWS_CONN_ID = "my_s3_conn"
BUCKET = "data-nebula-clinical-lake"

SOURCES_PREFIX = "sources/"
RAW_PREFIX = "raw/batch/"
METADATA_PREFIX = "raw/_metadata/ingestion_log/"

N_SEDES = 3  # sede_bogota, sede_medellin, sede_cali
SEDE_NOMBRES = {0: "sede_bogota", 1: "sede_medellin", 2: "sede_cali"}

# Los 16 archivos de MIMIC-IV (ajustar nombres reales si difieren)
MIMIC_FILES = [
    "admissions.csv",
    "patients.csv",
    "transfers.csv",
    "diagnoses_icd.csv",
    "procedures_icd.csv",
    "labevents.csv",
    "prescriptions.csv",
    "services.csv",
    "drgcodes.csv",
    "emar.csv",
    "pharmacy.csv",
    "chartevents.csv",
    "icustays.csv",
    "inputevents.csv",
    "outputevents.csv",
    "procedureevents.csv",
]
MIMIC_PREFIX = "sources/mimic/mimic_iv/"

# Archivos externos: (nombre_archivo, prefijo_s3, source_label)
EXTERNAL_FILES = [
    ("healthcare_dataset.csv", "sources/financial/healthcare/", "financial"),
    ("financial_data.csv", "sources/operational/supply_chain/", "operational"),
    ("inventory_data.csv", "sources/operational/supply_chain/", "operational"),
]


# ---------------------------------------------------------------------------
# Helpers compartidos
# ---------------------------------------------------------------------------

def assign_sede(subject_id) -> int:
    """Asigna sede simulada mediante hash MD5 estable de subject_id % N_SEDES.

    Se usa hashlib en lugar de hash() builtin porque hash() de Python no es
    determinista entre procesos (PYTHONHASHSEED), lo cual rompería la
    reproducibilidad entre workers.
    """
    digest = hashlib.md5(str(subject_id).encode("utf-8")).hexdigest()
    return int(digest, 16) % N_SEDES


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Convierte un DataFrame a bytes Parquet usando PyArrow."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def md5_of_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def upload_parquet_with_checksum(
    s3_hook: S3Hook,
    df: pd.DataFrame,
    source: str,
    sede: int,
    date_str: str,
    base_filename: str,
) -> dict:
    """Convierte df a parquet, calcula checksum, sube parquet + sidecar .md5.

    Retorna metadata del archivo escrito.
    """
    parquet_bytes = df_to_parquet_bytes(df)
    checksum = md5_of_bytes(parquet_bytes)

    parquet_filename = base_filename.replace(".csv", ".parquet")
    key_prefix = f"{RAW_PREFIX}source={source}/sede={SEDE_NOMBRES[sede]}/date={date_str}/"
    parquet_key = f"{key_prefix}{parquet_filename}"
    checksum_key = f"{parquet_key}.md5"

    s3_hook.load_bytes(
        bytes_data=parquet_bytes,
        key=parquet_key,
        bucket_name=BUCKET,
        replace=True,
    )
    s3_hook.load_string(
        string_data=checksum,
        key=checksum_key,
        bucket_name=BUCKET,
        replace=True,
    )

    return {
        "source": source,
        "sede": SEDE_NOMBRES[sede],
        "date": date_str,
        "file": parquet_filename,
        "s3_key": parquet_key,
        "checksum_md5": checksum,
        "n_registros": len(df),
        "size_bytes": len(parquet_bytes),
    }


def read_csv_from_s3(s3_hook: S3Hook, key: str) -> io.BytesIO:
    """Descarga el CSV desde S3 y retorna un buffer en memoria."""
    obj = s3_hook.get_key(key, bucket_name=BUCKET)
    raw = obj.get()["Body"].read()
    return io.BytesIO(raw)


def ingest_dataframe_partitioned_by_sede(
    s3_hook: S3Hook,
    csv_buffer: io.BytesIO,
    source: str,
    date_str: str,
    base_filename: str,
    sede_col: str = "subject_id",
    chunk_size: int = 50_000,
) -> list[dict]:
    """Lee el CSV en chunks para evitar OOM, asigna sede via hash % N_SEDES,
    acumula por sede y escribe un Parquet por sede al final.

    chunk_size=50_000 filas mantiene ~100-200MB RAM por chunk en datasets MIMIC.
    """
    # Acumuladores por sede: {sede_int: [df_chunk, ...]}
    sede_buffers: dict[int, list[pd.DataFrame]] = {s: [] for s in range(N_SEDES)}
    total_rows = 0

    for chunk in pd.read_csv(csv_buffer, chunksize=chunk_size, low_memory=False):
        if sede_col in chunk.columns:
            key_series = chunk[sede_col]
        else:
            if total_rows == 0:
                logger.warning(
                    "Columna '%s' no encontrada en %s; usando índice de fila para sede",
                    sede_col,
                    base_filename,
                )
            key_series = chunk.index

        chunk = chunk.copy()
        chunk["_sede_simulada"] = key_series.map(assign_sede)

        for sede in range(N_SEDES):
            sub = chunk[chunk["_sede_simulada"] == sede].drop(columns=["_sede_simulada"])
            if not sub.empty:
                sede_buffers[sede].append(sub)

        total_rows += len(chunk)
        del chunk  # liberar memoria del chunk procesado

    # Escribir un Parquet por sede
    results = []
    for sede, chunks in sede_buffers.items():
        if not chunks:
            continue
        df_sede = pd.concat(chunks, ignore_index=True)
        result = upload_parquet_with_checksum(
            s3_hook=s3_hook,
            df=df_sede,
            source=source,
            sede=sede,
            date_str=date_str,
            base_filename=base_filename,
        )
        results.append(result)
        del df_sede, chunks  # liberar memoria tras escribir

    return results


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="dag_ingest_raw",
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["B1", "ingest", "raw", "lab"],
    default_args={"retries": 1},
)
def dag_ingest_raw():

    @task
    def verificar_fuentes(**context) -> dict:
        """Confirma que los 19 CSVs estén presentes y accesibles en sources/."""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        expected = []
        for f in MIMIC_FILES:
            expected.append(f"{MIMIC_PREFIX}{f}")
        for fname, prefix, _ in EXTERNAL_FILES:
            expected.append(f"{prefix}{fname}")

        logger.info("Verificando %d archivos esperados en sources/", len(expected))

        missing = []
        present = []
        for key in expected:
            if s3_hook.check_for_key(key, bucket_name=BUCKET):
                present.append(key)
            else:
                missing.append(key)

        logger.info("Archivos presentes: %d / %d", len(present), len(expected))
        if missing:
            logger.error("Archivos faltantes: %s", missing)
            raise FileNotFoundError(
                f"Faltan {len(missing)} archivos en sources/: {missing}"
            )

        return {
            "expected_count": len(expected),
            "present_count": len(present),
            "missing": missing,
            "checked_at": datetime.utcnow().isoformat(),
        }

    @task
    def ingest_mimic(verificacion: dict, **context) -> list[dict]:
        """Lee los 16 CSVs de MIMIC-IV, asigna sede via hash(subject_id) % 3,
        convierte a Parquet y escribe particionado en raw/batch/."""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        date_str = context["ds"]

        all_results: list[dict] = []
        for filename in MIMIC_FILES:
            key = f"{MIMIC_PREFIX}{filename}"
            logger.info("Leyendo %s", key)
            csv_buffer = read_csv_from_s3(s3_hook, key)

            results = ingest_dataframe_partitioned_by_sede(
                s3_hook=s3_hook,
                csv_buffer=csv_buffer,
                source="mimic",
                date_str=date_str,
                base_filename=filename,
                sede_col="subject_id",
            )
            logger.info("%s -> %d particiones escritas", filename, len(results))
            all_results.extend(results)

        return all_results

    @task
    def ingest_externos(verificacion: dict, **context) -> list[dict]:
        """Lee healthcare_dataset.csv, inventory_data.csv y financial_data.csv,
        asigna sede simulada, convierte a Parquet y escribe particionado
        en raw/batch/ con source=financial|operational."""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        date_str = context["ds"]

        all_results: list[dict] = []
        for filename, prefix, source_label in EXTERNAL_FILES:
            key = f"{prefix}{filename}"
            logger.info("Leyendo %s", key)
            csv_buffer = read_csv_from_s3(s3_hook, key)

            results = ingest_dataframe_partitioned_by_sede(
                s3_hook=s3_hook,
                csv_buffer=csv_buffer,
                source=source_label,
                date_str=date_str,
                base_filename=filename,
                sede_col="subject_id",  # fallback a índice si no existe
            )
            logger.info("%s -> %d particiones escritas", filename, len(results))
            all_results.extend(results)

        return all_results

    @task
    def validar_checksums(mimic_results: list[dict], externos_results: list[dict]) -> dict:
        """Recalcula MD5 de cada parquet en raw/batch/ y compara contra el
        checksum generado durante la ingesta."""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        all_results = mimic_results + externos_results
        validated = []
        corrupted = []

        for item in all_results:
            obj = s3_hook.get_key(item["s3_key"], bucket_name=BUCKET)
            data = obj.get()["Body"].read()
            recalculated = md5_of_bytes(data)

            ok = recalculated == item["checksum_md5"]
            entry = {
                "s3_key": item["s3_key"],
                "expected_md5": item["checksum_md5"],
                "actual_md5": recalculated,
                "ok": ok,
            }
            if ok:
                validated.append(entry)
            else:
                corrupted.append(entry)
                logger.error("Checksum NO coincide para %s", item["s3_key"])

        logger.info(
            "Validación: %d OK, %d corruptos de %d archivos",
            len(validated),
            len(corrupted),
            len(all_results),
        )

        if corrupted:
            raise ValueError(f"Archivos con checksum inválido: {corrupted}")

        return {
            "total_files": len(all_results),
            "validated": len(validated),
            "corrupted": len(corrupted),
            "details": validated,
        }

    @task
    def registrar_metadata_ingesta(
        mimic_results: list[dict],
        externos_results: list[dict],
        validacion: dict,
        **context,
    ) -> dict:
        """Escribe el log de metadata de la ingesta en PostgreSQL 5432
        y también como JSON en S3 raw/_metadata/ para trazabilidad."""
        import sqlalchemy as sa

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        date_str = context["ds"]
        run_id = context["run_id"]
        all_results = mimic_results + externos_results

        dag_run = context["dag_run"]
        duration_seconds = None
        try:
            start = dag_run.start_date
            if start:
                duration_seconds = (datetime.utcnow() - start.replace(tzinfo=None)).total_seconds()
        except Exception:
            logger.warning("No se pudo calcular duración del DAG run")

        records_by_source: dict[str, int] = {}
        for item in all_results:
            records_by_source[item["source"]] = (
                records_by_source.get(item["source"], 0) + item["n_registros"]
            )

        logged_at = datetime.utcnow()

        # ── 1. Escribir en PostgreSQL 5432 ──────────────────────────────────
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg_hook = PostgresHook(postgres_conn_id="postgres_nebula")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Crear tabla si no existe (run_id UNIQUE evita duplicados)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_log (
                id              SERIAL PRIMARY KEY,
                dag_id          VARCHAR(255),
                run_id          VARCHAR(512) UNIQUE,
                execution_date  DATE,
                duration_seconds FLOAT,
                files_processed INTEGER,
                records_mimic   INTEGER,
                records_financial INTEGER,
                records_operational INTEGER,
                checksums_total INTEGER,
                checksums_ok    INTEGER,
                checksums_failed INTEGER,
                logged_at       TIMESTAMP
            );
        """)

        cursor.execute("""
            INSERT INTO ingestion_log (
                dag_id, run_id, execution_date, duration_seconds,
                files_processed, records_mimic, records_financial,
                records_operational, checksums_total, checksums_ok,
                checksums_failed, logged_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
        """, (
            "dag_ingest_raw",
            run_id,
            date_str,
            duration_seconds,
            len(all_results),
            records_by_source.get("mimic", 0),
            records_by_source.get("financial", 0),
            records_by_source.get("operational", 0),
            validacion["total_files"],
            validacion["validated"],
            validacion["corrupted"],
            logged_at,
        ))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Metadata registrada en PostgreSQL (ingestion_log)")

        # ── 2. También escribir JSON en S3 como respaldo ────────────────────
        log_payload = {
            "dag_id": "dag_ingest_raw",
            "run_id": run_id,
            "execution_date": date_str,
            "duration_seconds": duration_seconds,
            "files_processed": len(all_results),
            "records_by_source": records_by_source,
            "checksum_validation": {
                "total_files": validacion["total_files"],
                "validated": validacion["validated"],
                "corrupted": validacion["corrupted"],
            },
            "files": all_results,
            "logged_at": logged_at.isoformat(),
        }

        log_key = f"{METADATA_PREFIX}date={date_str}/ingest_raw_{run_id}.json"
        s3_hook.load_string(
            string_data=json.dumps(log_payload, indent=2, default=str),
            key=log_key,
            bucket_name=BUCKET,
            replace=True,
        )

        logger.info("Metadata de ingesta registrada en s3://%s/%s", BUCKET, log_key)

        return {"metadata_key": log_key, "files_processed": len(all_results)}

    # ---- Flujo del DAG ----
    verificacion = verificar_fuentes()
    mimic_results = ingest_mimic(verificacion)
    externos_results = ingest_externos(verificacion)
    validacion = validar_checksums(mimic_results, externos_results)
    registrar_metadata_ingesta(mimic_results, externos_results, validacion)


dag_ingest_raw()
