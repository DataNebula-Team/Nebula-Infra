# Master — SRV-MASTER

## Rol en el proyecto
Es el cerebro del cluster: orquesta todo el pipeline de Data Nebula.
Corre Apache Airflow (Scheduler, API server, DAG processor, Triggerer,
Flower) y PostgreSQL 5432, la base de datos operacional donde vive todo
el estado de Airflow: DAGs, ejecuciones, conexiones y resultados de
tareas. Si esta instancia cae, ningún DAG se programa ni se ejecuta —
el pipeline completo se detiene.

## Datos técnicos
**IP privada:** 10.0.2.54 · **Tipo EC2:** t3.small (2 vCPU / 2 GB) ·
**Imagen:** build custom desde el `Dockerfile` de la raíz del repo
(Airflow + providers de Amazon + pyarrow)

## Contenedores
| Contenedor | Servicio | Puerto |
|---|---|---|
| master-airflow-scheduler-1 | Scheduler — decide qué corre y cuándo | — |
| master-airflow-webserver-1 | API server (UI de Airflow) | 8080 |
| master-airflow-dag-processor-1 | Parsea los DAGs de `pipelines/dags/` | — |
| master-airflow-triggerer-1 | Tareas asíncronas (deferrable) | — |
| master-flower-1 | Monitor de Celery | 5555 |
| master-postgres-1 | PostgreSQL 16 (BD operacional) | 5432 |

## Dependencias
- **Depende de:** SRV-RABBIT (broker Celery, debe estar arriba antes).
- **Usada por:** los 3 Workers (leen la BD y el broker), SRV-CLUSTER-SPARK
  (recibe jobs lanzados desde los DAGs), Grafana (métricas del pipeline).
- **Orden de arranque:** después de PROXY y RABBIT; antes de los Workers.

## Configuración
1. `cp .env.example .env` y completar valores (pedir por canal seguro).
2. La imagen se construye desde el `Dockerfile` de la raíz
   (`build: context: .. dockerfile: Dockerfile`).
3. `docker compose up -d`. El servicio `airflow-init` migra la BD y crea
   el usuario admin solo en el primer arranque. Esperar ~40s antes de
   recargar la UI.

## Cómo fluye un DAG desde aquí
1. El Scheduler lee `pipelines/dags/` (montado como volumen) y encola
   las tareas en RabbitMQ (10.0.2.61).
2. Un Celery Worker toma la tarea. Si es un job Spark (B3-B5), la lanza
   vía REST API al clúster dedicado (11.0.2.183:6066, via VPC Peering).
3. El resultado de cada tarea vuelve por el Celery Result Backend, que
   apunta a este PostgreSQL — así la UI refleja éxito/fallo.
4. B6 cierra el ciclo cargando los marts en SRV-ANALYTICS (10.0.2.10).

## Decisiones y aprendizajes reales
- **CeleryExecutor sobre LocalExecutor:** permite distribuir tareas en
  3 Workers dedicados en vez de saturar esta instancia, que ya corre
  5 procesos simultáneos.
- **`api-server` en vez de `webserver`:** el proyecto migró a Airflow 3,
  que reemplaza el comando clásico. Mismo motivo del cambio en el
  healthcheck y el comando `celery worker` de los Workers.
- **PostgreSQL 13 → 16:** actualizado durante el proyecto sin fricción
  al recrear el volumen.
- **Error real #1:** `AIRFLOW__CELERY__RESULT_BACKEND` apunta a
  PostgreSQL (10.0.2.54), NO a RabbitMQ (10.0.2.61). Confundir estas
  IPs rompió el pipeline en el diagnóstico inicial del proyecto.
- **504 Gateway Timeout en la UI:** pool de conexiones saturado —
  `docker compose restart` y esperar 1 minuto.

## Notas operativas
- Fuente de verdad de los 7 DAGs (B1-B6 + S1): `pipelines/dags/` en la
  raíz del repo. Tras modificar un DAG aquí, distribuirlo a los Workers
  vía scp (ver docs/04_OPERACION_Y_ACCESOS.md).
- `AIRFLOW__CORE__FERNET_KEY` debe ser idéntica en Master y los 3
  Workers — si difiere, los Workers no pueden leer las conexiones
  encriptadas y fallan silenciosamente.
- Logs remotos en `s3://nebula2-airflow-logs/logs`.
- UI: `nebula-airflow.coderhivex.com` · Flower: `nebula-flower.coderhivex.com`
