# Workers — SRV-WORKER1/2/3

## Rol en el proyecto
Son los brazos ejecutores del pipeline: reciben tareas de Airflow via
Celery/RabbitMQ y las procesan. Cuando una tarea es un job Spark
(B3-B5), el Worker no lo ejecuta localmente — lo lanza vía REST API al
clúster dedicado (SRV-CLUSTER-SPARK) y espera el resultado. Sin al
menos un Worker activo, ninguna tarea del pipeline se ejecuta aunque
el Scheduler la programe correctamente.

## Datos técnicos
**IPs privadas:** 10.0.2.240 / 10.0.2.95 / 10.0.2.79 · **Tipo EC2:**
t3.small (2 vCPU / 2 GB) · **Imagen:** build custom desde el
`Dockerfile` de la raíz del repo (igual que Master)

## Contenedores
| Contenedor | Servicio |
|---|---|
| workers-airflow-worker-1 | Celery Worker (`celery worker --autoscale 4,1`) |

## Dependencias
- **Depende de:** SRV-RABBIT (recibe tareas), SRV-MASTER (BD de estado),
  SRV-CLUSTER-SPARK (ejecuta jobs pesados vía REST).
- **Usada por:** nadie directamente — es el último eslabón que ejecuta.
- **Orden de arranque:** después de RabbitMQ y Master.

## Configuración
1. `cp .env.example .env` y completar valores. Cambiar `CELERY_HOSTNAME`
   según la instancia (`WORKER-1`, `WORKER-2`, `WORKER-3`) — es la
   única variable que difiere entre las tres.
2. `docker compose up -d`

## Cómo fluye una tarea aquí
1. El Worker escucha la cola `default` en RabbitMQ.
2. Al recibir una tarea, la ejecuta (tareas Python simples) o la
   despacha al clúster Spark si es un job B3-B5.
3. El resultado se reporta de vuelta al Celery Result Backend en
   PostgreSQL (Master) — no se guarda nada localmente en el Worker.

## Decisiones y aprendizajes reales
- **`celery worker` en vez de `airflow celery worker`:** sintaxis
  actualizada para Airflow 3.
- **`AIRFLOW__CORE__DAGS_FOLDER=/opt/pipelines/dags`** aquí, distinto a
  `/opt/airflow/dags` en Master — funciona porque el compose monta
  `../pipelines:/opt/pipelines` completo (incluye `dags/` adentro), así
  ambas rutas terminan apuntando a los mismos archivos.
- **`CELERY_HOSTNAME` único por instancia:** si dos Workers comparten
  el mismo valor, el monitoreo en Flower queda roto — no se puede
  distinguir qué instancia ejecutó qué tarea.
- **`AIRFLOW__CORE__FERNET_KEY` debe ser idéntica a la de Master** — si
  difiere, este Worker no puede leer las conexiones encriptadas
  (como `my_s3_conn`) y las tareas fallan silenciosamente.

## Notas operativas
- Los DAGs NO viven en esta carpeta. La fuente de verdad es
  `pipelines/dags/` en la raíz del repo, commiteada desde Master. En
  operación real, los DAGs se distribuyen a los Workers vía `scp` desde
  SRV-PROXY después de cada cambio (ver docs/04_OPERACION_Y_ACCESOS.md).
- Worker2 y Worker3 comparten exactamente este mismo `docker-compose.yml`
  y `.env.example` — solo cambia `CELERY_HOSTNAME` en el `.env` real.
