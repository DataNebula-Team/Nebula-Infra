# DOC 4 — Guía de Variables de Entorno

> **Proyecto:** NEBULA  
> **Archivo:** `.env` (uno por instancia, dentro de su carpeta correspondiente en el repo)  
> **Estado:** ⚠️ Nunca debe subirse a GitHub

---

## Tabla de Contenidos

1. [¿Qué es un `.env` y por qué no va en Git?](#1-qué-es-un-env-y-por-qué-no-va-en-git)
2. [Variables críticas — explicación detallada](#2-variables-críticas--explicación-detallada)
3. [Referencia completa de variables](#3-referencia-completa-de-variables)
4. [Variables compartidas vs específicas por instancia](#4-variables-compartidas-vs-específicas-por-instancia)
5. [Dependencias entre variables](#5-dependencias-entre-variables)

---

## 1. ¿Qué es un `.env` y por qué no va en Git?

Un archivo `.env` contiene **variables de entorno**: pares `NOMBRE=valor` que los servicios leen al arrancar para saber cómo configurarse. En lugar de escribir contraseñas, IPs y tokens directamente en el código o en `docker-compose.yml`, los centralizamos aquí.

**¿Por qué no va en Git?** Contiene credenciales sensibles (contraseñas, claves de AWS, tokens de Airflow). Si llega a GitHub queda expuesto. Por eso está en `.gitignore`. Los nuevos integrantes lo reciben por un canal seguro y lo crean manualmente en cada servidor con `nano .env`.

**¿Cómo lo lee Docker Compose?**
```yaml
env_file:
  - .env
```
Todas las variables del `.env` quedan disponibles dentro del contenedor. Las referencias `${VARIABLE}` en el compose se reemplazan automáticamente.

El archivo es **casi idéntico** entre Master y Workers. Solo cambian 4 variables (ver [Sección 4](#4-variables-compartidas-vs-específicas-por-instancia)) y la sección `INIT`, que **únicamente existe en el Master**.

---

## 2. Variables Críticas — Explicación Detallada

Estas 7 variables tienen comportamientos no obvios, son fuente frecuente de errores o requieren coordinación entre instancias.

---

### `CELERY_HOSTNAME`

Nombre con el que cada instancia se registra en Celery. Airflow y Flower lo usan para identificar qué Worker ejecutó qué tarea. **Si dos instancias tienen el mismo valor, el monitoreo queda roto.**

```env
# Master:   CELERY_HOSTNAME=master
# Worker1:  CELERY_HOSTNAME=worker1
# Worker2:  CELERY_HOSTNAME=worker2
# Worker3:  CELERY_HOSTNAME=worker3
```

---

### `AIRFLOW__CORE__FERNET_KEY`

Clave criptográfica que Airflow usa para encriptar las contraseñas guardadas en Admin → Connections. **Debe ser exactamente la misma en Master y los 3 Workers.** Si difiere, los Workers no podrán leer las conexiones encriptadas y las tareas fallarán silenciosamente.

```env
AIRFLOW__CORE__FERNET_KEY=0K7wPg0qq-BlfjRTK1coSRSCEvaYgsOseevp4l0IMyo=
```

---

### `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`

Cadena de conexión completa a PostgreSQL. Es la variable más importante para que Airflow funcione. Si la IP de SRV-MASTER cambia, esta cadena debe actualizarse en **todas** las instancias.

```
Formato: postgresql+psycopg2://[usuario]:[contraseña]@[host]:[puerto]/[db]
Valor:   postgresql+psycopg2://coder-ra-c6:Riwi2026**@10.0.2.54:5432/nebula_db
```

---

### `AIRFLOW__CELERY__BROKER_URL`

URL de conexión a RabbitMQ. El Scheduler la usa para publicar tareas; los Workers para consumirlas. Si las credenciales de RabbitMQ o la IP de SRV-RABBIT cambian, actualizar aquí.

```
Formato: amqp://[usuario]:[contraseña]@[host]:[puerto]/[vhost]
Valor:   amqp://admin:admin123@10.0.2.61:5672//
```

> El `//` al final es el vhost raíz de RabbitMQ. No es un typo.

---

### `AIRFLOW__CELERY__RESULT_BACKEND`

Donde Celery guarda los resultados de cada tarea (éxito/fallo) para que Airflow actualice la UI. **Apunta a PostgreSQL en SRV-MASTER (`10.0.2.54`), NO a RabbitMQ (`10.0.2.61`).** Confundir estas dos IPs fue el **error #1** del diagnóstico del proyecto.

```env
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://coder-ra-c6:Riwi2026**@10.0.2.54:5432/nebula_db
```

---

### `SPARK_DRIVER_HOST`

IP del servidor que actúa como **Driver** de Spark: quien lanza los jobs y recibe los resultados. En NEBULA los jobs los lanza Airflow desde SRV-MASTER, por lo tanto esta variable apunta a `10.0.2.54`. Fue el **error #2** del diagnóstico (tenía un typo: `I25.0.2.90`).

> Si en el futuro los jobs se lanzan desde la VPC externa (`25.0.2.0/24`), cambiar a la IP del master de ese clúster.

---

### `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY`

Los Workers (SRV-WORKER1/2/3) son instancias `t3.small` (2 vCPU / 2 GB RAM) que corren simultáneamente un **Celery Worker** y un **Spark Worker**. Los recursos se dividen a la mitad para que ambos convivan:

| Instancia | Cores Spark | Memoria Spark |
|---|---|---|
| SRV-MASTER / SRV-SPARK | `2` | `2g` |
| SRV-WORKER1/2/3 | `1` | `1g` |

---

## 3. Referencia Completa de Variables

### Identificación de instancia

| Variable | Valor | Instancias | Nota |
|---|---|---|---|
| `AIRFLOW_UID` | `50000` | Master, Workers | UID estándar de Airflow en contenedor |
| `CELERY_HOSTNAME` | `master` / `workerN` | Master, Workers | ⚠️ Único por instancia — ver sección 2 |
| `QUEUES` | `default` | Master, Workers | Cola de tareas que escucha el Worker |
| `PYTHONPATH` | `/opt/pipelines` | Master, Workers | Ruta montada como volumen desde `pipelines/` |

### Airflow Core

| Variable | Valor | Instancias | Nota |
|---|---|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `CeleryExecutor` | Master, Workers | Distribuye tareas a Workers via RabbitMQ |
| `AIRFLOW__CORE__FERNET_KEY` | `0K7wPg0qq-...` | Master, Workers | ⚠️ Idéntica en todas — ver sección 2 |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `true` | Master, Workers | Los DAGs nuevos arrancan pausados |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | Master, Workers | No cargar DAGs de ejemplo de Airflow |
| `AIRFLOW__CORE__DAGS_FOLDER` | `/opt/pipelines/dag` | Master, Workers | Carpeta de DAGs dentro del contenedor |

### PostgreSQL

| Variable | Valor | Instancias | Nota |
|---|---|---|---|
| `POSTGRES_USER` | `coder-ra-c6` | Master, Workers | Usuario de la BD |
| `POSTGRES_PASSWORD` | `Riwi2026**` | Master, Workers | Contraseña de la BD |
| `POSTGRES_DB` | `nebula_db` | Master, Workers | Nombre de la BD |
| `POSTGRES_HOST` | `10.0.2.54` | Master, Workers | IP de SRV-MASTER |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://...@10.0.2.54:5432/nebula_db` | Master, Workers | ⚠️ Ver sección 2 |

### RabbitMQ

| Variable | Valor | Instancias | Nota |
|---|---|---|---|
| `RABBITMQ_DEFAULT_USER` | `admin` | Master, Workers, RabbitMQ | Mismo valor en todas las instancias |
| `RABBITMQ_DEFAULT_PASS` | `admin123` | Master, Workers, RabbitMQ | Mismo valor en todas las instancias |

### Celery

| Variable | Valor | Instancias | Nota |
|---|---|---|---|
| `AIRFLOW__CELERY__BROKER_URL` | `amqp://admin:admin123@10.0.2.61:5672//` | Master, Workers | ⚠️ Ver sección 2 |
| `AIRFLOW__CELERY__RESULT_BACKEND` | `db+postgresql://...@10.0.2.54:5432/nebula_db` | Master, Workers | ⚠️ Ver sección 2 |

### API Server y JWT

| Variable | Valor | Instancias | Nota |
|---|---|---|---|
| `AIRFLOW__WEBSERVER__BASE_URL` | `https://nebula-airflow.coderhivex.com` | Master, Workers | URL pública de Airflow |
| `AIRFLOW__API__INSTANCE_NAME` | `Airflow Data Platform` | Master, Workers | Nombre en UI y API |
| `AIRFLOW__API__SECRET_KEY` | `278de5c8...` | Master, Workers | Clave Flask, idéntica en todas |
| `AIRFLOW__API_AUTH__JWT_SECRET` | `f3d4a5e9...` | Master, Workers | Clave JWT, idéntica en todas |
| `AIRFLOW__API_AUTH__JWT_ISSUER` | `https://nebula-airflow.coderhivex.com/execution` | Master, Workers | Emisor de tokens JWT |

### Scheduler

| Variable | Valor | Instancias | Nota |
|---|---|---|---|
| `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK` | `true` | Master, Workers | Activa endpoint `/health` |
| `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL` | `300` | Master, Workers | Segundos entre escaneos de la carpeta DAGs |

### Init — solo Master

| Variable | Valor | Nota |
|---|---|---|
| `_AIRFLOW_DB_MIGRATE` | `true` | Crea tablas en PostgreSQL al primer arranque |
| `_AIRFLOW_WWW_USER_CREATE` | `true` | Crea el usuario admin al primer arranque |
| `_AIRFLOW_WWW_USER_USERNAME` | `admin` | Usuario de acceso a Airflow UI |
| `_AIRFLOW_WWW_USER_PASSWORD` | `admin` | ⚠️ Pendiente cambiar a contraseña segura |

> Esta sección solo se ejecuta en el primer `docker compose up -d`. En levantamientos posteriores `airflow-init` detecta que el usuario ya existe y no lo recrea.

### Logging y S3

| Variable | Valor | Instancias | Nota |
|---|---|---|---|
| `AIRFLOW__LOGGING__REMOTE_LOGGING` | `true` | Master, Workers | Guarda logs en S3 |
| `AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER` | `s3://nebula2-airflow-logs/logs` | Master, Workers | Ruta del bucket |
| `AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID` | `my_s3_conn` | Master, Workers | Conexión configurada en Admin → Connections |
| `AIRFLOW__LOGGING__ENCRYPT_S3_LOGS` | `false` | Master, Workers | Logs en texto plano en S3 |
| `AWS_ACCESS_KEY_ID` | `AKIA3XG5NKATWMALCF7I` | Master, Workers | ⚠️ Pendiente reemplazar por IAM Role |
| `AWS_SECRET_ACCESS_KEY` | `c2UQZkV7HJ...` | Master, Workers | ⚠️ Pendiente reemplazar por IAM Role |
| `AWS_DEFAULT_REGION` | `us-east-1` | Master, Workers | Región del bucket S3 |

### Spark

| Variable | Valor | Instancias | Nota |
|---|---|---|---|
| `SPARK_MODE` | `master` / `worker` | Master, Workers | Rol en el clúster Spark |
| `SPARK_MASTER_HOST` | `10.0.2.103` | Master, Workers, Spark | IP de SRV-SPARK |
| `SPARK_MASTER_PORT` | `7077` | Master, Workers, Spark | Puerto API del Spark Master |
| `SPARK_MASTER_WEBUI_PORT` | `8090` | Master, Workers, Spark | Puerto UI del Spark Master |
| `SPARK_WORKER_CORES` | `2` / `1` | Master, Workers, Spark | ⚠️ Ver sección 2 |
| `SPARK_WORKER_MEMORY` | `2g` / `1g` | Master, Workers, Spark | ⚠️ Ver sección 2 |
| `SPARK_WORKER_PORT` | `7000` | Master, Workers, Spark | Puerto comunicación interna Spark |
| `SPARK_WORKER_WEBUI_PORT` | `8091` | Master, Workers, Spark | Puerto UI del Spark Worker |
| `SPARK_DRIVER_HOST` | `10.0.2.54` | Master, Workers | ⚠️ Ver sección 2 |

---

## 4. Variables Compartidas vs Específicas por Instancia

![Dependencias entre variables de entorno](../assets/gv09_variables_dependencias.svg)

### Variables que difieren por instancia

Estas son las **únicas** variables que cambian entre instancias. Revisar antes de cada `docker compose up -d`:

| Variable | Master | Worker1 | Worker2 | Worker3 |
|---|---|---|---|---|
| `CELERY_HOSTNAME` | `master` | `worker1` | `worker2` | `worker3` |
| `SPARK_MODE` | `master` | `worker` | `worker` | `worker` |
| `SPARK_WORKER_CORES` | `2` | `1` | `1` | `1` |
| `SPARK_WORKER_MEMORY` | `2g` | `1g` | `1g` | `1g` |

### Variables exclusivas del Master

Solo deben estar en el `.env` del Master, **no en los Workers**:

`_AIRFLOW_DB_MIGRATE` · `_AIRFLOW_WWW_USER_CREATE` · `_AIRFLOW_WWW_USER_USERNAME` · `_AIRFLOW_WWW_USER_PASSWORD`

---

## 5. Dependencias entre Variables

Si cambia una IP o credencial, estas son todas las variables que hay que actualizar en cascada:

**Si cambia la IP de SRV-MASTER (`10.0.2.54`):**
```
→ POSTGRES_HOST
→ AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
→ AIRFLOW__CELERY__RESULT_BACKEND
→ SPARK_DRIVER_HOST
```

**Si cambia la IP de SRV-RABBIT (`10.0.2.61`):**
```
→ AIRFLOW__CELERY__BROKER_URL
```

**Si cambia la IP de SRV-SPARK (`10.0.2.103`):**
```
→ SPARK_MASTER_HOST
```

**Si cambian las credenciales de PostgreSQL:**
```
→ POSTGRES_USER / POSTGRES_PASSWORD
→ AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
→ AIRFLOW__CELERY__RESULT_BACKEND
```

**Si cambian las credenciales de RabbitMQ:**
```
→ RABBITMQ_DEFAULT_USER / RABBITMQ_DEFAULT_PASS
→ AIRFLOW__CELERY__BROKER_URL
```

> Todos los cambios aplican en Master **y** en los 3 Workers — el `.env` es compartido.

---

*Este documento es parte de la documentación oficial del proyecto NEBULA. Para el contexto general, ver **DOC1**. Para la infraestructura AWS, ver **DOC2**. Para el flujo operativo, ver **DOC3**.*
