# RabbitMQ — SRV-RABBIT

## Rol en el proyecto
Es el cartero del pipeline: recibe las tareas que el Scheduler de
Airflow (Master) encola y las reparte entre los 3 Celery Workers
disponibles. Sin RabbitMQ activo, el Scheduler puede seguir programando
DAGs, pero ninguna tarea llega nunca a ejecutarse — el pipeline queda
programado pero congelado.

## Datos técnicos
**IP privada:** 10.0.2.61 · **Imagen:** rabbitmq:3.12-management

## Contenedores
| Contenedor | Servicio | Puertos |
|---|---|---|
| rabbitmq | Broker de mensajería + UI de administración | 5672 (AMQP), 15672 (UI) |

## Dependencias
- **Depende de:** ninguna — es de las primeras en levantarse.
- **Usada por:** Master (Scheduler publica tareas) y los 3 Workers
  (consumen tareas).
- **Orden de arranque:** ANTES de Master y Workers — ambos intentan
  conectarse al broker al iniciar y fallan si no está disponible.

## Configuración
1. `cp .env.example .env` y completar usuario/contraseña.
2. `docker compose up -d`

## Notas operativas
- Las mismas credenciales (`RABBITMQ_DEFAULT_USER/PASS`) deben coincidir
  exactamente con las que usa `AIRFLOW__CELERY__BROKER_URL` en el `.env`
  de Master y Workers — un desajuste aquí rompe la conexión Celery de
  forma silenciosa.
- UI de administración: `nebula-rabbitmq.coderhivex.com`, montada bajo
  `RABBITMQ_MANAGEMENT_PATH_PREFIX=/rabbitmq` en el compose.
- El `.env` real de esta instancia conserva variables heredadas del
  `.env` compartido original de Nebula (Airflow, Postgres, Spark, AWS)
  que el `docker-compose.yml` de RabbitMQ no usa — solo son ruido, no
  configuración activa. El `.env.example` refleja únicamente lo que
  este servicio necesita.
