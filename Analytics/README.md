# Analytics — SRV-ANALYTICS

## Rol en el proyecto
Es el destino final del pipeline batch: aquí llegan los datos ya
procesados y agregados (los 4 marts con 17 KPIs) para consulta
analítica. Corre PostgreSQL 5433, la base de datos analítica, y
Superset, la herramienta de visualización que usa el equipo clínico
para explorar los dashboards. Sin esta instancia, el pipeline puede
correr de punta a punta, pero nadie puede consultar los resultados.

## Datos técnicos
**IP privada:** 10.0.2.10 · **Imágenes:** postgres:16, apache/superset:latest

## Contenedores
| Contenedor | Servicio | Puerto |
|---|---|---|
| analytics-postgres | PostgreSQL 5433 (BD analítica) | 5433 → 5432 |
| analytics-superset | Apache Superset | 8088 |

## Dependencias
- **Depende de:** ninguna instancia externa para levantarse (Superset
  depende internamente de su propio Postgres, vía `depends_on`).
- **Usada por:** el DAG B6 (`dag_load_analytics`) carga los marts aquí
  al final de cada corrida batch; Superset lee de este Postgres para
  construir los dashboards.
- **Orden de arranque:** sin restricciones — puede levantarse en
  cualquier momento, no bloquea al resto del pipeline.

## Configuración
1. `cp .env.example .env` y completar valores.
2. `docker compose up -d`

## Cómo fluye la información aquí
1. El DAG B6 en Master ejecuta la carga final de los 4 marts
   (`mart_clinico`, `mart_farmacia`, `mart_financiero`,
   `mart_operacional`) desde S3 hacia este PostgreSQL.
2. Superset se conecta a este Postgres como fuente de datos y expone
   los dashboards al equipo clínico.

## Decisiones y aprendizajes reales
- Este PostgreSQL es independiente del operacional de Master — puertos
  distintos (5433 vs 5432) y propósitos distintos: aquí llegan datos ya
  procesados para consulta, no el estado interno de Airflow.
- El `.env` de esta instancia no arrastra variables heredadas de otras
  partes del proyecto — contiene únicamente las 4 variables que el
  `docker-compose.yml` realmente usa.

## Notas operativas
- Login por defecto de Superset: admin/admin — cambiar en un entorno
  real, aquí se deja así por ser laboratorio.
- UI disponible en `nebula-superset.coderhivex.com`.
