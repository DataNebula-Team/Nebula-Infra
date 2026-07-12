# Proxy-Nginx — SRV-PROXY

## Rol en el proyecto
Es el único punto de entrada público del cluster Data_Nebula y el jump
host SSH hacia todas las instancias privadas. Expone Nginx Proxy
Manager, que enruta cada subdominio de `coderhivex.com` hacia la IP
privada y puerto interno del servicio correspondiente (Airflow,
Superset, Grafana, RabbitMQ). Sin esta instancia, nadie puede acceder
a ninguna interfaz web del proyecto ni conectarse por SSH a las
instancias privadas.

## Datos técnicos
**IP:** 10.0.2.233 (privada) / 34.225.146.184 (pública) ·
**Imagen:** jc21/nginx-proxy-manager:latest

## Contenedores
| Contenedor | Servicio | Puertos |
|---|---|---|
| app | Nginx Proxy Manager | 80 (HTTP), 443 (HTTPS), 81 (UI admin) |

## Dependencias
- **Depende de:** ninguna — es el punto de entrada, no depende de que
  otros servicios estén arriba para levantarse.
- **Usada por:** todo el mundo — cualquier acceso externo a Airflow,
  Superset, Grafana o RabbitMQ pasa por aquí.
- **Orden de arranque:** primero, sin dependencias.

## Configuración
1. `docker compose up -d` — no requiere `.env`, ver nota abajo.
2. La configuración de proxies, dominios y certificados se administra
   desde la UI web en el puerto 81, no desde archivos de configuración
   en el repo.

## Decisiones y aprendizajes reales
- **Sin `.env` real:** el `docker-compose.yml` no declara `env_file`
  ni lee ninguna variable de entorno — toda la configuración vive en
  el volumen `./data`, gestionada desde la UI. El `.env` que existía
  en esta instancia era una copia heredada del `.env` compartido de
  Nebula (Airflow, Postgres, Spark, e incluso una AWS key real), sin
  ninguna relación con este servicio — se eliminó por no aportar nada
  y representar riesgo innecesario de tener credenciales sueltas.
- Las llaves SSH para saltar a otras instancias (`KEY-MASTER-NEBULA-2.pem`,
  `KEY-WORKERS.pem`) viven en `~/.ssh/` de esta instancia, fuera del repo.

## Notas operativas
- `./data/` y `./letsencrypt/` contienen configuración runtime y
  certificados SSL reales — protegidos en `.gitignore`, nunca se suben.
- Enruta cada subdominio (`nebula-airflow`, `nebula-superset`,
  `nebula-grafana`, `nebula-rabbitmq`) hacia la IP privada y puerto
  correspondiente de cada instancia.
