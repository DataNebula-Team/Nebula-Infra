# Monitoring — SRV-MONITORING

## Rol en el proyecto
Es la capa de observabilidad del cluster. Prometheus recolecta métricas
de infraestructura (contenedores, recursos) y Grafana las visualiza en
dashboards. Es la instancia que responde "¿está sano el cluster ahora
mismo?" — sin ella, el pipeline sigue funcionando, pero nadie puede ver
métricas de infraestructura ni recibir alertas visuales de problemas.

## Datos técnicos
**IP privada:** 10.0.2.5 · **Imágenes:** prom/prometheus:v2.52.0,
grafana/grafana:10.4.2

## Contenedores
| Contenedor | Servicio | Puerto |
|---|---|---|
| prometheus | Recolección de métricas (retención 30 días) | 9090 |
| grafana | Visualización y dashboards | 3000 |

## Dependencias
- **Depende de:** ninguna instancia externa para levantarse (Grafana
  depende internamente de Prometheus, vía `depends_on`).
- **Usada por:** nadie depende de esta instancia para funcionar — es
  observabilidad, no una pieza del flujo de datos.
- **Orden de arranque:** sin restricciones.

## Configuración
1. `cp .env.example .env` y completar la contraseña de admin de Grafana.
2. `docker compose up -d`

## Decisiones y aprendizajes reales
- **Contraseña de Grafana como variable, no hardcodeada:** el
  `docker-compose.yml` originalmente traía
  `GF_SECURITY_ADMIN_PASSWORD=admin` escrito directo en texto plano.
  Se corrigió a `${GF_ADMIN_PASSWORD}` leyendo del `.env` — buena
  práctica incluso en un lab, para no normalizar credenciales
  hardcodeadas en archivos versionados.
- **`GF_SERVER_ROOT_URL` apunta a `nebula-superset.coderhivex.com`:**
  no es un error — Grafana corre con
  `GF_SERVER_SERVE_FROM_SUB_PATH=true`, montado como subpath detrás de
  Nginx Proxy Manager, que resuelve el enrutamiento real de IP y puerto
  interno hacia esta instancia.
- Dashboards y datasources se provisionan automáticamente al arrancar,
  sin configuración manual en la UI (`grafana/provisioning/`).

## Notas operativas
- Dashboard principal: `grafana/dashboards/data_nebula_infra.json`.
- Configuración de scraping de Prometheus en
  `prometheus/prometheus.yml`.
- UI de Grafana accesible vía Nginx Proxy Manager (ver Proxy-Nginx).
