# Kafka — SRV-KAFKA

## Rol en el proyecto
Simula el broker de streaming en tiempo real del proyecto. En el flujo
de producción, Kafka reemplazaría la simulación batch-to-streaming
(DAG S1) con eventos reales llegando de sistemas clínicos. En el lab,
la instancia permanece normalmente apagada — se enciende solo para
demostrar la arquitectura de streaming y validar que el diseño KRaft
funciona, no forma parte de la ejecución rutinaria del pipeline batch.

## Datos técnicos
**IP privada:** 10.0.2.234 · **Modo:** KRaft (sin Zookeeper) ·
**Imagen:** confluentinc/cp-kafka:7.6.0

## Contenedores
| Contenedor | Servicio | Puertos |
|---|---|---|
| kafka-kraft | Broker Kafka (KRaft, broker+controller en un nodo) | 9092 (plaintext), 9093 (controller), 9994 (JMX) |

## Dependencias
- **Depende de:** ninguna — es standalone.
- **Usada por:** en producción, alimentaría el DAG S2 (merge streaming).
  En el lab, no está conectada al pipeline activo — S1 simula su
  función leyendo directo de datos batch.
- **Orden de arranque:** sin restricciones, se enciende solo cuando se
  necesita demostrar el flujo de streaming.

## Configuración
1. `docker compose up -d` — no requiere `.env`, ver nota abajo.
2. Topics usados en las pruebas: `fhir-events`, `medicamentos`,
   `urgencias`, `vitales`.

## Decisiones y aprendizajes reales
- **Sin `.env`:** todos los valores del compose están hardcodeados
  directo (CLUSTER_ID, retención, listeners) porque ninguno es
  sensible — no hay usuario/contraseña en modo PLAINTEXT. No hay nada
  que enmascarar ni parametrizar aquí.
- **Modo PLAINTEXT sin autenticación:** válido para el lab, donde el
  tráfico nunca sale de la red privada. En un entorno de producción
  real, esto requeriría SASL/SSL.
- **Retención de 7 días (168h), segmentos de 1GB:** configurado para
  simular un comportamiento realista sin consumir disco excesivo en
  una instancia pequeña.

## Notas operativas
- `generar_evidencia.sh` genera un snapshot del estado del cluster
  (versión Docker, contenedores activos, topics) — ver ejemplo en
  `evidencia_kafka_2026-06-01_14-16-22.txt`.
- `test_producer.py` / `test_consumer.py` son scripts de validación
  manual con mensajes simulados (pacientes ficticios tipo `PAC-001`,
  sin datos reales).
- `kafka-data/` (volumen de datos runtime) no se sube al repo.
