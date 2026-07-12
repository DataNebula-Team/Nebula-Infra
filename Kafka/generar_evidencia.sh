#!/bin/bash
FECHA=$(date '+%Y-%m-%d_%H-%M-%S')
REPORTE="evidencia_kafka_${FECHA}.txt"

echo "Generando reporte de evidencia: $REPORTE"

{
  echo "========================================"
  echo "  EVIDENCIA: SRV-KAFKA KRaft"
  echo "  Fecha: $(date)"
  echo "  Host: $(hostname)"
  echo "  OS: $(lsb_release -d | cut -f2)"
  echo "========================================"
  echo ""
  
  echo "── 1. VERSIÓN DE DOCKER ──────────────────"
  docker --version
  docker compose version
  echo ""
  
  echo "── 2. ESTADO DEL CONTAINER ───────────────"
  docker compose ps
  echo ""
  
  echo "── 3. TOPICS CREADOS ─────────────────────"
  docker exec kafka-kraft kafka-topics \
    --bootstrap-server localhost:9092 --list
  echo ""
  
  echo "── 4. DETALLE DE TOPICS ──────────────────"
  docker exec kafka-kraft kafka-topics \
    --bootstrap-server localhost:9092 --describe
  echo ""
  
  echo "── 5. CONSUMER GROUPS ────────────────────"
  docker exec kafka-kraft kafka-consumer-groups \
    --bootstrap-server localhost:9092 --list
  echo ""
  
  echo "── 6. PUERTOS EN ESCUCHA ─────────────────"
  ss -tlnp | grep -E '9092|9093|9994'
  echo ""
  
  echo "── 7. REGLAS DE FIREWALL (UFW) ───────────"
  sudo ufw status
  echo ""

  echo "── 8. LOGS RECIENTES DEL BROKER ──────────"
  docker compose logs --tail=20 kafka
  echo ""
  
  echo "✅ Evidencia generada exitosamente"
} | tee "$REPORTE"

echo "Archivo guardado: ~/kafka-kraft/$REPORTE"
