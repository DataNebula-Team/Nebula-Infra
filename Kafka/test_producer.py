"""
test_producer.py
Simula mensajes enviados por microservicios hospitalarios a Kafka.
Compatible con Python 3.12+ y kafka-python-ng
"""
import json
import datetime
from kafka import KafkaProducer

# Conectar al broker Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

print("✅ Producer conectado a Kafka")
print("=" * 50)

# ── Topic: urgencias ─────────────────────────────
mensaje_urgencia = {
    "paciente_id": "PAC-001",
    "tipo": "CODIGO_ROJO",
    "descripcion": "Paro cardíaco - UCI cama 3",
    "nivel_prioridad": 1,
    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
}
producer.send('urgencias', key='PAC-001', value=mensaje_urgencia)
print(f"📤 [urgencias]    → {mensaje_urgencia['tipo']}: {mensaje_urgencia['descripcion']}")

# ── Topic: vitales ────────────────────────────────
mensaje_vitales = {
    "paciente_id": "PAC-002",
    "frecuencia_cardiaca": 78,
    "presion_arterial": "120/80",
    "temperatura": 36.5,
    "saturacion_oxigeno": 98,
    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
}
producer.send('vitales', key='PAC-002', value=mensaje_vitales)
print(f"📤 [vitales]      → FC: {mensaje_vitales['frecuencia_cardiaca']} bpm | SpO2: {mensaje_vitales['saturacion_oxigeno']}%")

# ── Topic: medicamentos ───────────────────────────
mensaje_medicamento = {
    "paciente_id": "PAC-003",
    "medicamento": "Amoxicilina",
    "dosis_mg": 500,
    "via": "oral",
    "hora_administracion": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "enfermera_id": "ENF-042"
}
producer.send('medicamentos', key='PAC-003', value=mensaje_medicamento)
print(f"📤 [medicamentos] → {mensaje_medicamento['medicamento']} {mensaje_medicamento['dosis_mg']}mg para {mensaje_medicamento['paciente_id']}")

# ── Topic: fhir-events ────────────────────────────
mensaje_fhir = {
    "resourceType": "Observation",
    "id": "obs-78923",
    "status": "final",
    "subject": {"reference": "Patient/PAC-004"},
    "code": {"text": "Hemoglobina"},
    "valueQuantity": {"value": 13.5, "unit": "g/dL"},
    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
}
producer.send('fhir-events', key='obs-78923', value=mensaje_fhir)
print(f"📤 [fhir-events]  → {mensaje_fhir['resourceType']}/{mensaje_fhir['id']}: {mensaje_fhir['code']['text']}")

# Asegurar que todos los mensajes se envíen antes de cerrar
producer.flush()
producer.close()

print("=" * 50)
print("✅ Todos los mensajes enviados exitosamente")
