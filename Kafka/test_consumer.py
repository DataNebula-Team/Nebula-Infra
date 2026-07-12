"""
test_consumer.py
Lee mensajes de los 4 topics y los imprime en consola.
Se detiene automáticamente después de 30 segundos.
Compatible con Python 3.12+ y kafka-python-ng
"""
import json
from kafka import KafkaConsumer

TOPICS = ['urgencias', 'vitales', 'medicamentos', 'fhir-events']
TIMEOUT_SEGUNDOS = 30

print(f"🎧 Consumer iniciado. Escuchando topics: {', '.join(TOPICS)}")
print(f"⏱  Se detendrá en {TIMEOUT_SEGUNDOS} segundos...")
print("=" * 60)

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=['10.0.2.234:9092'],
    auto_offset_reset='earliest',
    group_id='test-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    api_version=(3, 4, 0),             # Forzar versión de API evita bloqueos en el handshake
    request_timeout_ms=15000,          # 15 segundos máximo por petición
    session_timeout_ms=10000,          # Sesión estándar
    metadata_max_age_ms=5000,          # No esperar demasiado por metadatos
    consumer_timeout_ms=TIMEOUT_SEGUNDOS * 1000
)


mensajes_recibidos = 0

try:
    for mensaje in consumer:
        mensajes_recibidos += 1
        print(f"\n📥 MENSAJE #{mensajes_recibidos}")
        print(f"   Topic     : {mensaje.topic}")
        print(f"   Partición : {mensaje.partition}")
        print(f"   Offset    : {mensaje.offset}")
        print(f"   Key       : {mensaje.key}")
        print(f"   Payload   : {json.dumps(mensaje.value, indent=6, ensure_ascii=False)}")
        print("-" * 60)

except Exception as e:
    print(f"\n⏰ Timeout alcanzado o error: {e}")
finally:
    consumer.close()
    print(f"\n✅ Consumer finalizado. Total mensajes recibidos: {mensajes_recibidos}")

    if mensajes_recibidos == 0:
        print("⚠️  No se recibieron mensajes. Verifica que el producer se ejecutó antes.")
    elif mensajes_recibidos == 4:
        print("🎉 ¡Validación exitosa! Los 4 topics funcionan correctamente.")
    else:
        print(f"📊 Se recibieron {mensajes_recibidos} mensajes de 4 esperados.")
