from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="example_world",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "celery-check"],
)
def example_dag():
    """
    DAG de verificación del CeleryExecutor.
    Triggéalo manualmente desde la UI y confirma en Flower
    que los workers reciben y ejecutan las tareas.
    """

    @task(queue="default")
    def hello():
        import socket
        hostname = socket.gethostname()
        print(f"✅ Tarea 'hello' ejecutada en worker: {hostname}")
        return {"status": "ok", "worker": hostname}

    @task(queue="default")
    def world(info: dict):
        print(f"✅ Tarea 'world' recibió resultado del worker: {info['worker']}")
        return "Pipeline completado exitosamente"

    @task(queue="default")
    def spark_queue_check():
        """Verifica que la cola 'default' también tenga un worker escuchando."""
        import socket
        hostname = socket.gethostname()
        print(f"✅ Cola 'default' respondió desde: {hostname}")
        return hostname

    result = hello()
    world(result)
    spark_queue_check()


dag_instance = example_dag()
