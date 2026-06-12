from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "nebula",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def verificar_conexiones():
    import socket
    servicios = {
        "Postgres": ("27.0.x.xx", 5432),
        "RabbitMQ": ("27.0.x.xx", 5672),
    }
    for nombre, (host, puerto) in servicios.items():
        try:
            sock = socket.create_connection((host, puerto), timeout=5)
            sock.close()
            print(f"OK — {nombre} ({host}:{puerto})")
        except Exception as e:
            raise ValueError(f"FAIL — {nombre}: {e}")

def imprimir_worker(**context):
    import socket
    print(f"Tarea ejecutada en worker: {socket.gethostname()}")
    print(f"Run ID: {context['run_id']}")
    print(f"Fecha de ejecución: {context['ds']}")
    return "ok"

with DAG(
    dag_id="nebula_health_check",
    description="DAG de prueba — verifica conectividad de la infraestructura",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["nebula", "health"],
) as dag:

    verificar = PythonOperator(
        task_id="verificar_conexiones",
        python_callable=verificar_conexiones,
        queue="default",
    )

    worker_info = PythonOperator(
        task_id="info_del_worker",
        python_callable=imprimir_worker,
        provide_context=True,
        queue="default",
    )

    finalizar = BashOperator(
        task_id="finalizar",
        bash_command='echo "Pipeline completado: $(date) en $(hostname)"',
        queue="default",
    )

    verificar >> worker_info >> finalizar
    