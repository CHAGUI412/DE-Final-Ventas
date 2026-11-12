from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from runtime import run_dbt

with DAG(
    dag_id="examen_ventas",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 0},
    tags=["examen"],
) as dag:
    # revisa que raw.ventas no este vieja antes de construir el resumen
    # warning: dbt sale con codigo 0 y la tarea queda en success
    # error: dbt sale con codigo distinto de 0, run_dbt lanza la excepcion y la tarea falla
    freshness = PythonOperator(
        task_id="freshness",
        python_callable=run_dbt,
        op_args=[["source", "freshness"]],
    )

    construir_resumen = PythonOperator(
        task_id="construir_resumen",
        python_callable=run_dbt,
        op_args=[["build"]],
    )

    # la regla de ejecucion queda en all_success (la de por defecto),
    # asi que si freshness falla, construir_resumen queda en upstream_failed
    freshness >> construir_resumen
