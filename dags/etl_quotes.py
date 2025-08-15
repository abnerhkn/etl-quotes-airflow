import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append('/opt/airflow/scripts')

from extract_quotes import coletar_frases
from transform_quotes import limpar_frases
from load_quotes import carregar_frases

with DAG(
    dag_id="etl_quotes",
    description="Pipeline ETL para coletar, transformar e carregar frases no Postgres",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_quotes_task",
        python_callable=coletar_frases
    )

    transform_task = PythonOperator(
        task_id="transform_quotes_task",
        python_callable=limpar_frases
    )

    load_task = PythonOperator(
        task_id="load_quotes_task",
        python_callable=carregar_frases
    )

    extract_task >> transform_task >> load_task
