from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.source_to_landing_folder import main


with DAG(
    dag_id="daily_source_to_landing",
    start_date=datetime(2026, 8, 21),
    schedule="@daily",
    catchup=False,
    tags=["source-to-landing"],
) as dag:

    extract_and_land = PythonOperator(
        task_id="extract_and_land",
        python_callable=main
    )

    