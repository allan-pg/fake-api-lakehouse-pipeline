from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

from pipeline.source_to_landing_folder import (
    create_s3_bucket,
    extract_and_land,
    save_all_watermarks
)

from utils.config import (
    API_URL,
    ENDPOINTS,
    BUCKETS,
    LOOKBACK_HOURS
)


with DAG(
    dag_id="source_to_landing",
    start_date=datetime(2026, 8, 26),
    schedule="@daily",
    catchup=False,
    tags=["source", "s3", "incremental"]
) as dag:

    create_bucket_task = PythonOperator(
        task_id="create_s3_bucket",
        python_callable=create_s3_bucket
    )

    extract_task = PythonOperator(
        task_id="extract_and_land",
        python_callable=extract_and_land,
        op_kwargs={
            "url": API_URL,
            "endpoints": ENDPOINTS,
            "bucket_name": BUCKETS[0],
            "lookback_hours": LOOKBACK_HOURS
        }
    )

    save_watermarks_task = PythonOperator(
        task_id="save_all_watermarks",
        python_callable=save_all_watermarks
    )

    create_bucket_task >> extract_task >> save_watermarks_task