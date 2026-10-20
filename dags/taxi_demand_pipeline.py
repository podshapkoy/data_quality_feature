import os
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_ingestion():
    from taxi_pipeline.ingest import ingest_trips
    ingest_trips(os.getenv("TLC_CSV_PATH", "/app/data/raw/yellow_tripdata_2024-01.parquet"))


def run_weather():
    from taxi_pipeline.ingest import ingest_weather_for_trips
    path = os.getenv("TLC_CSV_PATH", "/app/data/raw/yellow_tripdata_2024-01.parquet")
    ingest_weather_for_trips(path)


def run_quality(**context):
    from taxi_pipeline.transform import validate_and_clean
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, context["dag_run"].run_id))
    validate_and_clean(run_id)
    return run_id


def run_features():
    from taxi_pipeline.transform import build_features
    build_features()


def run_result_checks(**context):
    from taxi_pipeline.transform import check_features
    run_id = context["ti"].xcom_pull(task_ids="validate_and_clean")
    check_features(run_id)


with DAG(
    dag_id="taxi_demand_pipeline",
    description="Ingest, validate, clean and feature taxi demand data",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["data-quality", "taxi", "features"],
) as dag:
    ingest = PythonOperator(task_id="load_trip_csv", python_callable=run_ingestion)
    weather = PythonOperator(
        task_id="load_weather", python_callable=run_weather,
        retries=2, retry_delay=timedelta(minutes=2),
    )
    quality = PythonOperator(task_id="validate_and_clean", python_callable=run_quality)
    features = PythonOperator(task_id="calculate_features", python_callable=run_features)
    checks = PythonOperator(task_id="check_feature_mart", python_callable=run_result_checks)
    ingest >> weather >> quality >> features >> checks
