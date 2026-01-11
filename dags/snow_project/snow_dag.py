from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

SCRIPTS_PATH = "/opt/airflow/dags/snow_project/scripts"
CSV_PATH = "/opt/airflow/dags/snow_project/data/ski_resorts_weather.csv"


GCS_BUCKET = "snow-resort-data-joana"
GCS_OBJECT = "ski_data/ski_resorts.csv"

BQ_TABLE = "project-988eacf9-e36e-4a78-91b.snow_resort_data.ski_resorts_final"

with DAG(
    dag_id="snow_resort_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *"
    catchup=False,
    description="Scrape, clean, geocode ski resorts and fetch weather data",
) as dag:

    scrape_resorts = BashOperator(
        task_id="scrape_resorts",
        bash_command=f"python {SCRIPTS_PATH}/scrape_resorts.py",
    )

    clean_resort_data = BashOperator(
        task_id="clean_resort_data",
        bash_command=f"python {SCRIPTS_PATH}/clean_data.py",
    )

    geocode_resorts = BashOperator(
        task_id="geocode_resorts",
        bash_command=f"python {SCRIPTS_PATH}/geocode_resorts.py",
    )

    fetch_weather = BashOperator(
        task_id="fetch_weather",
        bash_command=f"python {SCRIPTS_PATH}/fetch_weather.py",
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src=CSV_PATH,
        dst=GCS_OBJECT,
        bucket=GCS_BUCKET,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJECT],
        destination_project_dataset_table=BQ_TABLE,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    scrape_resorts >> clean_resort_data >> geocode_resorts >> fetch_weather >> upload_to_gcs >> load_to_bigquery
