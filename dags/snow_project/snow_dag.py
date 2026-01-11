from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SCRIPTS_PATH = "/opt/airflow/dags/snow_project/scripts"

with DAG(
    dag_id="snow_resort_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
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

    scrape_resorts >> clean_resort_data >> geocode_resorts >> fetch_weather
