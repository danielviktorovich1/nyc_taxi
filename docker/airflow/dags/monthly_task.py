from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess


def run_script():
    subprocess.run(["python", "/opt/airflow/dags/PIPELINE.py"], check=True)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "monthly_script",
    default_args=default_args,
    schedule="25 17 1 * *", # 20 25 every month (+3 UTC Moscow time): 25 - minutes 17 - hours 1 - day number * - every month * - every year
    catchup=False, # the code will not catch the missed dates (since start date) up
) as dag:

    task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_script,
    )

    task
