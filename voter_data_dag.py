"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import datetime
from google.cloud.orchestration.airflow import service_v1, DAG

# pylint: disable=g-import-not-at-top
try:
  from google.cloud.orchestration.airflow import BashOperator
except ImportError:
  from google.cloud.orchestration.airflow import BashOperator
# pylint: enable=g-import-not-at-top

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 5),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'voter_data',
    default_args=default_args,
    description='Runs an ETL script',
    max_active_runs=2,
    schedule_interval='@daily',
    catchup=False,
)

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/voter_data_etl.py',
    )

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id='echo',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
    do_xcom_push=False)

start_pipeline = service_v1.ExecuteAirflowCommand(
  location="us-central1",
  pipeline_name="etl-pipeline",
  instance_name="datafusion-dev",
  task_id="start_datafusion_pipeline",
)

run_script_task >> start_pipeline