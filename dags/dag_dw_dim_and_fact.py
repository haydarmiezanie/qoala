import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

# Default arguments for all tasks in the DAG
default_args = {
    "owner": "Haydar",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

# Define DAG for DW data modeling triggered by upstream raw data DAG
with DAG(
    dag_id="dag_dw_dim_and_fact",
    default_args=default_args,
    start_date=datetime.datetime(2025, 6, 15),
    schedule=None,  # Manual trigger or event-based only
    catchup=False,
    description="DAG for running DBT data modeling after raw data ingestion",
    tags=["dbt", "dwh", "modelling"],  # Optional but useful for UI filtering
) as dag:

    # Wait for upstream DAG's final marker task
    upstream_sensor = ExternalTaskSensor(
        task_id="upstream_sensor",
        external_dag_id="dag_datalake_movies_and_series",
        external_task_id="upstream_poking",  # Must match ExternalTaskMarker
        mode="poke",
        timeout=600,        # Max time to wait (in seconds)
        poke_interval=60,   # Check every 60 seconds
    )

    # Run DBT data modeling
    data_modelling = BashOperator(
        task_id="data_modelling",
        bash_command="cd /opt/airflow/dbt && dbt run",
    )

    # Define task dependencies
    upstream_sensor >> data_modelling
