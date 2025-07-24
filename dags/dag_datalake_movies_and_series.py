import datetime
from airflow.sdk import DAG
from airflow.providers.standard.sensors.external_task import ExternalTaskMarker
from custom_operator.ingestion_operator import IngestionOperator
from custom_operator.loader_operator import LoaderOperator

# Default arguments for all tasks in the DAG
default_args = {
    "owner": "Haydar",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id="dag_datalake_movies_and_series",
    default_args=default_args,
    start_date=datetime.datetime(2025, 6, 15),
    schedule=None,  # DAG will only run manually unless overridden
    catchup=False,
    description="DAG for ingesting and loading TMDB movies and series data into DuckDB",
    tags=["datalake", "tmdb", "kaggle"],  # Optional: tags for UI filtering
) as dag:
    # Task 1: Ingest dataset from Kaggle using IngestionOperator
    data_ingestion = IngestionOperator(
        task_id="ingest_movies_and_series",
        dataset="edgartanaka1/tmdb-movies-and-series"
    )

    # Task 2: Load data into DuckDB for both "movies" and "series" tables
    loader_tasks = []
    for table in ["movies", "series"]:
        loader_task = LoaderOperator(
            task_id=f"load_{table}",
            table=table,
            max_files=3000,
            duckdb_path="tmdb_movies_and_series.duckdb"
        )
        loader_tasks.append(loader_task)

    # Task 3: Marker for downstream dependency in another DAG
    downstream_poking = ExternalTaskMarker(
        task_id="upstream_poking",
        external_dag_id="dag_dw_dim_and_fact",
        external_task_id="data_modelling"
    )

    # Define task dependencies
    data_ingestion >> loader_tasks >> downstream_poking
