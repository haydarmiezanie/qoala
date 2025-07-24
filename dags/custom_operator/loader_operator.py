from airflow.sdk import BaseOperator
import duckdb
import os
import json
import pandas as pd
from typing import Any

class LoaderOperator(BaseOperator):
    """
    Custom Airflow Operator to load JSON files into a DuckDB table.

    Attributes:
        table (str): Target table name in DuckDB.
        max_files (int): Maximum number of JSON files to load.
        duckdb_path (str): Relative path to the DuckDB database file.
    """

    def __init__(self, table: str, max_files: int, duckdb_path: str, **kwargs) -> None:
        """
        Initialize the LoaderOperator.

        Args:
            table (str): DuckDB table name.
            max_files (int): Max number of JSON files to read.
            duckdb_path (str): Path to DuckDB database file.
            **kwargs: Additional arguments for BaseOperator.
        """
        super().__init__(**kwargs)
        self.table = table
        self.max_files = max_files
        self.duckdb_path = duckdb_path

    def execute(self, context: Any) -> None:
        """
        Executes the loading of JSON files into a DuckDB table.

        Args:
            context (Any): Airflow context.
        """
        json_dir = f"/opt/airflow/raw/datasets/edgartanaka1/tmdb-movies-and-series/versions/1/{self.table}/{self.table}"

        if not os.path.exists(json_dir):
            self.log.error("Directory does not exist: %s", json_dir)
            raise FileNotFoundError(f"Directory not found: {json_dir}")

        all_dfs = []
        loaded_files = 0

        for filename in os.listdir(json_dir):
            if filename.lower().endswith('.json'):
                file_path = os.path.join(json_dir, filename)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    df = pd.json_normalize(data)
                    all_dfs.append(df)
                    loaded_files += 1
                    if loaded_files >= self.max_files:
                        break
                except Exception as e:
                    self.log.warning("Failed to process file %s: %s", file_path, e)

        if not all_dfs:
            self.log.warning("No valid JSON files found for table: %s", self.table)
            return

        final_df = pd.concat(all_dfs, ignore_index=True)
        self.log.info("DataFrame sample (first 5 rows):\n%s", final_df.head())

        duckdb_file_path = os.path.join("/opt/airflow/database", self.duckdb_path)

        try:
            con = duckdb.connect(duckdb_file_path)
            con.execute(f"CREATE OR REPLACE TABLE {self.table} AS SELECT * FROM final_df")
            con.close()
            self.log.info("Data successfully loaded into DuckDB table: %s", self.table)
        except Exception as e:
            self.log.error("Failed to write to DuckDB: %s", e)
            raise
