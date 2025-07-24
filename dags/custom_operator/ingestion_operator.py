from airflow.sdk import BaseOperator
import logging
from typing import Any
import kagglehub

class IngestionOperator(BaseOperator):
    """
    Custom Airflow Operator to download datasets from Kaggle using kagglehub.

    Attributes:
        dataset (str): The Kaggle dataset identifier (e.g., "username/dataset-name").
    """

    def __init__(self, dataset: str, **kwargs) -> None:
        """
        Initialize the IngestionOperator.

        Args:
            dataset (str): The Kaggle dataset path.
            **kwargs: Additional keyword arguments passed to the BaseOperator.
        """
        super().__init__(**kwargs)
        self.dataset = dataset
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def execute(self, context: Any) -> None:
        """
        Executes the download of the Kaggle dataset using kagglehub.

        Args:
            context (Any): Airflow context dictionary.
        
        Raises:
            Exception: If the download fails for any reason.
        """
        try:
            dataset_path: str = kagglehub.dataset_download(self.dataset)
            self.logger.info("Successfully downloaded dataset: %s", self.dataset)
            self.logger.info("Dataset path: %s", dataset_path)
        except Exception as e:
            self.logger.error("Failed to download dataset %s: %s", self.dataset, e)
            raise
