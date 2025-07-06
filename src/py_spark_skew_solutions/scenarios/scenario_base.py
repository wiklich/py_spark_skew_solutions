"""
Abstract base class for all scenarios.
"""

from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame


class ScenarioBase(ABC):
    """
    Abstract base class for all scenarios.

    Subclasses must define the following class-level attributes:
        - id: str
        - name: str
        - description: str
        - skew_type: str
        - example: str
    """

    id: str
    name: str
    description: str
    skew_type: str
    example: str

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def generate_data(self, **kwargs) -> DataFrame:
        """
        Generate a synthetic DataFrame based on the scenario's characteristics.

        Args:
            **kwargs: Optional additional parameters for data generation.

        Returns:
            DataFrame: The generated DataFrame.
        """
        raise NotImplementedError("Subclasses must implement generate_data method.")

    @abstractmethod
    def detect_skew(self, **kwargs) -> bool:
        """
        Detects if skew data exists in the given DataFrame for this scenario.

        Args:
            **kwargs: Optional additional parameters for detection logic.

        Returns:
            bool: True if skew is detected, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement detect_skew method.")

    @abstractmethod
    def run_demonstration(
        self,
        aqe_enabled: bool,
        demonstrate_skew: bool,
        shuffle_partitions: int = 18,
        **kwargs        
    ) -> None:
        """
        Runs a full demonstration of the scenario, including data generation,
        skew detection, and any relevant output or visualization.

        Args:
            aqe_enabled (bool): Whether Adaptive Query Execution is enabled during the demonstration.
            demonstrate_skew (bool): If True, demonstrates the impact of skew; if False, may skip it.
            shuffle_partitions (int): Number of shuffle partitions if AQE is disabled. Default: 18.
            **kwargs: Optional additional parameters for running demonstration.
        """
        raise NotImplementedError("Subclasses must implement run_demonstration method.")