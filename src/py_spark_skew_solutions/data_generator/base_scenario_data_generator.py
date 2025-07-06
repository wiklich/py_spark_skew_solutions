"""
Base module for scenario data generators.
"""

from abc import ABC, abstractmethod
from typing import List
from pyspark.sql import SparkSession, DataFrame


class BaseScenarioDataGenerator(ABC):
    """
    Abstract base class for all scenario data generators.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def generate(self, **kwargs) -> List[DataFrame]:
        """
        Generate synthetic DataFrame(s) based on the scenario's characteristics.

        Returns:
            List[DataFrame]: A list of generated DataFrames. Can be empty or contain multiple frames.
        """
        raise NotImplementedError("Subclasses must implement the generate method.")