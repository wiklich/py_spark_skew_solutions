"""
Scenario A: Generates a DataFrame with many keys but few records per key.
"""

import random
from typing import List
from pyspark.sql import DataFrame
from py_spark_skew_solutions.data_generator.base_scenario_data_generator import BaseScenarioDataGenerator
from py_spark_skew_solutions.utils.spark_utils import require_active_spark_session

class ScenarioADataGenerator(BaseScenarioDataGenerator):
    """
    Generates a DataFrame with many keys but few records per key.
    Ideal for testing shuffling and partitioning strategies.
    """

    def generate(self, n_keys: int = 2_000_000) -> List[DataFrame]:
        """
        Generate rows with unique keys and random values.

        Parameters:
            n_keys (int): Number of distinct keys to generate.

        Returns:
            List[DataFrame]: A list containing one DataFrame with columns ['id', 'value'].
        """
        rows = [{"id": f"k_{i}", "value": random.random()} for i in range(n_keys)]
        df = self.spark.createDataFrame(rows)
        return [df]