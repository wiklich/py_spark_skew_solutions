"""
Scenario D1: Generates a DataFrame with mix of many keys but some are disproportionately frequent.
"""

from typing import List
from pyspark.sql import DataFrame
from py_spark_skew_solutions.data_generator.base_scenario_data_generator import BaseScenarioDataGenerator
from py_spark_skew_solutions.utils.spark_utils import require_active_spark_session

class ScenarioD1DataGenerator(BaseScenarioDataGenerator):
    """
    Generates high-cardinality data with a subtle skew in one key.
    Useful for detecting hidden skews in distributed processing.
    """

    def generate(self, n_keys: int = 1_000_000, heavy_key: str = "VIP",
                 heavy_rows: int = 100_000) -> List[DataFrame]:
        """
        Generate high-cardinality data with a slightly heavier key.

        Parameters:
            n_keys (int): Total number of distinct keys.
            heavy_key (str): Key that appears more frequently than others.
            heavy_rows (int): Number of times the heavy key is repeated.

        Returns:
            DataFrame: Spark DataFrame with columns ['id', 'value'].
        """
        rows = [{"id": f"k_{i}", "value": i} for i in range(n_keys)]
        rows += [{"id": heavy_key, "value": i} for i in range(heavy_rows)]
        df = require_active_spark_session().createDataFrame(rows)
        return [df]