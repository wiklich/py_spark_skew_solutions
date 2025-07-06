"""
Scenario C: Generates a DataFrame with mix of many small keys and few dominant ones.
"""

from typing import List
from pyspark.sql import DataFrame
from py_spark_skew_solutions.data_generator.base_scenario_data_generator import BaseScenarioDataGenerator
from py_spark_skew_solutions.utils.spark_utils import require_active_spark_session

class ScenarioCDataGenerator(BaseScenarioDataGenerator):
    """
    Generates a mix of many small keys and one hot key.
    Simulates real-world mixed data distributions.
    """  

    def generate(self, n_small_keys: int = 2_000_000, heavy_key: str = "HOT",
                 heavy_rows: int = 200_000) -> List[DataFrame]:
        """
        Generate a mix of small and large partitions.

        Parameters:
            n_small_keys (int): Number of small keys.
            heavy_key (str): Hot key with many rows.
            heavy_rows (int): Number of rows for the hot key.

        Returns:
            DataFrame: Spark DataFrame with columns ['id', 'value'].
        """
        rows = [{"id": f"k_{i}", "value": i} for i in range(n_small_keys)]
        rows += [{"id": heavy_key, "value": i} for i in range(heavy_rows)]
        df = require_active_spark_session().createDataFrame(rows)
        return [df]