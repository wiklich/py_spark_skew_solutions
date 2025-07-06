"""
Scenario B: Generates a DataFrame with few keys with many records.
"""

from typing import List
from pyspark.sql import DataFrame
from py_spark_skew_solutions.data_generator.base_scenario_data_generator import BaseScenarioDataGenerator
from py_spark_skew_solutions.utils.spark_utils import require_active_spark_session

class ScenarioBDataGenerator(BaseScenarioDataGenerator):
    """
    Generates a DataFrame with a single hot key containing many records,
    and several cold keys with fewer records.
    Useful for simulating skewed joins or aggregations.
    """

    def generate(self, heavy_key: str = "HOT", heavy_rows: int = 2_000_000,
                 other_keys: int = 10_000) -> List[DataFrame]:
        """
        Generate skewed data with one heavy key.

        Parameters:
            heavy_key (str): Key with high frequency.
            heavy_rows (int): Number of rows for the heavy key.
            other_keys (int): Number of additional keys with small data.

        Returns:
            DataFrame: Spark DataFrame with columns ['id', 'value'].
        """
        rows = [{"id": heavy_key, "value": i} for i in range(heavy_rows)]
        rows += [{"id": f"cold_{i}", "value": i} for i in range(other_keys)]
        df = require_active_spark_session().createDataFrame(rows)
        return [df]